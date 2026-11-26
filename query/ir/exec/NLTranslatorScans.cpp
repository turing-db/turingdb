#include "NLTranslator.h"

#include <optional>
#include <string>

#include "NLOps.h"

#include "mlir/IR/Block.h"
#include "mlir/IR/BuiltinAttributes.h"
#include "llvm/ADT/DenseMap.h"
#include "llvm/ADT/SmallVector.h"

#include "metadata/LabelSet.h"
#include "metadata/PropertyType.h"

#include "NLExecutor.h"
#include "NLTranslatorChunkTypes.h"

using namespace db;

namespace nl = mlir::nl;
namespace storage = mlir::storage;

namespace {

// Resolves a property-value scan's literal to the property's stored type
bool resolvePropertyScanLiteral(ValueType valueType,
                                mlir::TypedAttr literal,
                                NLScanByPropertyValueLoopData::Literal& value) {
    const mlir::IntegerAttr integer = mlir::dyn_cast<mlir::IntegerAttr>(literal);
    const mlir::FloatAttr floating = mlir::dyn_cast<mlir::FloatAttr>(literal);
    const bool isBoolLiteral = integer && integer.getType().isSignlessInteger(1);

    NLScanByPropertyValueLoopData::Literal given;
    if (isBoolLiteral) {
        given = CustomBool(integer.getInt() != 0);
    } else if (integer) {
        given = integer.getInt();
    } else if (floating) {
        given = floating.getValueAsDouble();
    } else {
        given = std::string(mlir::cast<mlir::StringAttr>(literal).getValue());
    }

    return NLScanByPropertyValueLoopData::convertToStored(valueType, given, value);
}

}

void NLTranslator::translateScanLoop(mlir::Block& loopBody, NLLimitState* limit, NLStmtContainer* body) {
    // For::verify guarantees one block argument per iterator chunk, and a
    // node scan iterator has exactly one chunk of node IDs
    ColumnNodeIDs* nodeIDs = static_cast<ColumnNodeIDs*>(allocColumn(loopBody.getArgument(0)));

    NLScanLoopData* loopData = _program->allocFunctionData<NLScanLoopData>(nodeIDs);
    loopData->setLimit(limit);

    body->emplaceStmt(&NLExecutor::runScanNodesLoop, loopData);

    translateBlock(loopBody, loopData->getStmts());
}

void NLTranslator::translateScanByLabelLoop(const IteratorConfig& config,
                                            mlir::Block& loopBody,
                                            NLLimitState* limit,
                                            NLStmtContainer* body) {
    // A label scan binds the same single node ID chunk as a plain scan.
    const mlir::Value nodeChunk = loopBody.getArgument(0);
    ColumnNodeIDs* nodeIDs = static_cast<ColumnNodeIDs*>(allocColumn(nodeChunk));

    // An unmatchable scan emits nothing rather than dropping the absent name and
    // matching a weaker set.
    LabelSet labelset;
    const bool matchable = resolveLabelSet(config._labels, labelset);

    NLScanByLabelLoopData* loopData = _program->allocFunctionData<NLScanByLabelLoopData>(nodeIDs, labelset, matchable);
    loopData->setLimit(limit);

    body->emplaceStmt(&NLExecutor::runScanNodesByLabelLoop, loopData);

    translateBlock(loopBody, loopData->getStmts());
}

void NLTranslator::translateConstScanLoop(const IteratorConfig& config,
                                          mlir::Block& loopBody,
                                          NLLimitState* limit,
                                          NLStmtContainer* body) {
    // A const scan binds the same single node ID chunk as a plain scan.
    const mlir::Value nodeChunk = loopBody.getArgument(0);
    ColumnNodeIDs* nodeIDs = static_cast<ColumnNodeIDs*>(allocColumn(nodeChunk));

    NLConstScanLoopData* loopData = _program->allocFunctionData<NLConstScanLoopData>(nodeIDs);
    loopData->setLimit(limit);

    loopData->reserveNodeIDs(config._listedIDs.size());
    for (const int64_t nodeID : config._listedIDs) {
        loopData->addNodeID(NodeID(static_cast<uint64_t>(nodeID)));
    }

    for (const mlir::Value bound : config._boundIDs) {
        loopData->addBoundIDs(getColumn(bound));
    }

    body->emplaceStmt(&NLExecutor::runConstScanNodesLoop, loopData);

    translateBlock(loopBody, loopData->getStmts());
}

void NLTranslator::translateScanByPropertyValueLoop(const IteratorConfig& config,
                                                    mlir::Block& loopBody,
                                                    NLLimitState* limit,
                                                    NLStmtContainer* body) {
    const mlir::Value nodeChunk = loopBody.getArgument(0);
    ColumnNodeIDs* nodeIDs = static_cast<ColumnNodeIDs*>(allocColumn(nodeChunk));

    LabelSet labelset;
    const bool byLabel = !config._labels.empty();
    const bool labelsResolved = !byLabel || resolveLabelSet(config._labels, labelset);

    const llvm::StringRef property = config._property;
    const std::optional<PropertyType> propertyType = findPropertyType(property);

    PropertyTypeID propertyTypeID;
    ValueType valueType = ValueType::Invalid;
    NLScanByPropertyValueLoopData::Literal literal;
    bool matchable = false;

    const mlir::Value bound = config._boundPropertyValue;
    if (labelsResolved && propertyType) {
        propertyTypeID = propertyType->_id;
        valueType = propertyType->_valueType;
        matchable = bound || resolvePropertyScanLiteral(valueType, config._propertyValue, literal);
    }

    NLScanByPropertyValueLoopData* loopData = _program->allocFunctionData<NLScanByPropertyValueLoopData>(nodeIDs,
                                                                                                         propertyTypeID,
                                                                                                         valueType,
                                                                                                         literal,
                                                                                                         labelset,
                                                                                                         byLabel,
                                                                                                         matchable);
    loopData->setLimit(limit);

    if (bound) {
        loopData->setBoundValue(getColumn(bound), valueTypeFromChunkType(bound.getType()));
    }

    body->emplaceStmt(&NLExecutor::runScanNodesByPropertyValueLoop, loopData);

    translateBlock(loopBody, loopData->getStmts());
}

void NLTranslator::translateScanEdgesLoop(mlir::Block& loopBody, NLLimitState* limit, NLStmtContainer* body) {
    // For::verify guarantees one block argument per iterator chunk, and an edge
    // scan iterator has exactly four chunks in the order getEdgeIteratorType
    // establishes: sources, edge IDs, edge type IDs, targets.
    ColumnNodeIDs* sources = static_cast<ColumnNodeIDs*>(allocColumn(loopBody.getArgument(0)));
    ColumnEdgeIDs* edgeIDs = static_cast<ColumnEdgeIDs*>(allocColumn(loopBody.getArgument(1)));
    ColumnEdgeTypes* edgeTypes = static_cast<ColumnEdgeTypes*>(allocColumn(loopBody.getArgument(2)));
    ColumnNodeIDs* targets = static_cast<ColumnNodeIDs*>(allocColumn(loopBody.getArgument(3)));

    NLScanEdgesLoopData* loopData = _program->allocFunctionData<NLScanEdgesLoopData>(sources,
                                                                                     edgeIDs,
                                                                                     edgeTypes,
                                                                                     targets);
    loopData->setLimit(limit);

    body->addStmt(NLFunctionDescriptor {&NLExecutor::runScanEdgesLoop, loopData});

    translateBlock(loopBody, loopData->getStmts());
}

void NLTranslator::translateConstScanEdgesLoop(const IteratorConfig& config,
                                               mlir::Block& loopBody,
                                               NLLimitState* limit,
                                               NLStmtContainer* body) {
    ColumnNodeIDs* sources = static_cast<ColumnNodeIDs*>(allocColumn(loopBody.getArgument(0)));
    ColumnEdgeIDs* edgeIDs = static_cast<ColumnEdgeIDs*>(allocColumn(loopBody.getArgument(1)));
    ColumnEdgeTypes* edgeTypes = static_cast<ColumnEdgeTypes*>(allocColumn(loopBody.getArgument(2)));
    ColumnNodeIDs* targets = static_cast<ColumnNodeIDs*>(allocColumn(loopBody.getArgument(3)));

    NLConstScanEdgesLoopData* loopData = _program->allocFunctionData<NLConstScanEdgesLoopData>(sources,
                                                                                               edgeIDs,
                                                                                               edgeTypes,
                                                                                               targets,
                                                                                               config._undirected);
    loopData->setLimit(limit);

    loopData->reserveEdgeIDs(config._listedIDs.size());
    for (const int64_t edgeID : config._listedIDs) {
        loopData->addEdgeID(EdgeID(static_cast<uint64_t>(edgeID)));
    }

    for (const mlir::Value bound : config._boundIDs) {
        loopData->addBoundIDs(getColumn(bound));
    }

    body->emplaceStmt(&NLExecutor::runConstScanEdgesLoop, loopData);

    translateBlock(loopBody, loopData->getStmts());
}

void NLTranslator::translateScanEdgesByTypeLoop(const IteratorConfig& config,
                                                mlir::Block& loopBody,
                                                NLLimitState* limit,
                                                NLStmtContainer* body) {
    ColumnNodeIDs* sources = static_cast<ColumnNodeIDs*>(allocColumn(loopBody.getArgument(0)));
    ColumnEdgeIDs* edgeIDs = static_cast<ColumnEdgeIDs*>(allocColumn(loopBody.getArgument(1)));
    ColumnEdgeTypes* edgeTypes = static_cast<ColumnEdgeTypes*>(allocColumn(loopBody.getArgument(2)));
    ColumnNodeIDs* targets = static_cast<ColumnNodeIDs*>(allocColumn(loopBody.getArgument(3)));

    // Resolve the edge type names against the schema, exactly as translateEdgeLoop
    // does for a by-type hop.
    llvm::SmallVector<EdgeTypeID, 4> requestedTypes;
    resolveEdgeTypes(config._edgeTypes, requestedTypes);

    NLScanEdgesByTypeLoopData* loopData =
        _program->allocFunctionData<NLScanEdgesByTypeLoopData>(sources,
                                                               edgeIDs,
                                                               edgeTypes,
                                                               targets,
                                                               requestedTypes);
    loopData->setLimit(limit);

    body->addStmt(NLFunctionDescriptor {&NLExecutor::runScanEdgesByTypeLoop, loopData});

    translateBlock(loopBody, loopData->getStmts());
}

void NLTranslator::translateScanEdgesByLabelLoop(const IteratorConfig& config,
                                                 mlir::Block& loopBody,
                                                 NLLimitState* limit,
                                                 NLStmtContainer* body,
                                                 NLHandlerFunction executor) {
    ColumnNodeIDs* sources = static_cast<ColumnNodeIDs*>(allocColumn(loopBody.getArgument(0)));
    ColumnEdgeIDs* edgeIDs = static_cast<ColumnEdgeIDs*>(allocColumn(loopBody.getArgument(1)));
    ColumnEdgeTypes* edgeTypes = static_cast<ColumnEdgeTypes*>(allocColumn(loopBody.getArgument(2)));
    ColumnNodeIDs* targets = static_cast<ColumnNodeIDs*>(allocColumn(loopBody.getArgument(3)));

    // An unmatchable scan emits nothing rather than dropping the absent name and
    // matching a weaker set, exactly as translateScanByLabelLoop does.
    LabelSet labelset;
    const bool matchable = resolveLabelSet(config._labels, labelset);

    NLScanEdgesByLabelLoopData* loopData =
        _program->allocFunctionData<NLScanEdgesByLabelLoopData>(sources,
                                                                edgeIDs,
                                                                edgeTypes,
                                                                targets,
                                                                labelset,
                                                                matchable);
    loopData->setLimit(limit);

    body->addStmt(NLFunctionDescriptor {executor, loopData});

    translateBlock(loopBody, loopData->getStmts());
}

void NLTranslator::translateScanEdgesByEndpointLabelsLoop(const IteratorConfig& config,
                                                          mlir::Block& loopBody,
                                                          NLLimitState* limit,
                                                          NLStmtContainer* body,
                                                          NLHandlerFunction executor) {
    ColumnNodeIDs* sources = static_cast<ColumnNodeIDs*>(allocColumn(loopBody.getArgument(0)));
    ColumnEdgeIDs* edgeIDs = static_cast<ColumnEdgeIDs*>(allocColumn(loopBody.getArgument(1)));
    ColumnEdgeTypes* edgeTypes = static_cast<ColumnEdgeTypes*>(allocColumn(loopBody.getArgument(2)));
    ColumnNodeIDs* targets = static_cast<ColumnNodeIDs*>(allocColumn(loopBody.getArgument(3)));

    LabelSet indexedLabelSet;
    const bool indexedMatchable = resolveLabelSet(config._labels, indexedLabelSet);

    LabelSet otherEndLabelSet;
    const bool otherEndMatchable = resolveLabelSet(config._otherEndLabels, otherEndLabelSet);

    NLScanEdgesByEndpointLabelsLoopData* loopData =
        _program->allocFunctionData<NLScanEdgesByEndpointLabelsLoopData>(sources,
                                                                         edgeIDs,
                                                                         edgeTypes,
                                                                         targets,
                                                                         indexedLabelSet,
                                                                         otherEndLabelSet,
                                                                         indexedMatchable && otherEndMatchable);
    loopData->setLimit(limit);

    body->addStmt(NLFunctionDescriptor {executor, loopData});

    translateBlock(loopBody, loopData->getStmts());
}

void NLTranslator::translateFetchNodes(nl::FetchNodes fetch, NLStmtContainer* body) {
    const mlir::Value idsValue = fetch.getIds();
    const Column* ids = getColumn(idsValue);

    const mlir::Value nodesValue = fetch.getNodes();
    ColumnNodeIDs* nodes = static_cast<ColumnNodeIDs*>(allocColumnForChunkType(nodesValue.getType()));
    _valueSlots[nodesValue] = nodes;

    const mlir::Type nodeType = mlir::cast<nl::ChunkType>(nodesValue.getType()).getElementType();
    const NLFetchNodesFunction fetchFunction = selectFetchForChunkType<NodeID>(idsValue.getType(), nodeType);
    NLFetchNodesData* data = _program->allocFunctionData<NLFetchNodesData>(ids, fetchFunction, nodes);

    data->getIndices()->reserve(_program->getChunkSize());
    nodes->reserve(_program->getChunkSize());

    const mlir::OperandRange columns = fetch.getColumnsToFilter();
    const mlir::ResultRange results = fetch.getCarried();
    for (size_t columnIndex = 0; columnIndex < columns.size(); columnIndex++) {
        const mlir::Type columnType = columns[columnIndex].getType();

        const Column* input = getColumn(columns[columnIndex]);
        Column* output = allocColumnForChunkType(columnType);
        _valueSlots[results[columnIndex]] = output;

        data->addColumn(NLFilterData::FilterColumn {input, output, selectGatherForChunkType(columnType)});
    }

    body->emplaceStmt(&NLExecutor::runFetchNodes, data);
}

void NLTranslator::translateFetchEdges(nl::FetchEdges fetch, NLStmtContainer* body) {
    const mlir::Value idsValue = fetch.getIds();
    const Column* ids = getColumn(idsValue);

    ColumnNodeIDs* sources = static_cast<ColumnNodeIDs*>(allocColumnForChunkType(fetch.getSrcids().getType()));
    ColumnEdgeIDs* edgeIDs = static_cast<ColumnEdgeIDs*>(allocColumnForChunkType(fetch.getEids().getType()));
    ColumnEdgeTypes* edgeTypes = static_cast<ColumnEdgeTypes*>(allocColumnForChunkType(fetch.getEtypes().getType()));
    ColumnNodeIDs* targets = static_cast<ColumnNodeIDs*>(allocColumnForChunkType(fetch.getTgtids().getType()));

    _valueSlots[fetch.getSrcids()] = sources;
    _valueSlots[fetch.getEids()] = edgeIDs;
    _valueSlots[fetch.getEtypes()] = edgeTypes;
    _valueSlots[fetch.getTgtids()] = targets;

    const mlir::Type edgeType = mlir::cast<nl::ChunkType>(fetch.getEids().getType()).getElementType();
    const NLFetchEdgesFunction fetchFunction = selectFetchForChunkType<EdgeID>(idsValue.getType(), edgeType);
    NLFetchEdgesData* data = _program->allocFunctionData<NLFetchEdgesData>(ids,
                                                                           fetchFunction,
                                                                           fetch.getUndirected(),
                                                                           sources,
                                                                           edgeIDs,
                                                                           edgeTypes,
                                                                           targets);

    data->getIndices()->reserve(_program->getChunkSize());

    const mlir::OperandRange columns = fetch.getColumnsToFilter();
    const mlir::ResultRange results = fetch.getCarried();
    for (size_t columnIndex = 0; columnIndex < columns.size(); columnIndex++) {
        const mlir::Type columnType = columns[columnIndex].getType();

        const Column* input = getColumn(columns[columnIndex]);
        Column* output = allocColumnForChunkType(columnType);
        _valueSlots[results[columnIndex]] = output;

        data->addColumn(NLFilterData::FilterColumn {input, output, selectGatherForChunkType(columnType)});
    }

    body->emplaceStmt(&NLExecutor::runFetchEdges, data);
}

template <TypedInternalID IDType>
NLFetchFunction<IDType> NLTranslator::selectFetchForChunkType(mlir::Type chunkType, mlir::Type entityType) {
    const mlir::Type elementType = mlir::cast<nl::ChunkType>(chunkType).getElementType();

    if (elementType == entityType) {
        return NLExecutor::selectEntityFetchFunction<IDType>();
    } else if (mlir::isa<storage::AnyType>(elementType)) {
        return NLExecutor::selectTaggedFetchFunction<IDType>();
    }

    const auto nullableType = mlir::dyn_cast<storage::NullableType>(elementType);

    const bool nullable = static_cast<bool>(nullableType);
    const mlir::Type valueType = nullable ? nullableType.getValueType() : elementType;
    const NLChunkKind kind = nullable ? chunkKindFromNullableValue(valueType) : chunkKindFromElementType(valueType);

    return NLExecutor::selectIntegerFetchFunction<IDType>(kind, nullable);
}

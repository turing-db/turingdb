#include "NLTranslator.h"

#include <optional>
#include <vector>
#include <string>
#include <string_view>

#include "NLOps.h"
#include "StorageEnums.h"

#include "IRValueTypes.h"
#include "mlir/IR/BuiltinAttributes.h"
#include "llvm/ADT/DenseMap.h"
#include "llvm/ADT/SmallVector.h"

#include "metadata/GraphMetadata.h"
#include "metadata/LabelSet.h"
#include "metadata/LabelSetHandle.h"
#include "metadata/PropertyType.h"
#include "views/GraphView.h"
#include "writers/MetadataBuilder.h"

#include "NLRuntimeProperty.h"
#include "NLExecutor.h"
#include "NLTranslatorChunkTypes.h"

#include "LocalMemory.h"
#include "IRException.h"
#include "BioAssert.h"

using namespace db;

namespace nl = mlir::nl;
namespace storage = mlir::storage;

namespace {

NLMergeDirection mergeDirectionFromValue(int64_t raw) {
    const std::optional<storage::EdgeDirection> direction =
        storage::symbolizeEdgeDirection(static_cast<uint64_t>(raw));

    if (!direction) {
        throw IRException("nl.merge carries an unknown edge direction");
    }

    switch (*direction) {
        case storage::EdgeDirection::Undirected:
            return NLMergeDirection::Undirected;
        break;

        case storage::EdgeDirection::Backward:
            return NLMergeDirection::Backward;
        break;

        case storage::EdgeDirection::Forward:
            return NLMergeDirection::Forward;
        break;
    }

    bioassert(false, "Unhandled edge direction");
    return NLMergeDirection::Forward;
}

// A property written from tagged cells that has no type yet is told apart by its name,
// behind the invalid ID no typed property carries
void appendMergePropertySignature(const std::vector<NLMergeProperty>& properties,
                                  std::string& signature) {
    for (const NLMergeProperty& property : properties) {
        const PropertyTypeID::Type id = property._propertyType._id.getValue();
        const ValueType valueType = property._propertyType._valueType;

        signature.append(reinterpret_cast<const char*>(&id), sizeof(id));
        signature.append(reinterpret_cast<const char*>(&valueType), sizeof(valueType));

        if (!property._propertyType.isValid()) {
            const std::string_view name = property._runtimeProperty->getName();
            const size_t length = name.size();

            signature.append(reinterpret_cast<const char*>(&length), sizeof(length));
            signature.append(name);
        }
    }
}

// What the pending log's keys for one chain-node spec start with: its label set and the
// property it keys on, in order. Two specs writing the same pattern share the prefix, so
// each binds what the other wrote; two that differ never collide.
void appendMergeNodeSignature(const LabelSet& labels,
                              const std::vector<NLMergeProperty>& properties,
                              std::string& signature) {
    signature.clear();

    for (const LabelSet::IntegerType integer : labels.integers()) {
        signature.append(reinterpret_cast<const char*>(&integer), sizeof(integer));
    }

    appendMergePropertySignature(properties, signature);
}

}

bool NLTranslator::isPendingValue(mlir::Value value, bool isNode) const {
    const llvm::DenseSet<mlir::Value>& pendingValues = isNode ? _pendingNodeValues : _pendingEdgeValues;

    return pendingValues.contains(value);
}

void NLTranslator::translateCreateNode(nl::CreateNode createNode, NLStmtContainer* body) {
    if (!_metadataBuilder) {
        throw IRException("Cannot perform CREATE outside of a write transaction.");
    }

    llvm::SmallVector<llvm::StringRef, 4> names;
    resolveLabelNames(createNode.getLabels(), names);

    LabelSet labelset;
    for (const llvm::StringRef labelName : names) {
        const LabelID labelID = _metadataBuilder->getOrCreateLabel(labelName);
        labelset.set(labelID);
    }
    bioassert(not labelset.empty(), "Node must have at least one label.");

    const LabelSetHandle labelsetHandle = _metadataBuilder->getOrCreateLabelSet(labelset);

    ColumnNodeIDs* result = _memory->alloc<ColumnNodeIDs>();
    _valueSlots[createNode.getResult()] = result;
    _pendingNodeValues.insert(createNode.getResult());

    NLCreateNodeData* data = _program->allocFunctionData<NLCreateNodeData>(labelsetHandle, result);

    const mlir::OperandRange propValues = createNode.getPropValues();
    const mlir::ArrayAttr propNames = createNode.getPropNames();

    for (size_t propIndex = 0; propIndex < propNames.size(); propIndex++) {
        const llvm::StringRef propName = mlir::cast<mlir::StringAttr>(propNames[propIndex]).getValue();

        NLCreatedProperty property;
        translateCreatedProperty(propName, propValues[propIndex], property);
        data->addProperty(property);
    }

    if (const mlir::Value cardinality = createNode.getCardinality()) {
        data->setCardinality(getColumn(cardinality));
    }

    body->emplaceStmt(&NLExecutor::runCreateNode, data);
}

void NLTranslator::translateCreateEdge(nl::CreateEdge createEdge, NLStmtContainer* body) {
    if (!_metadataBuilder) {
        throw IRException("Cannot perform CREATE outside of a write transaction.");
    }

    const llvm::StringRef edgeTypeName = resolveEdgeTypeName(createEdge.getEdgeType());
    const EdgeTypeID edgeTypeID = _metadataBuilder->getOrCreateEdgeType(edgeTypeName);

    const mlir::Value srcValue = createEdge.getSrcIds();
    const mlir::Value tgtValue = createEdge.getTgtIds();
    const bool srcIsPending = createEdge.getSrcAllPending() || isPendingValue(srcValue, /*isNode=*/true);
    const bool tgtIsPending = createEdge.getTgtAllPending() || isPendingValue(tgtValue, /*isNode=*/true);

    const ColumnNodeIDs* srcColumn = static_cast<const ColumnNodeIDs*>(getColumn(srcValue));
    const ColumnNodeIDs* tgtColumn = static_cast<const ColumnNodeIDs*>(getColumn(tgtValue));

    ColumnEdgeIDs* result = _memory->alloc<ColumnEdgeIDs>();
    _valueSlots[createEdge.getResult()] = result;
    _pendingEdgeValues.insert(createEdge.getResult());

    NLCreateEdgeData* data = _program->allocFunctionData<NLCreateEdgeData>(
        edgeTypeID,
        srcColumn,
        srcIsPending,
        tgtColumn,
        tgtIsPending,
        result);

    data->setSrcPendingMask(getMaskColumn(createEdge.getSrcPending()));
    data->setTgtPendingMask(getMaskColumn(createEdge.getTgtPending()));

    const mlir::OperandRange propValues = createEdge.getPropValues();
    const mlir::ArrayAttr propNames = createEdge.getPropNames();

    for (size_t propIndex = 0; propIndex < propNames.size(); propIndex++) {
        const llvm::StringRef propName = mlir::cast<mlir::StringAttr>(propNames[propIndex]).getValue();

        NLCreatedProperty property;
        translateCreatedProperty(propName, propValues[propIndex], property);
        data->addProperty(property);
    }

    body->emplaceStmt(&NLExecutor::runCreateEdge, data);
}

void NLTranslator::translateCreatedProperty(llvm::StringRef propName,
                                            mlir::Value propValue,
                                            NLCreatedProperty& property) {
    const mlir::Type valueChunkType = propValue.getType();
    property._values = getColumn(propValue);

    if (isAnyChunk(valueChunkType)) {
        property._runtimeProperty = _program->getRuntimeProperty(propName);
    } else {
        const ValueType valueType = valueTypeFromChunkType(valueChunkType);
        property._propertyTypeID = _metadataBuilder->getOrCreatePropertyType(propName, valueType)._id;
    }
}

void NLTranslator::translateMerge(nl::Merge merge, NLStmtContainer* body) {
    if (!_metadataBuilder) {
        throw IRException("Cannot perform MERGE outside of a write transaction.");
    }

    const mlir::ArrayAttr nodeLabels = merge.getNodeLabels();
    const mlir::ArrayAttr nodePropNames = merge.getNodePropNames();
    const mlir::ArrayAttr edgeTypes = merge.getEdgeTypes();
    const mlir::ArrayAttr edgePropNames = merge.getEdgePropNames();
    const llvm::ArrayRef<int64_t> directions = merge.getEdgeDirections();

    const mlir::OperandRange boundNodes = merge.getBoundNodes();
    const mlir::OperandRange boundPending = merge.getBoundPending();
    const mlir::OperandRange nodePropValues = merge.getNodePropValues();
    const mlir::OperandRange edgePropValues = merge.getEdgePropValues();
    const mlir::OperandRange carriedColumns = merge.getCarriedColumns();
    const mlir::ResultRange results = merge.getResults();

    ColumnVector<CustomBool>* created = _memory->alloc<ColumnVector<CustomBool>>();
    created->reserve(_program->getChunkSize());

    NLMergeData* data = _program->allocFunctionData<NLMergeData>(_program->getMergePendingEdges(), created, _memory);

    const size_t nodeCount = nodeLabels.size();
    const size_t hopCount = nodeCount - 1;

    // A bound node has no result pair of its own: its rows come back through the carry set
    size_t resultIndex = 0;
    size_t boundIndex = 0;
    size_t nodeValueIndex = 0;
    for (size_t nodeIndex = 0; nodeIndex < nodeCount; nodeIndex++) {
        const mlir::ArrayAttr labels = mlir::cast<mlir::ArrayAttr>(nodeLabels[nodeIndex]);
        const mlir::ArrayAttr propNames = mlir::cast<mlir::ArrayAttr>(nodePropNames[nodeIndex]);

        NLMergeData::Node node;

        if (labels.empty()) {
            node._boundColumn = static_cast<const ColumnNodeIDs*>(getColumn(boundNodes[boundIndex]));
            boundIndex++;
        } else {
            translateMergeNodeSpec(labels, propNames, nodePropValues.slice(nodeValueIndex, propNames.size()), node);

            node._output = static_cast<ColumnNodeIDs*>(allocColumn(results[resultIndex]));
            node._outputPending = static_cast<ColumnVector<CustomBool>*>(allocColumn(results[resultIndex + 1]));
            resultIndex += 2;
        }

        nodeValueIndex += propNames.size();

        data->addNode(node);
    }

    // Only a bound node whose column a merge produced carries a mask, so each one names
    // the chain node it belongs to rather than leaving a hole in a per-node group
    const llvm::ArrayRef<int64_t> pendingNodes = merge.getPendingNodes();
    std::vector<NLMergeData::Node>& nodes = data->nodes();
    for (size_t index = 0; index < pendingNodes.size(); index++) {
        nodes[pendingNodes[index]]._boundPending = getMaskColumn(boundPending[index]);
    }

    size_t edgeValueIndex = 0;
    for (size_t hopIndex = 0; hopIndex < hopCount; hopIndex++) {
        const llvm::StringRef edgeTypeName = resolveEdgeTypeName(edgeTypes[hopIndex]);
        const mlir::ArrayAttr propNames = mlir::cast<mlir::ArrayAttr>(edgePropNames[hopIndex]);

        NLMergeData::Hop hop;
        hop._writeEdgeType = _metadataBuilder->getOrCreateEdgeType(edgeTypeName);
        hop._direction = mergeDirectionFromValue(directions[hopIndex]);

        // A type the graph's schema does not have is on no committed edge, so the match
        // reads the graph only when the name resolves there. It is created above all the
        // same, since a written hop needs it.
        const std::optional<EdgeTypeID> matchEdgeType = _view->metadata().edgeTypes().get(edgeTypeName);
        if (matchEdgeType) {
            hop._matchEdgeType = *matchEdgeType;
        }

        bool matchable = matchEdgeType.has_value();
        for (size_t propIndex = 0; propIndex < propNames.size(); propIndex++) {
            const llvm::StringRef propName = mlir::cast<mlir::StringAttr>(propNames[propIndex]).getValue();
            const mlir::Value propValue = edgePropValues[edgeValueIndex + propIndex];

            translateMergeProperty(propName, propValue, hop._properties, hop._scanProperties, matchable);
        }

        // A property the graph does not have is on no committed edge either, so the hop
        // reads no candidate rather than reading one it cannot key
        if (!matchable) {
            hop._matchEdgeType = EdgeTypeID();
            hop._scanProperties.clear();
        }

        edgeValueIndex += propNames.size();

        collectWrittenMergeProperties(hop._properties, hop._writtenProperties);

        hop._scanSources = _memory->alloc<ColumnNodeIDs>();
        hop._scanEdges = _memory->alloc<ColumnEdgeIDs>();

        hop._output = static_cast<ColumnEdgeIDs*>(allocColumn(results[resultIndex]));
        hop._outputPending = static_cast<ColumnVector<CustomBool>*>(allocColumn(results[resultIndex + 1]));
        resultIndex += 2;

        data->addHop(hop);
    }

    _valueSlots[results[resultIndex]] = created;
    resultIndex++;

    for (size_t index = 0; index < carriedColumns.size(); index++) {
        const mlir::Value input = carriedColumns[index];

        data->addCarriedColumn({getColumn(input),
                                allocColumn(results[resultIndex + index]),
                                selectGatherForChunkType(input.getType())});
    }

    // Every row the op runs over is a row of its inputs, so any of them holding rows of
    // its own gives their count. A pattern reading only constants runs over one row.
    for (const mlir::Value input : merge->getOperands()) {
        const Column* column = getColumn(input);
        if (!isConstantColumn(column)) {
            data->setRowCarrier(column);
            break;
        }
    }

    body->emplaceStmt(&NLExecutor::runMerge, data);
}

void NLTranslator::translateMergeNodeSpec(mlir::ArrayAttr labels,
                                          mlir::ArrayAttr propNames,
                                          mlir::OperandRange propValues,
                                          NLMergeData::Node& node) {
    // The labels the node is written with, created where the schema lacks them, and the
    // same set resolved against the graph for the match. A name the graph does not have
    // is on no committed node, so the conjunction matches nothing there - the way
    // nl.scan_nodes_by_label treats it - and only what this query wrote can match.
    LabelSet writeLabels;
    LabelSet matchLabels;
    bool matchable = true;

    llvm::SmallVector<llvm::StringRef, 4> names;
    resolveLabelNames(labels, names);

    const LabelMap& graphLabels = _view->metadata().labels();
    for (const llvm::StringRef labelName : names) {
        writeLabels.set(_metadataBuilder->getOrCreateLabel(labelName));

        const std::optional<LabelID> graphLabel = graphLabels.get(labelName);
        if (graphLabel) {
            matchLabels.set(*graphLabel);
        } else {
            matchable = false;
        }
    }

    node._labelSetHandle = _metadataBuilder->getOrCreateLabelSet(writeLabels);

    std::vector<NLMergeScanProperty> scanProperties;
    for (size_t propIndex = 0; propIndex < propNames.size(); propIndex++) {
        const llvm::StringRef propName = mlir::cast<mlir::StringAttr>(propNames[propIndex]).getValue();

        translateMergeProperty(propName, propValues[propIndex], node._properties, scanProperties, matchable);
    }

    if (!matchable) {
        scanProperties.clear();
    }

    std::string signature;
    appendMergeNodeSignature(writeLabels, node._properties, signature);

    // Every chain node of the same signature looks its candidates up in one index, so
    // the label set is scanned once however many times the query merges the pattern
    node._index = _program->findMergeNodeIndex(signature);
    if (node._index) {
        return;
    }

    ColumnNodeIDs* scanNodes = _memory->alloc<ColumnNodeIDs>();
    node._index = _program->addMergeNodeIndex(signature, matchLabels, writeLabels, matchable, scanNodes);
    for (const NLMergeScanProperty& scanProperty : scanProperties) {
        node._index->addScanProperty(scanProperty);
    }

    NLMergeScanProperties writtenProperties;
    collectWrittenMergeProperties(node._properties, writtenProperties);
    for (const NLMergeScanProperty& writtenProperty : writtenProperties) {
        node._index->addWrittenProperty(writtenProperty);
    }
}

void NLTranslator::collectWrittenMergeProperties(const std::vector<NLMergeProperty>& properties,
                                                 NLMergeScanProperties& writtenProperties) {
    for (const NLMergeProperty& property : properties) {
        const ValueType valueType = property._propertyType._valueType;

        if (!property._propertyType.isValid()) {
            writtenProperties.emplace_back();
        } else {
            writtenProperties.push_back({._propertyType=property._propertyType,
                                         ._values=allocOptColumnForValueType(valueType),
                                         ._keyAppend=NLExecutor::selectOptKeyAppendFunction(chunkKindFromValueType(valueType))});
        }
    }
}

void NLTranslator::translateMergeProperty(llvm::StringRef propName,
                                          mlir::Value propValue,
                                          std::vector<NLMergeProperty>& properties,
                                          std::vector<NLMergeScanProperty>& scanProperties,
                                          bool& matchable) {
    const Column* column = getColumn(propValue);
    const mlir::Type valueChunkType = propValue.getType();

    if (isAnyChunk(valueChunkType)) {
        // A property the change knows is typed here, so the merge keys it as a typed merge
        // of it does and shares that merge's index
        NLRuntimeProperty* runtimeProperty = _program->getRuntimeProperty(propName);
        const bool typed = runtimeProperty->findType(_metadataBuilder);

        properties.push_back({._propertyType=typed ? runtimeProperty->getType() : PropertyType {},
                              ._values=column,
                              ._runtimeProperty=runtimeProperty});
    } else {
        // Created where the schema lacks it, since a written pattern carries the property.
        // Both sides of the key serialize in its registered type: the analyzer lets an
        // integer constrain a double-typed property, and 5 keys like the 5.0 the graph side
        // reads back only once converted to it.
        const ValueType valueType = valueTypeFromChunkType(valueChunkType);
        const PropertyType writeType = _metadataBuilder->getOrCreatePropertyType(propName, valueType);
        properties.push_back({._propertyType=writeType,
                              ._values=column,
                              ._keyAppend=selectMergeKeyAppend(valueChunkType,
                                                               column,
                                                               writeType._valueType)});
    }

    // The graph's own property, which is what a candidate's value is read back through.
    // Absent means no committed entity carries it, so nothing there can match.
    const std::optional<PropertyType> graphType = _view->metadata().propTypes().get(propName);
    if (!graphType) {
        matchable = false;
        return;
    }

    scanProperties.push_back({._propertyType=*graphType,
                              ._values=allocOptColumnForValueType(graphType->_valueType),
                              ._keyAppend=NLExecutor::selectOptKeyAppendFunction(chunkKindFromValueType(graphType->_valueType))});
}

PropertyType NLTranslator::setPropertyType(llvm::StringRef propName, mlir::Type valueChunkType) const {
    const bool typedAsItRuns = isUntypedNullChunkType(valueChunkType) || isAnyChunk(valueChunkType);
    if (typedAsItRuns) {
        return PropertyType {};
    }

    const ValueType valueType = valueTypeFromChunkType(valueChunkType);

    return _metadataBuilder->getOrCreatePropertyType(propName, valueType);
}

void NLTranslator::translateSetNodeProperty(nl::SetNodeProperty setNodeProperty, NLStmtContainer* body) {
    if (!_metadataBuilder) {
        throw IRException("Cannot perform SET or REMOVE outside of a write transaction.");
    }

    const llvm::StringRef propName = setNodeProperty.getProperty();
    const mlir::Value inputValue = setNodeProperty.getInputNodes();
    const mlir::Value propValue = setNodeProperty.getValue();
    const mlir::Type valueChunkType = propValue.getType();
    const PropertyType propType = setPropertyType(propName, valueChunkType);

    const ColumnNodeIDs* inputColumn = static_cast<const ColumnNodeIDs*>(getColumn(inputValue));
    const Column* valueColumn = getColumn(propValue);

    NLSetNodePropertyData* data = _program->allocFunctionData<NLSetNodePropertyData>(
        propType._id,
        inputColumn,
        valueColumn);

    if (!propType.isValid()) {
        const bool writesNull = isUntypedNullChunkType(valueChunkType);

        data->setRuntimeProperty(_program->getRuntimeProperty(propName));
        data->setWritesNull(writesNull);

        if (!writesNull) {
            ColumnVector<CustomBool>* writtenRows = _memory->alloc<ColumnVector<CustomBool>>();
            writtenRows->reserve(_program->getChunkSize());
            data->setWrittenRows(writtenRows);
        }
    }

    data->setPending(getMaskColumn(setNodeProperty.getPending()));
    data->setAllPending(setNodeProperty.getAllPending() || isPendingValue(inputValue, /*isNode=*/true));
    data->setRows(getMaskColumn(setNodeProperty.getRows()));

    body->emplaceStmt(&NLExecutor::runSetNodeProperty, data);
}

void NLTranslator::translateSetEdgeProperty(nl::SetEdgeProperty setEdgeProperty, NLStmtContainer* body) {
    if (!_metadataBuilder) {
        throw IRException("Cannot perform SET outside of a write transaction.");
    }

    const llvm::StringRef propName = setEdgeProperty.getProperty();
    const mlir::Value inputValue = setEdgeProperty.getInputEdges();
    const mlir::Value propValue = setEdgeProperty.getValue();
    const mlir::Type valueChunkType = propValue.getType();
    const PropertyType propType = setPropertyType(propName, valueChunkType);

    const ColumnEdgeIDs* inputColumn = static_cast<const ColumnEdgeIDs*>(getColumn(inputValue));
    const Column* valueColumn = getColumn(propValue);

    NLSetEdgePropertyData* data = _program->allocFunctionData<NLSetEdgePropertyData>(
        propType._id,
        inputColumn,
        valueColumn);

    if (!propType.isValid()) {
        const bool writesNull = isUntypedNullChunkType(valueChunkType);

        data->setRuntimeProperty(_program->getRuntimeProperty(propName));
        data->setWritesNull(writesNull);

        if (!writesNull) {
            ColumnVector<CustomBool>* writtenRows = _memory->alloc<ColumnVector<CustomBool>>();
            writtenRows->reserve(_program->getChunkSize());
            data->setWrittenRows(writtenRows);
        }
    }

    data->setPending(getMaskColumn(setEdgeProperty.getPending()));
    data->setAllPending(setEdgeProperty.getAllPending() || isPendingValue(inputValue, /*isNode=*/false));
    data->setRows(getMaskColumn(setEdgeProperty.getRows()));

    body->emplaceStmt(&NLExecutor::runSetEdgeProperty, data);
}

void NLTranslator::createMapKeyProperties(mlir::DictionaryAttr createdTypes) const {
    if (!createdTypes) {
        return;
    }

    for (const mlir::NamedAttribute created : createdTypes) {
        const mlir::StringAttr name = created.getName();
        const llvm::StringRef key = name.getValue();
        if (findPropertyType(key)) {
            continue;
        }

        const mlir::TypeAttr typeAttr = mlir::cast<mlir::TypeAttr>(created.getValue());
        const mlir::Type elementType = typeAttr.getValue();
        _metadataBuilder->getOrCreatePropertyType(std::string_view {key.data(), key.size()},
                                                  elementTypeToValueType(elementType));
    }
}

void NLTranslator::translateDeleteNode(nl::DeleteNode deleteNode, NLStmtContainer* body) {
    if (!_metadataBuilder) {
        throw IRException("Cannot perform DELETE outside of a write transaction.");
    }

    const mlir::Value inputValue = deleteNode.getInputNodes();
    const ColumnNodeIDs* inputColumn = static_cast<const ColumnNodeIDs*>(getColumn(inputValue));

    NLDeleteNodeData* data = _program->allocFunctionData<NLDeleteNodeData>(
        inputColumn,
        deleteNode.getDetach(),
        _memory->alloc<ColumnNodeIDs>());

    data->setPending(getMaskColumn(deleteNode.getPending()));
    data->setAllPending(deleteNode.getAllPending() || isPendingValue(inputValue, /*isNode=*/true));

    body->emplaceStmt(&NLExecutor::runDeleteNode, data);
}

void NLTranslator::translateDeleteEdge(nl::DeleteEdge deleteEdge, NLStmtContainer* body) {
    if (!_metadataBuilder) {
        throw IRException("Cannot perform DELETE outside of a write transaction.");
    }

    const mlir::Value inputValue = deleteEdge.getInputEdges();
    const ColumnEdgeIDs* inputColumn = static_cast<const ColumnEdgeIDs*>(getColumn(inputValue));

    NLDeleteEdgeData* data = _program->allocFunctionData<NLDeleteEdgeData>(
        inputColumn,
        _memory->alloc<ColumnEdgeIDs>());

    data->setPending(getMaskColumn(deleteEdge.getPending()));
    data->setAllPending(deleteEdge.getAllPending() || isPendingValue(inputValue, /*isNode=*/false));

    body->emplaceStmt(&NLExecutor::runDeleteEdge, data);
}

const ColumnVector<CustomBool>* NLTranslator::getMaskColumn(mlir::Value chunkValue) const {
    if (!chunkValue) {
        return nullptr;
    }

    return static_cast<const ColumnVector<CustomBool>*>(getColumn(chunkValue));
}

NLKeyAppendFunction NLTranslator::selectMergeKeyAppend(mlir::Type chunkType,
                                                      const Column* column,
                                                      ValueType keyType) {
    if (isUntypedNullChunkType(chunkType)) {
        return NLExecutor::selectNullMergeKeyAppendFunction();
    }

    if (isConstantColumn(column)) {
        return NLExecutor::selectConstMergeKeyAppendFunction(valueTypeFromChunkType(chunkType), keyType);
    }

    const mlir::Type elementType = mlir::cast<nl::ChunkType>(chunkType).getElementType();

    if (const auto nullableType = mlir::dyn_cast<storage::NullableType>(elementType)) {
        return NLExecutor::selectOptMergeKeyAppendFunction(chunkKindFromNullableValue(nullableType.getValueType()), keyType);
    }

    return NLExecutor::selectPlainMergeKeyAppendFunction(chunkKindFromElementType(elementType), keyType);
}

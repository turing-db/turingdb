#include "NLTranslator.h"

#include <algorithm>
#include <optional>
#include <span>

#include "NLOps.h"

#include "mlir/IR/BuiltinAttributes.h"
#include "llvm/ADT/DenseMap.h"
#include "llvm/ADT/SmallVector.h"

#include "columns/ColumnOptVector.h"
#include "metadata/GraphMetadata.h"
#include "metadata/LabelSet.h"
#include "metadata/LabelSetMap.h"
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

namespace {

// The with-null fetch handler for a property's value type, on the node side
// when isNode is true and the edge side otherwise. Selecting it here keeps the
// value-type dispatch with the rest of translation; the handler bodies live in
// NLExecutor.
NLHandlerFunction selectPropertyFetchHandler(bool isNode, ValueType valueType) {
    // The handler is the runPropertyFetch instantiation for the node or edge ID
    // type and the property's value type; ValueTypeDispatcher maps the runtime
    // value type to that compile-time type, so the per-type fan-out lives in one
    // shared place rather than a switch repeated across the lowering.
    NLHandlerFunction handler = nullptr;
    const auto select = [&]<SupportedType T>() {
        handler = isNode ? &NLExecutor::runPropertyFetch<NodeID, T>
                         : &NLExecutor::runPropertyFetch<EdgeID, T>;
    };
    ValueTypeDispatcher(valueType).execute(select);

    return handler;
}

}

void NLTranslator::translatePropertyFetch(mlir::Value inputValue,
                                          mlir::Value propertyTypeValue,
                                          mlir::Value pendingValue,
                                          bool allPending,
                                          mlir::Value resultValue,
                                          bool isNode,
                                          NLStmtContainer* body) {
    // The property name lives on the nl.get_property_type that produced the handle
    nl::GetPropertyType handleOp = propertyTypeValue.getDefiningOp<nl::GetPropertyType>();
    if (!handleOp) {
        throw IRException("property_type operand must come from nl.get_property_type");
    }

    const llvm::StringRef name = handleOp.getName();

    // Resolve the name against the schema once, here, so execution works from a
    // PropertyTypeID and value type and never sees the name again. A CREATE earlier in the
    // program may have introduced the name, which puts it in the change's own schema and
    // nowhere else until the commit
    const std::optional<PropertyType> propertyType = findPropertyType(name);
    if (!propertyType) {
        throw IRException("Unknown property '" + name.str() + "'");
    }

    const ValueType valueType = propertyType->_valueType;

    const Column* input = getColumn(inputValue);
    Column* output = allocOptColumnForValueType(valueType);
    _valueSlots[resultValue] = output;

    NLPropertyFetchData* fetchData = _program->allocFunctionData<NLPropertyFetchData>(input,
                                                                                      output,
                                                                                      propertyType->_id);
    fetchData->setPending(getMaskColumn(pendingValue));
    fetchData->setAllPending(allPending || isPendingValue(inputValue, isNode));

    const NLHandlerFunction handler = selectPropertyFetchHandler(isNode, valueType);
    body->emplaceStmt(handler, fetchData);
}

void NLTranslator::translateGetNodeLabelSet(nl::GetNodeLabelSet op, NLStmtContainer* body) {
    const mlir::TypedValue<::mlir::nl::ChunkType> nodes = op.getInputNodes();
    const Column* nodeCol = getColumn(nodes);
    const ColumnNodeIDs* input = static_cast<const ColumnNodeIDs*>(nodeCol);

    ColumnLabelSetIDs* output = _memory->alloc<ColumnLabelSetIDs>();
    output->reserve(_program->getChunkSize());
    _valueSlots[op.getLabelSetIds()] = output;

    NLGetNodeLabelSetData* data = _program->allocFunctionData<NLGetNodeLabelSetData>(input, output);
    body->emplaceStmt(&NLExecutor::runGetNodeLabelSet, data);
}

void NLTranslator::translateGetEdgeTypes(nl::GetEdgeTypes op, NLStmtContainer* body) {
    const mlir::TypedValue<::mlir::nl::ChunkType> edges = op.getInputEdges();
    const Column* edgeCol = getColumn(edges);
    const ColumnEdgeIDs* input = static_cast<const ColumnEdgeIDs*>(edgeCol);

    ColumnEdgeTypes* output = _memory->alloc<ColumnEdgeTypes>();
    output->reserve(_program->getChunkSize());
    _valueSlots[op.getEdgeTypeIds()] = output;

    NLGetEdgeTypesData* data = _program->allocFunctionData<NLGetEdgeTypesData>(input, output);
    body->emplaceStmt(&NLExecutor::runGetEdgeTypes, data);
}

void NLTranslator::translateCheckLabelConstraint(nl::CheckLabelConstraint op, NLStmtContainer* body) {
    const mlir::TypedValue<::mlir::nl::ChunkType> lbls = op.getLabelsetIds();
    const Column* lblsCol = getColumn(lbls);
    bioassert(lblsCol->getKind() == ColumnLabelSetIDs::staticKind(), "nl.check_label_constraint reads a chunk of label sets");
    const ColumnLabelSetIDs* input = static_cast<const ColumnLabelSetIDs*>(lblsCol);

    const bool nullable = isNullableChunkType(op.getResult().getType());
    Column* output = allocCheckMask(nullable);
    _valueSlots[op.getResult()] = output;

    NLCheckLabelConstraintData* data = _program->allocFunctionData<NLCheckLabelConstraintData>(input, output);

    // Each alternative is a conjunction, so one label no node has ever carried makes that
    // alternative match nothing, where dropping the missing label would test a weaker
    // constraint than the query wrote.
    llvm::SmallVector<LabelSet, 4> constraints;
    for (const mlir::Attribute alternative : op.getAlternatives()) {
        llvm::SmallVector<llvm::StringRef> labels;
        resolveLabelNames(mlir::cast<mlir::ArrayAttr>(alternative), labels);

        LabelSet constraint;
        if (resolveLabelSet(labels, constraint)) {
            constraints.push_back(constraint);
        }
    }

    if (!constraints.empty()) {
        collectMatchingLabelSets(constraints, data);
    }

    if (nullable) {
        body->emplaceStmt(&NLExecutor::runCheckLabelConstraint<ColumnOptVector<CustomBool>>, data);
    } else {
        body->emplaceStmt(&NLExecutor::runCheckLabelConstraint<ColumnVector<CustomBool>>, data);
    }
}

Column* NLTranslator::allocCheckMask(bool nullable) {
    const size_t chunkSize = _program->getChunkSize();

    if (nullable) {
        ColumnOptVector<CustomBool>* mask = _memory->alloc<ColumnOptVector<CustomBool>>();
        mask->reserve(chunkSize);
        return mask;
    }

    ColumnVector<CustomBool>* mask = _memory->alloc<ColumnVector<CustomBool>>();
    mask->reserve(chunkSize);

    return mask;
}

void NLTranslator::collectMatchingLabelSets(std::span<const LabelSet> constraints, NLCheckLabelConstraintData* data) const {
    const auto collectMatching = [constraints, data](LabelSetID id, const LabelSet& labelset) {
        const bool matches = std::any_of(constraints.begin(), constraints.end(), [&labelset](const LabelSet& constraint) {
            return labelset.hasAtLeastLabels(constraint);
        });

        if (matches) {
            data->addMatchingID(id);
        }
    };

    if (_metadataBuilder) {
        _metadataBuilder->forEachLabelSet(collectMatching);
    } else {
        for (const LabelSetMap::Pair& pair : _view->metadata().labelsets()) {
            collectMatching(pair._id, *pair._value);
        }
    }
}

void NLTranslator::translateCheckEdgeTypeConstraint(nl::CheckEdgeTypeConstraint op, NLStmtContainer* body) {
    const mlir::TypedValue<::mlir::nl::ChunkType> etypes = op.getEdgeTypeIds();
    const Column* etypesCol = getColumn(etypes);
    bioassert(etypesCol->getKind() == ColumnEdgeTypes::staticKind(), "nl.check_edge_type_constraint reads a chunk of edge types");
    const ColumnEdgeTypes* input = static_cast<const ColumnEdgeTypes*>(etypesCol);

    const bool nullable = isNullableChunkType(op.getResult().getType());
    Column* output = allocCheckMask(nullable);
    _valueSlots[op.getResult()] = output;

    NLCheckEdgeTypeConstraintData* data = _program->allocFunctionData<NLCheckEdgeTypeConstraintData>(input, output);

    llvm::SmallVector<EdgeTypeID, 4> matchingTypes;
    resolveEdgeTypes(op.getEdgeTypes(), matchingTypes);

    for (const EdgeTypeID edgeTypeID : matchingTypes) {
        data->addMatchingID(edgeTypeID);
    }

    if (nullable) {
        body->emplaceStmt(&NLExecutor::runCheckEdgeTypeConstraint<ColumnOptVector<CustomBool>>, data);
    } else {
        body->emplaceStmt(&NLExecutor::runCheckEdgeTypeConstraint<ColumnVector<CustomBool>>, data);
    }
}

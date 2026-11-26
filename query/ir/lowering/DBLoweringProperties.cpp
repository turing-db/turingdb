#include "DBLowering.h"

#include "mlir/IR/Builders.h"
#include "mlir/IR/Types.h"

#include "NLOps.h"

using namespace db;

namespace nl = mlir::nl;
namespace storage = mlir::storage;

void DBLowering::lowerGetNodeProperties(mlir::db::GetNodeProperties getNodeProperties) {
    const mlir::Value inputChunk = mapValue(getNodeProperties.getInputNodes());
    const llvm::StringRef property = getNodeProperties.getProperty();

    // Resolve the name once, hoisted above the loops.
    const mlir::Value handle = getOrCreatePropertyTypeHandle(property);
    const mlir::Type valueChunkType = nl::ChunkType::get(_builder.getContext(), columnType(getNodeProperties.getResult()));

    // A property read maps the input chunk in place, one value per node, so the
    // fetch nests in the loop that binds that chunk - it opens no loop of its own.
    setInsertionInto(ownerBlock(inputChunk));

    nl::GetNodeProperties fetch = _builder.create<nl::GetNodeProperties>(_builder.getUnknownLoc(),
                                                                         valueChunkType,
                                                                         inputChunk,
                                                                         handle,
                                                                         mapOptionalMask(getNodeProperties.getPending()),
                                                                         getNodeProperties.getAllPending());
    _valueMap[getNodeProperties.getResult()] = fetch.getValues();
}

void DBLowering::lowerGetEdgeProperties(mlir::db::GetEdgeProperties getEdgeProperties) {
    const mlir::Value inputChunk = mapValue(getEdgeProperties.getInputEdges());
    const llvm::StringRef property = getEdgeProperties.getProperty();

    const mlir::Value handle = getOrCreatePropertyTypeHandle(property);
    const mlir::Type valueChunkType = nl::ChunkType::get(_builder.getContext(), columnType(getEdgeProperties.getResult()));

    setInsertionInto(ownerBlock(inputChunk));

    nl::GetEdgeProperties fetch = _builder.create<nl::GetEdgeProperties>(_builder.getUnknownLoc(),
                                                                         valueChunkType,
                                                                         inputChunk,
                                                                         handle,
                                                                         mapOptionalMask(getEdgeProperties.getPending()),
                                                                         getEdgeProperties.getAllPending());
    _valueMap[getEdgeProperties.getResult()] = fetch.getValues();
}

void DBLowering::lowerGetNodeLabelSet(mlir::db::GetNodeLabelSet getNodeLabelSet) {
    const mlir::Value inputChunk = mapValue(getNodeLabelSet.getInputNodes());

    setInsertionInto(ownerBlock(inputChunk));

    const mlir::Type labelSetIDChunkType = nl::ChunkType::get(
        _builder.getContext(),
        storage::LabelSetIDType::get(_builder.getContext()));

    nl::GetNodeLabelSet fetch = _builder.create<nl::GetNodeLabelSet>(
        _builder.getUnknownLoc(),
        labelSetIDChunkType,
        inputChunk);

    _valueMap[getNodeLabelSet.getResult()] = fetch.getLabelSetIds();
}

void DBLowering::lowerGetEdgeTypes(mlir::db::GetEdgeTypes getEdgeTypes) {
    const mlir::Value inputChunk = mapValue(getEdgeTypes.getInputEdges());

    setInsertionInto(ownerBlock(inputChunk));

    const mlir::Type edgeTypeIDChunkType = nl::ChunkType::get(
        _builder.getContext(),
        storage::EdgeTypeIDType::get(_builder.getContext()));

    nl::GetEdgeTypes fetch = _builder.create<nl::GetEdgeTypes>(
        _builder.getUnknownLoc(),
        edgeTypeIDChunkType,
        inputChunk);

    _valueMap[getEdgeTypes.getResult()] = fetch.getEdgeTypeIds();
}

void DBLowering::lowerCheckLabelConstraint(mlir::db::CheckLabelConstraint checkLabelConstraint) {
    const mlir::Value inputChunk = mapValue(checkLabelConstraint.getLabelsetIds());

    setInsertionInto(ownerBlock(inputChunk));

    const mlir::Type boolChunkType = checkResultChunkType(checkLabelConstraint.getNullable());

    nl::CheckLabelConstraint check = _builder.create<nl::CheckLabelConstraint>(
        _builder.getUnknownLoc(),
        boolChunkType,
        inputChunk,
        checkLabelConstraint.getAlternatives());

    _valueMap[checkLabelConstraint.getResult()] = check.getResult();
}

void DBLowering::lowerCheckEdgeTypeConstraint(mlir::db::CheckEdgeTypeConstraint checkEdgeTypeConstraint) {
    const mlir::Value inputChunk = mapValue(checkEdgeTypeConstraint.getEdgeTypeIds());

    setInsertionInto(ownerBlock(inputChunk));

    const mlir::Type boolChunkType = checkResultChunkType(checkEdgeTypeConstraint.getNullable());

    nl::CheckEdgeTypeConstraint check = _builder.create<nl::CheckEdgeTypeConstraint>(
        _builder.getUnknownLoc(),
        boolChunkType,
        inputChunk,
        checkEdgeTypeConstraint.getEdgeTypes());

    _valueMap[checkEdgeTypeConstraint.getResult()] = check.getResult();
}

mlir::Type DBLowering::checkResultChunkType(bool nullable) {
    mlir::MLIRContext* const context = _builder.getContext();
    if (nullable) {
        return nl::ChunkType::get(context, storage::NullableType::get(context, _builder.getI1Type()));
    }

    return nl::ChunkType::get(context, storage::BoolType::get(context));
}

mlir::Value DBLowering::getOrCreatePropertyTypeHandle(llvm::StringRef propertyName) {
    const auto existing = _propertyTypes.find(propertyName);
    if (existing != _propertyTypes.end()) {
        return existing->second;
    }

    // The handle reads no chunk, so it joins the entry block's hoisted run, above every
    // loop, where it dominates all the fetches that use it.
    setInsertionAfterHoisted();

    nl::GetPropertyType handleOp = _builder.create<nl::GetPropertyType>(_builder.getUnknownLoc(),
                                                                        _builder.getStringAttr(propertyName));
    _lastHoisted = handleOp.getOperation();

    const mlir::Value handle = handleOp.getResult();
    _propertyTypes[propertyName] = handle;

    return handle;
}

mlir::Value DBLowering::getOrCreateEdgeTypeSetHandle(mlir::ArrayAttr edgeTypeNames) {
    // The edge sibling of getOrCreatePropertyTypeHandle: dedup per set of names and
    // hoist the handle into the entry block's hoisted run, above every loop, so a by-type
    // hop nested in a loop reuses one resolved handle rather than re-carrying them.
    const auto existing = _edgeTypeSets.find(edgeTypeNames);
    if (existing != _edgeTypeSets.end()) {
        return existing->second;
    }

    setInsertionAfterHoisted();

    nl::GetEdgeTypeSet handleOp = _builder.create<nl::GetEdgeTypeSet>(_builder.getUnknownLoc(),
                                                                     edgeTypeNames);
    _lastHoisted = handleOp.getOperation();

    const mlir::Value handle = handleOp.getResult();
    _edgeTypeSets[edgeTypeNames] = handle;

    return handle;
}

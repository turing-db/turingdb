#include "DBLowering.h"

#include <mlir/IR/Location.h>
#include <optional>

#include "mlir/IR/Block.h"
#include "mlir/IR/Builders.h"
#include "mlir/IR/Types.h"

#include "NLOps.h"
#include "DBLoweringChunkTypes.h"

#include "MergePatternShape.h"

#include "views/GraphView.h"
#include "metadata/GraphMetadata.h"
#include "metadata/PropertyType.h"

#include "llvm/Support/Casting.h"

using namespace db;

namespace nl = mlir::nl;
namespace storage = mlir::storage;

mlir::Value DBLowering::propertyWriteChunk(llvm::StringRef propertyName, mlir::Value chunk) {
    if (!_view || !_view->isValid()) {
        return chunk;
    }

    // if the property type is a double in the db, but an integer was provided, convert
    // that int to double
    const GraphMetadata& metadata = _view->metadata();
    const PropertyTypeMap& propTypes = metadata.propTypes();
    const std::optional<PropertyType> propertyType = propTypes.get(propertyName);
    if (!propertyType || propertyType->_valueType != ValueType::Double) {
        return chunk;
    }

    const mlir::Type element = chunkValueElement(_builder, chunk.getType());
    const auto integerElement = mlir::dyn_cast<mlir::IntegerType>(element);
    if (!integerElement || integerElement.getWidth() != 64) {
        return chunk;
    }

    return chunkAsElement(chunk, _builder.getF64Type());
}

void DBLowering::coercePropertyWriteChunks(mlir::ArrayAttr names,
                                           llvm::SmallVectorImpl<mlir::Value>& chunks) {
    for (size_t index = 0; index < chunks.size(); index++) {
        const llvm::StringRef name = mlir::cast<mlir::StringAttr>(names[index]).getValue();

        chunks[index] = propertyWriteChunk(name, chunks[index]);
    }
}

void DBLowering::coerceGroupedPropertyWriteChunks(mlir::ArrayAttr groupedNames,
                                                  llvm::SmallVectorImpl<mlir::Value>& chunks) {
    size_t index = 0;

    for (const mlir::Attribute group : groupedNames) {
        for (const mlir::Attribute name : mlir::cast<mlir::ArrayAttr>(group)) {
            chunks[index] = propertyWriteChunk(mlir::cast<mlir::StringAttr>(name).getValue(), chunks[index]);
            index++;
        }
    }
}

void DBLowering::lowerCreateNode(mlir::db::CreateNode createNode) {
    const mlir::Location loc = _builder.getUnknownLoc();

    llvm::SmallVector<mlir::Value, 4> propChunks;
    for (const mlir::Value propValue : createNode.getPropValues()) {
        propChunks.push_back(mapValue(propValue));
    }

    coercePropertyWriteChunks(createNode.getPropNames(), propChunks);

    // A constant stands for the rows in flight, which is how many nodes the op writes
    mlir::Value cardinalityChunk;
    if (createNode.getCardinality()) {
        cardinalityChunk = mapValue(createNode.getCardinality());

        const mlir::Value rows = rowsInFlight();
        if (rows) {
            cardinalityChunk = rowAlignedChunk(cardinalityChunk, rows);
        }
    }

    if (cardinalityChunk) {
        setInsertionInto(ownerBlock(cardinalityChunk));
    } else {
        mlir::Block* targetBlock = _rootBlock;
        for (const mlir::OpOperand& use : createNode.getResult().getUses()) {
            auto createEdge = mlir::dyn_cast<mlir::db::CreateEdge>(use.getOwner());
            if (!createEdge) {
                continue;
            }
            for (const mlir::Value candidate : {createEdge.getSrcIds(), createEdge.getTgtIds()}) {
                if (candidate == createNode.getResult()) {
                    continue;
                }
                const auto it = _valueMap.find(candidate);
                if (it == _valueMap.end()) {
                    continue;
                }
                mlir::Block* const candidateBlock = ownerBlock(it->second);
                if (candidateBlock != _rootBlock) {
                    targetBlock = candidateBlock;
                    break;
                }
            }
            if (targetBlock != _rootBlock) {
                break;
            }
        }

        setInsertionInto(targetBlock);
    }

    nl::CreateNode create = _builder.create<nl::CreateNode>(
        loc,
        createNode.getLabelsAttr(),
        createNode.getPropNamesAttr(),
        propChunks,
        cardinalityChunk);
    _valueMap[createNode.getResult()] = create.getResult();
}

void DBLowering::lowerCreateEdge(mlir::db::CreateEdge createEdge) {
    const mlir::Location loc = _builder.getUnknownLoc();
    const mlir::Value srcChunk = mapValue(createEdge.getSrcIds());
    const mlir::Value tgtChunk = mapValue(createEdge.getTgtIds());

    llvm::SmallVector<mlir::Value, 4> propChunks;
    for (const mlir::Value propValue : createEdge.getPropValues()) {
        propChunks.push_back(mapValue(propValue));
    }

    coercePropertyWriteChunks(createEdge.getPropNames(), propChunks);

    llvm::SmallVector<mlir::Value, 8> operandChunks {srcChunk, tgtChunk};
    operandChunks.append(propChunks.begin(), propChunks.end());

    const mlir::Value reference = deepestBoundChunk(operandChunks);
    setInsertionInto(ownerBlock(reference));

    nl::CreateEdge create = _builder.create<nl::CreateEdge>(
        loc,
        srcChunk,
        tgtChunk,
        createEdge.getEdgeTypeAttr(),
        createEdge.getPropNamesAttr(),
        propChunks,
        mapOptionalMask(createEdge.getSrcPending()),
        mapOptionalMask(createEdge.getTgtPending()),
        createEdge.getSrcAllPending(),
        createEdge.getTgtAllPending());
    _valueMap[createEdge.getResult()] = create.getResult();
}

mlir::Value DBLowering::mapOptionalMask(mlir::Value mask) {
    if (!mask) {
        return mlir::Value();
    }

    return pendingMaskChunk(mapValue(mask));
}

// An OPTIONAL CALL pads the mask of a merged entity it returns with null, and the row it
// pads names no entity, so none this change wrote
mlir::Value DBLowering::pendingMaskChunk(mlir::Value chunk) {
    if (!isNullableChunk(chunk.getType())) {
        return chunk;
    }

    const mlir::Location loc = _builder.getUnknownLoc();
    mlir::MLIRContext* const context = _builder.getContext();
    const nl::ChunkType maskType = nl::ChunkType::get(context, storage::BoolType::get(context));

    const mlir::Value notPending = _builder.create<nl::IsNotTrue>(loc, maskType, chunk).getResult();
    return _builder.create<nl::Not>(loc, maskType, notPending).getResult();
}

void DBLowering::mapColumns(mlir::OperandRange columns, llvm::SmallVectorImpl<mlir::Value>& chunks) {
    chunks.clear();
    for (const mlir::Value column : columns) {
        chunks.push_back(mapValue(column));
    }
}

void DBLowering::lowerMerge(mlir::db::Merge merge) {
    llvm::SmallVector<mlir::Value, 8> boundNodes;
    llvm::SmallVector<mlir::Value, 8> boundPending;
    llvm::SmallVector<mlir::Value, 8> nodePropValues;
    llvm::SmallVector<mlir::Value, 8> edgePropValues;
    llvm::SmallVector<mlir::Value, 8> carriedColumns;

    mapColumns(merge.getBoundNodes(), boundNodes);
    mapColumns(merge.getBoundPending(), boundPending);

    for (mlir::Value& pending : boundPending) {
        pending = rowAlignedChunk(pending, boundNodes.front());
    }
    mapColumns(merge.getNodePropValues(), nodePropValues);
    mapColumns(merge.getEdgePropValues(), edgePropValues);
    mapColumns(merge.getCarriedColumns(), carriedColumns);

    coerceGroupedPropertyWriteChunks(merge.getNodePropNames(), nodePropValues);
    coerceGroupedPropertyWriteChunks(merge.getEdgePropNames(), edgePropValues);

    llvm::SmallVector<mlir::Value, 8> operandChunks(boundNodes);
    operandChunks.append(boundPending.begin(), boundPending.end());
    operandChunks.append(nodePropValues.begin(), nodePropValues.end());
    operandChunks.append(edgePropValues.begin(), edgePropValues.end());
    operandChunks.append(carriedColumns.begin(), carriedColumns.end());

    // Inserted into the deepest block, where all the operands are defined. A merge whose
    // operands are all bound above the root - literals alone - opens in the root, as a
    // CREATE does, so a subquery branch that is not taken merges nothing.
    mlir::Block* targetBlock = _rootBlock;
    if (const mlir::Value insertionReference = deepestBoundChunk(operandChunks)) {
        mlir::Block* const referenceBlock = ownerBlock(insertionReference);
        if (enclosesBlock(_rootBlock, referenceBlock)) {
            targetBlock = referenceBlock;
        }
    }

    setInsertionInto(targetBlock);

    for (mlir::Value& pending : boundPending) {
        pending = pendingMaskChunk(pending);
    }

    mlir::MLIRContext* const context = _builder.getContext();
    const mlir::Type nodeChunkType = nl::ChunkType::get(context, storage::NodeIDType::get(context));
    const mlir::Type edgeChunkType = nl::ChunkType::get(context, storage::EdgeIDType::get(context));
    const mlir::Type maskChunkType = nl::ChunkType::get(context, storage::BoolType::get(context));

    const mlir::ArrayAttr nodeLabels = merge.getNodeLabels();
    const size_t hopCount = nodeLabels.size() - 1;
    const size_t matchedNodeCount = mlir::mergeMatchedNodeCount(nodeLabels);

    llvm::SmallVector<mlir::Type, 8> resultTypes;
    for (size_t nodeIndex = 0; nodeIndex < matchedNodeCount; nodeIndex++) {
        resultTypes.push_back(nodeChunkType);
        resultTypes.push_back(maskChunkType);
    }

    for (size_t hopIndex = 0; hopIndex < hopCount; hopIndex++) {
        resultTypes.push_back(edgeChunkType);
        resultTypes.push_back(maskChunkType);
    }

    resultTypes.push_back(maskChunkType);

    for (const mlir::Value carriedChunk : carriedColumns) {
        resultTypes.push_back(carriedChunk.getType());
    }

    nl::Merge nlMerge = _builder.create<nl::Merge>(_builder.getUnknownLoc(),
                                                   resultTypes,
                                                   merge.getNodeLabelsAttr(),
                                                   merge.getNodePropNamesAttr(),
                                                   merge.getEdgeTypesAttr(),
                                                   merge.getEdgePropNamesAttr(),
                                                   merge.getEdgeDirectionsAttr(),
                                                   merge.getPendingNodesAttr(),
                                                   boundNodes,
                                                   boundPending,
                                                   nodePropValues,
                                                   edgePropValues,
                                                   carriedColumns);

    const mlir::ResultRange dbResults = merge.getResults();
    const mlir::ResultRange nlResults = nlMerge.getResults();
    for (size_t index = 0; index < dbResults.size(); index++) {
        _valueMap[dbResults[index]] = nlResults[index];
    }

    // The merge grows or shrinks the rows in flight, so from here on what sizes a
    // projection of constants alone is its own node chunk
    _innermostCardinality = nlResults.front();
}

void DBLowering::lowerSetNodeProperty(mlir::db::SetNodeProperty setNodeProperty) {
    const mlir::Location loc = _builder.getUnknownLoc();
    const mlir::Value inputChunk = mapValue(setNodeProperty.getInputNodes());
    const mlir::Value valueChunk = propertyWriteChunk(setNodeProperty.getProperty(),
                                                      mapValue(setNodeProperty.getValue()));

    setWriteInsertion(inputChunk, valueChunk);

    _builder.create<nl::SetNodeProperty>(
        loc,
        inputChunk,
        setNodeProperty.getPropertyAttr(),
        valueChunk,
        mapOptionalMask(setNodeProperty.getPending()),
        mapOptionalMask(setNodeProperty.getRows()),
        setNodeProperty.getAllPending());
}

void DBLowering::lowerSetEdgeProperty(mlir::db::SetEdgeProperty setEdgeProperty) {
    const mlir::Location loc = _builder.getUnknownLoc();
    const mlir::Value inputChunk = mapValue(setEdgeProperty.getInputEdges());
    const mlir::Value valueChunk = propertyWriteChunk(setEdgeProperty.getProperty(),
                                                      mapValue(setEdgeProperty.getValue()));

    setWriteInsertion(inputChunk, valueChunk);

    _builder.create<nl::SetEdgeProperty>(
        loc,
        inputChunk,
        setEdgeProperty.getPropertyAttr(),
        valueChunk,
        mapOptionalMask(setEdgeProperty.getPending()),
        mapOptionalMask(setEdgeProperty.getRows()),
        setEdgeProperty.getAllPending());
}

void DBLowering::lowerDeleteNode(mlir::db::DeleteNode deleteNode) {
    const mlir::Location loc = _builder.getUnknownLoc();
    const mlir::Value inputChunk = mapValue(deleteNode.getInputNodes());

    // The delete runs where its node chunk is live - the block that owns it.
    setInsertionInto(ownerBlock(inputChunk));

    _builder.create<nl::DeleteNode>(loc,
                                    inputChunk,
                                    deleteNode.getDetach(),
                                    mapOptionalMask(deleteNode.getPending()),
                                    deleteNode.getAllPending());
}

void DBLowering::lowerDeleteEdge(mlir::db::DeleteEdge deleteEdge) {
    const mlir::Location loc = _builder.getUnknownLoc();
    const mlir::Value inputChunk = mapValue(deleteEdge.getInputEdges());

    setInsertionInto(ownerBlock(inputChunk));

    _builder.create<nl::DeleteEdge>(loc,
                                    inputChunk,
                                    mapOptionalMask(deleteEdge.getPending()),
                                    deleteEdge.getAllPending());
}

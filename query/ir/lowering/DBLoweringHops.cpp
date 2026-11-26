#include "DBLowering.h"

#include <mlir/IR/Location.h>

#include "mlir/IR/Builders.h"

#include "NLOps.h"

using namespace db;

namespace nl = mlir::nl;

void DBLowering::lowerGetOutEdges(mlir::db::GetOutEdges getOutEdges) {
    // Map the input node column and the carry set to the nl chunks they lowered
    // to. The fetch nests in the loop that binds its input chunk.
    const mlir::Value inputChunk = mapValue(getOutEdges.getInputNodes());

    llvm::SmallVector<mlir::Value, 4> carriedChunks;
    for (const mlir::Value carriedColumn : getOutEdges.getColumnsToFilter()) {
        carriedChunks.push_back(mapValue(carriedColumn));
    }

    setInsertionInto(ownerBlock(inputChunk));

    // The result iterator type - the four fixed edge chunks plus one per
    // carried chunk - is inferred from the operands.
    nl::GetOutEdges edges = _builder.create<nl::GetOutEdges>(_builder.getUnknownLoc(), inputChunk, carriedChunks);
    buildLoopForSource(edges.getResult(), getOutEdges.getOperation());
}

void DBLowering::lowerGetInEdges(mlir::db::GetInEdges getInEdges) {
    // The predecessor counterpart of lowerGetOutEdges: same shape, reverse
    // direction. Map the input node column and the carry set to the nl chunks
    // they lowered to. The fetch nests in the loop that binds its input chunk.
    const mlir::Value inputChunk = mapValue(getInEdges.getInputNodes());

    llvm::SmallVector<mlir::Value, 4> carriedChunks;
    for (const mlir::Value carriedColumn : getInEdges.getColumnsToFilter()) {
        carriedChunks.push_back(mapValue(carriedColumn));
    }

    setInsertionInto(ownerBlock(inputChunk));

    // The result iterator type - the four fixed edge chunks plus one per
    // carried chunk - is inferred from the operands.
    nl::GetInEdges edges = _builder.create<nl::GetInEdges>(_builder.getUnknownLoc(), inputChunk, carriedChunks);
    buildLoopForSource(edges.getResult(), getInEdges.getOperation());
}

void DBLowering::lowerGetEdges(mlir::db::GetEdges getEdges) {
    const mlir::Value inputChunk = mapValue(getEdges.getInputNodes());

    llvm::SmallVector<mlir::Value, 4> carriedChunks;
    for (const mlir::Value carriedColumn : getEdges.getColumnsToFilter()) {
        carriedChunks.push_back(mapValue(carriedColumn));
    }

    setInsertionInto(ownerBlock(inputChunk));


    const mlir::Location uloc = _builder.getUnknownLoc();
    nl::GetEdges edges = _builder.create<nl::GetEdges>(uloc, inputChunk, carriedChunks);
    buildLoopForSource(edges.getResult(), getEdges.getOperation());
}

void DBLowering::lowerGetOutEdgesByType(mlir::db::GetOutEdgesByType getOutEdgesByType) {
    const mlir::Value inputChunk = mapValue(getOutEdgesByType.getInputNodes());
    const mlir::Value edgeTypeHandle = getOrCreateEdgeTypeSetHandle(getOutEdgesByType.getEdgeTypes());

    llvm::SmallVector<mlir::Value, 4> carriedChunks;
    for (const mlir::Value carriedColumn : getOutEdgesByType.getColumnsToFilter()) {
        carriedChunks.push_back(mapValue(carriedColumn));
    }

    setInsertionInto(ownerBlock(inputChunk));

    nl::GetOutEdgesByType edges = _builder.create<nl::GetOutEdgesByType>(_builder.getUnknownLoc(),
                                                                         inputChunk,
                                                                         edgeTypeHandle,
                                                                         carriedChunks);
    buildLoopForSource(edges.getResult(), getOutEdgesByType.getOperation());
}

void DBLowering::lowerGetInEdgesByType(mlir::db::GetInEdgesByType getInEdgesByType) {
    // The predecessor counterpart of lowerGetOutEdgesByType: same shape, reverse
    // direction, edge types hoisted into the same nl.get_edge_type_set handle.
    const mlir::Value inputChunk = mapValue(getInEdgesByType.getInputNodes());
    const mlir::Value edgeTypeHandle = getOrCreateEdgeTypeSetHandle(getInEdgesByType.getEdgeTypes());

    llvm::SmallVector<mlir::Value, 4> carriedChunks;
    for (const mlir::Value carriedColumn : getInEdgesByType.getColumnsToFilter()) {
        carriedChunks.push_back(mapValue(carriedColumn));
    }

    setInsertionInto(ownerBlock(inputChunk));

    // The result iterator type - the four fixed edge chunks plus one per carried
    // chunk - is inferred from the operands, exactly as for get_in_edges.
    nl::GetInEdgesByType edges = _builder.create<nl::GetInEdgesByType>(_builder.getUnknownLoc(),
                                                                       inputChunk,
                                                                       edgeTypeHandle,
                                                                       carriedChunks);
    buildLoopForSource(edges.getResult(), getInEdgesByType.getOperation());
}

void DBLowering::lowerGetOutEdgesByLabel(mlir::db::GetOutEdgesByLabel getOutEdgesByLabel) {
    const mlir::Value inputChunk = mapValue(getOutEdgesByLabel.getInputNodes());

    llvm::SmallVector<mlir::Value, 4> carriedChunks;
    for (const mlir::Value carriedColumn : getOutEdgesByLabel.getColumnsToFilter()) {
        carriedChunks.push_back(mapValue(carriedColumn));
    }

    setInsertionInto(ownerBlock(inputChunk));

    // The label list rides on the op the way a by-label scan carries it, rather than
    // through a handle: it is resolved to a LabelSet when the loop is translated.
    nl::GetOutEdgesByLabel edges = _builder.create<nl::GetOutEdgesByLabel>(_builder.getUnknownLoc(),
                                                                           inputChunk,
                                                                           getOutEdgesByLabel.getLabelsAttr(),
                                                                           carriedChunks);
    buildLoopForSource(edges.getResult(), getOutEdgesByLabel.getOperation());
}

void DBLowering::lowerGetInEdgesByLabel(mlir::db::GetInEdgesByLabel getInEdgesByLabel) {
    // The predecessor counterpart of lowerGetOutEdgesByLabel: same shape, reverse
    // direction, the labels constraining the source the hop leaves.
    const mlir::Value inputChunk = mapValue(getInEdgesByLabel.getInputNodes());

    llvm::SmallVector<mlir::Value, 4> carriedChunks;
    for (const mlir::Value carriedColumn : getInEdgesByLabel.getColumnsToFilter()) {
        carriedChunks.push_back(mapValue(carriedColumn));
    }

    setInsertionInto(ownerBlock(inputChunk));

    nl::GetInEdgesByLabel edges = _builder.create<nl::GetInEdgesByLabel>(_builder.getUnknownLoc(),
                                                                         inputChunk,
                                                                         getInEdgesByLabel.getLabelsAttr(),
                                                                         carriedChunks);
    buildLoopForSource(edges.getResult(), getInEdgesByLabel.getOperation());
}

void DBLowering::lowerGetOutEdgesByTypeAndLabel(mlir::db::GetOutEdgesByTypeAndLabel getOutEdgesByTypeAndLabel) {
    const mlir::Value inputChunk = mapValue(getOutEdgesByTypeAndLabel.getInputNodes());
    const mlir::Value edgeTypeHandle = getOrCreateEdgeTypeSetHandle(getOutEdgesByTypeAndLabel.getEdgeTypes());

    llvm::SmallVector<mlir::Value, 4> carriedChunks;
    for (const mlir::Value carriedColumn : getOutEdgesByTypeAndLabel.getColumnsToFilter()) {
        carriedChunks.push_back(mapValue(carriedColumn));
    }

    setInsertionInto(ownerBlock(inputChunk));

    nl::GetOutEdgesByTypeAndLabel edges = _builder.create<nl::GetOutEdgesByTypeAndLabel>(_builder.getUnknownLoc(),
                                                                                         inputChunk,
                                                                                         edgeTypeHandle,
                                                                                         getOutEdgesByTypeAndLabel.getLabelsAttr(),
                                                                                         carriedChunks);
    buildLoopForSource(edges.getResult(), getOutEdgesByTypeAndLabel.getOperation());
}

void DBLowering::lowerGetInEdgesByTypeAndLabel(mlir::db::GetInEdgesByTypeAndLabel getInEdgesByTypeAndLabel) {
    const mlir::Value inputChunk = mapValue(getInEdgesByTypeAndLabel.getInputNodes());
    const mlir::Value edgeTypeHandle = getOrCreateEdgeTypeSetHandle(getInEdgesByTypeAndLabel.getEdgeTypes());

    llvm::SmallVector<mlir::Value, 4> carriedChunks;
    for (const mlir::Value carriedColumn : getInEdgesByTypeAndLabel.getColumnsToFilter()) {
        carriedChunks.push_back(mapValue(carriedColumn));
    }

    setInsertionInto(ownerBlock(inputChunk));

    nl::GetInEdgesByTypeAndLabel edges = _builder.create<nl::GetInEdgesByTypeAndLabel>(_builder.getUnknownLoc(),
                                                                                       inputChunk,
                                                                                       edgeTypeHandle,
                                                                                       getInEdgesByTypeAndLabel.getLabelsAttr(),
                                                                                       carriedChunks);
    buildLoopForSource(edges.getResult(), getInEdgesByTypeAndLabel.getOperation());
}

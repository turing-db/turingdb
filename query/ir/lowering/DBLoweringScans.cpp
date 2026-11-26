#include "DBLowering.h"

#include "mlir/IR/Builders.h"
#include "mlir/IR/Types.h"

#include "NLOps.h"

using namespace db;

namespace nl = mlir::nl;
namespace storage = mlir::storage;

void DBLowering::lowerScanNodes(mlir::db::ScanNodes scanNodes) {
    // A scan reads no column, so its loop sits at the top of the current root
    // block: the function entry at top level, or - inside a cross product - the
    // outer factor's innermost loop body, so the inner factor nests under it.
    setInsertionInto(_rootBlock);

    nl::ScanNodes nodes = _builder.create<nl::ScanNodes>(_builder.getUnknownLoc());
    buildLoopForSource(nodes.getResult(), scanNodes.getOperation());
}

void DBLowering::lowerScanNodesByLabel(mlir::db::ScanNodesByLabel scanNodesByLabel) {
    // The label sibling of lowerScanNodes: a scan reads no column, so its loop
    // sits at the top of the current root block. The label list is a filter on
    // the rows, not a column input, so it is forwarded as-is to the nl op -
    // translation resolves the names against the schema, the same way the
    // property name on nl.get_property_type is resolved by its consumer.
    setInsertionInto(_rootBlock);

    nl::ScanNodesByLabel nodes = _builder.create<nl::ScanNodesByLabel>(_builder.getUnknownLoc(),
                                                                       scanNodesByLabel.getLabelsAttr());
    buildLoopForSource(nodes.getResult(), scanNodesByLabel.getOperation());
}

void DBLowering::lowerConstScanNodes(mlir::db::ConstScanNodes constScanNodes) {
    // The constant-set sibling of lowerScanNodes: a scan reads no row, so its
    // loop sits at the top of the current root block. The node ID list is the set
    // of rows to emit, not a column input, so it is forwarded as-is to the nl op -
    // translation resolves each entry to a storage NodeID.
    setInsertionInto(_rootBlock);

    llvm::SmallVector<mlir::Value, 2> boundIDs;
    for (const mlir::Value bound : constScanNodes.getBoundIDs()) {
        boundIDs.push_back(mapValue(bound));
    }

    nl::ConstScanNodes nodes = _builder.create<nl::ConstScanNodes>(_builder.getUnknownLoc(),
                                                                   constScanNodes.getNodeIDsAttr(),
                                                                   boundIDs);
    buildLoopForSource(nodes.getResult(), constScanNodes.getOperation());
}

void DBLowering::lowerScanNodesByPropertyValue(mlir::db::ScanNodesByPropertyValue scanNodesByPropertyValue) {
    setInsertionInto(_rootBlock);

    const mlir::Value bound = scanNodesByPropertyValue.getBoundValue();
    const mlir::Value boundChunk = bound ? mapValue(bound) : mlir::Value();

    nl::ScanNodesByPropertyValue nodes = _builder.create<nl::ScanNodesByPropertyValue>(_builder.getUnknownLoc(),
                                                                                       scanNodesByPropertyValue.getPropertyAttr(),
                                                                                       scanNodesByPropertyValue.getValueAttr(),
                                                                                       boundChunk,
                                                                                       scanNodesByPropertyValue.getLabelsAttr());
    buildLoopForSource(nodes.getResult(), scanNodesByPropertyValue.getOperation());
}

void DBLowering::lowerScanEdges(mlir::db::ScanEdges scanEdges) {
    // The edge sibling of lowerScanNodes: a scan reads no column, so its loop
    // sits at the top of the current root block. The nl.scan_edges iterator
    // produces the four fixed edge chunks, which buildLoopForSource binds to the
    // op's four results in order.
    setInsertionInto(_rootBlock);

    nl::ScanEdges edges = _builder.create<nl::ScanEdges>(_builder.getUnknownLoc());
    buildLoopForSource(edges.getResult(), scanEdges.getOperation());
}

void DBLowering::lowerConstScanEdges(mlir::db::ConstScanEdges constScanEdges) {
    setInsertionInto(_rootBlock);

    llvm::SmallVector<mlir::Value, 2> boundIDs;
    for (const mlir::Value bound : constScanEdges.getBoundIDs()) {
        boundIDs.push_back(mapValue(bound));
    }

    nl::ConstScanEdges edges = _builder.create<nl::ConstScanEdges>(_builder.getUnknownLoc(),
                                                                   constScanEdges.getEdgeIDsAttr(),
                                                                   boundIDs,
                                                                   constScanEdges.getUndirectedAttr());
    buildLoopForSource(edges.getResult(), constScanEdges.getOperation());
}

void DBLowering::lowerScanEdgesByType(mlir::db::ScanEdgesByType scanEdgesByType) {
    // The by-type sibling of lowerScanEdges: same placement at the top of the root
    // block, with the type names hoisted into the nl.get_edge_type_set handle the
    // by-type hops already share.
    const mlir::Value edgeTypeHandle = getOrCreateEdgeTypeSetHandle(scanEdgesByType.getEdgeTypes());

    setInsertionInto(_rootBlock);

    nl::ScanEdgesByType edges = _builder.create<nl::ScanEdgesByType>(_builder.getUnknownLoc(), edgeTypeHandle);
    buildLoopForSource(edges.getResult(), scanEdgesByType.getOperation());
}

void DBLowering::lowerScanOutEdgesByLabelSrc(mlir::db::ScanOutEdgesByLabelSrc scanOutEdgesByLabelSrc) {
    // The by-label sibling of lowerScanEdges: same placement at the top of the root
    // block, with the label list forwarded as-is to the nl op - translation resolves
    // the names against the schema, as it does for nl.scan_nodes_by_label.
    setInsertionInto(_rootBlock);

    nl::ScanOutEdgesByLabelSrc edges = _builder.create<nl::ScanOutEdgesByLabelSrc>(_builder.getUnknownLoc(),
                                                                                   scanOutEdgesByLabelSrc.getLabelsAttr());
    buildLoopForSource(edges.getResult(), scanOutEdgesByLabelSrc.getOperation());
}

void DBLowering::lowerScanInEdgesByLabelTgt(mlir::db::ScanInEdgesByLabelTgt scanInEdgesByLabelTgt) {
    // The reverse-direction sibling of lowerScanOutEdgesByLabelSrc, placed and forwarded
    // the same way.
    setInsertionInto(_rootBlock);

    nl::ScanInEdgesByLabelTgt edges = _builder.create<nl::ScanInEdgesByLabelTgt>(_builder.getUnknownLoc(),
                                                                                 scanInEdgesByLabelTgt.getLabelsAttr());
    buildLoopForSource(edges.getResult(), scanInEdgesByLabelTgt.getOperation());
}

void DBLowering::lowerScanOutEdgesByLabelTgt(mlir::db::ScanOutEdgesByLabelTgt scanOutEdgesByLabelTgt) {
    setInsertionInto(_rootBlock);

    nl::ScanOutEdgesByLabelTgt edges = _builder.create<nl::ScanOutEdgesByLabelTgt>(_builder.getUnknownLoc(),
                                                                                   scanOutEdgesByLabelTgt.getLabelsAttr());
    buildLoopForSource(edges.getResult(), scanOutEdgesByLabelTgt.getOperation());
}

void DBLowering::lowerScanInEdgesByLabelSrc(mlir::db::ScanInEdgesByLabelSrc scanInEdgesByLabelSrc) {
    setInsertionInto(_rootBlock);

    nl::ScanInEdgesByLabelSrc edges = _builder.create<nl::ScanInEdgesByLabelSrc>(_builder.getUnknownLoc(),
                                                                                 scanInEdgesByLabelSrc.getLabelsAttr());
    buildLoopForSource(edges.getResult(), scanInEdgesByLabelSrc.getOperation());
}

void DBLowering::lowerScanOutEdgesByLabelSrcTgt(mlir::db::ScanOutEdgesByLabelSrcTgt scanOutEdgesByLabelSrcTgt) {
    setInsertionInto(_rootBlock);

    nl::ScanOutEdgesByLabelSrcTgt edges = _builder.create<nl::ScanOutEdgesByLabelSrcTgt>(_builder.getUnknownLoc(),
                                                                                         scanOutEdgesByLabelSrcTgt.getSrcLabelsAttr(),
                                                                                         scanOutEdgesByLabelSrcTgt.getTgtLabelsAttr());
    buildLoopForSource(edges.getResult(), scanOutEdgesByLabelSrcTgt.getOperation());
}

void DBLowering::lowerScanInEdgesByLabelSrcTgt(mlir::db::ScanInEdgesByLabelSrcTgt scanInEdgesByLabelSrcTgt) {
    setInsertionInto(_rootBlock);

    nl::ScanInEdgesByLabelSrcTgt edges = _builder.create<nl::ScanInEdgesByLabelSrcTgt>(_builder.getUnknownLoc(),
                                                                                       scanInEdgesByLabelSrcTgt.getSrcLabelsAttr(),
                                                                                       scanInEdgesByLabelSrcTgt.getTgtLabelsAttr());
    buildLoopForSource(edges.getResult(), scanInEdgesByLabelSrcTgt.getOperation());
}

void DBLowering::lowerFetchNodes(mlir::db::FetchNodes fetch) {
    mlir::MLIRContext* const context = _builder.getContext();

    llvm::SmallVector<mlir::Value, 4> carriedChunks;
    llvm::SmallVector<mlir::Type, 4> resultTypes {nl::ChunkType::get(context, storage::NodeIDType::get(context))};
    for (const mlir::Value column : fetch.getColumnsToFilter()) {
        const mlir::Value columnChunk = mapValue(column);

        carriedChunks.push_back(columnChunk);
        resultTypes.push_back(columnChunk.getType());
    }

    const mlir::Value idsChunk = rowAlignedChunk(mapValue(fetch.getIds()), cardinalityDriver(carriedChunks));

    llvm::SmallVector<mlir::Value, 4> inputChunks {idsChunk};
    llvm::append_range(inputChunks, carriedChunks);

    setInsertionInto(deeperOfBlocks(_rootBlock, ownerBlock(deepestBoundChunk(inputChunks))));

    nl::FetchNodes nlFetch = _builder.create<nl::FetchNodes>(_builder.getUnknownLoc(),
                                                             resultTypes,
                                                             idsChunk,
                                                             carriedChunks);

    const mlir::ResultRange results = fetch->getResults();
    for (size_t resultIndex = 0; resultIndex < results.size(); resultIndex++) {
        _valueMap[results[resultIndex]] = nlFetch->getResult(resultIndex);
    }

    followCardinalityThrough(inputChunks, nlFetch->getResults());
}

void DBLowering::lowerFetchEdges(mlir::db::FetchEdges fetch) {
    mlir::MLIRContext* const context = _builder.getContext();
    const mlir::Type nodeChunk = nl::ChunkType::get(context, storage::NodeIDType::get(context));

    llvm::SmallVector<mlir::Value, 4> carriedChunks;
    llvm::SmallVector<mlir::Type, 8> resultTypes {nodeChunk,
                                                  nl::ChunkType::get(context, storage::EdgeIDType::get(context)),
                                                  nl::ChunkType::get(context, storage::EdgeTypeIDType::get(context)),
                                                  nodeChunk};
    for (const mlir::Value column : fetch.getColumnsToFilter()) {
        const mlir::Value columnChunk = mapValue(column);

        carriedChunks.push_back(columnChunk);
        resultTypes.push_back(columnChunk.getType());
    }

    const mlir::Value idsChunk = rowAlignedChunk(mapValue(fetch.getIds()), cardinalityDriver(carriedChunks));

    llvm::SmallVector<mlir::Value, 4> inputChunks {idsChunk};
    llvm::append_range(inputChunks, carriedChunks);

    setInsertionInto(deeperOfBlocks(_rootBlock, ownerBlock(deepestBoundChunk(inputChunks))));

    nl::FetchEdges nlFetch = _builder.create<nl::FetchEdges>(_builder.getUnknownLoc(),
                                                             resultTypes,
                                                             idsChunk,
                                                             carriedChunks,
                                                             fetch.getUndirectedAttr());

    const mlir::ResultRange results = fetch->getResults();
    for (size_t resultIndex = 0; resultIndex < results.size(); resultIndex++) {
        _valueMap[results[resultIndex]] = nlFetch->getResult(resultIndex);
    }

    followCardinalityThrough(inputChunks, nlFetch->getResults());
}

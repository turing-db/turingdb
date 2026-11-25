#pragma once

#include "NLTypes.h"

namespace mlir::nl {

Type getNodeIDChunkType(MLIRContext* context);

Type valueElementOf(Type chunkType);

Type getEdgeIDChunkType(MLIRContext* context);

// Whether a key read can be answered over this chunk: a chunk of maps, or one of tagged
// cells, which a read of a nested container hands back and whose row says what it holds
bool holdsAMapPerRow(Type chunkType);

// Out- and in-edge fetches expose the same row of chunks - source node IDs,
// edge IDs, edge type IDs and target node IDs - followed by one filtered chunk
// per carried column, so a loop body written for one direction works unchanged
// for the other.
Type getEdgeIteratorType(MLIRContext* context, TypeRange carriedChunkTypes);

LogicalResult verifyReadAsCells(Operation* op, TypeRange chunkTypes);

LogicalResult verifyCellWrites(Operation* op, Type writtenChunkType, TypeRange chunkTypes);

}

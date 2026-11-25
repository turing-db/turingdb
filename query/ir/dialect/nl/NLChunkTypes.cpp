#include "NLChunkTypes.h"

#include "llvm/ADT/STLExtras.h"
#include "llvm/ADT/SmallVector.h"

#include "NullLiteralType.h"

using namespace mlir;
using namespace mlir::nl;

namespace storage = mlir::storage;

namespace {

Type getEdgeTypeIDChunkType(MLIRContext* context) {
    return ChunkType::get(context, storage::EdgeTypeIDType::get(context));
}

bool isReadAsCellChunk(Type chunkType) {
    return isReadAsCells(cast<ChunkType>(chunkType).getElementType());
}

}

Type mlir::nl::getNodeIDChunkType(MLIRContext* context) {
    return ChunkType::get(context, storage::NodeIDType::get(context));
}

Type mlir::nl::valueElementOf(Type chunkType) {
    const Type element = llvm::cast<ChunkType>(chunkType).getElementType();
    const auto nullable = llvm::dyn_cast<storage::NullableType>(element);

    return nullable ? nullable.getValueType() : element;
}

Type mlir::nl::getEdgeIDChunkType(MLIRContext* context) {
    return ChunkType::get(context, storage::EdgeIDType::get(context));
}

bool mlir::nl::holdsAMapPerRow(Type chunkType) {
    const auto chunk = dyn_cast<ChunkType>(chunkType);
    if (!chunk) {
        return false;
    }

    const auto nullable = dyn_cast<storage::NullableType>(chunk.getElementType());
    const Type read = nullable ? nullable.getValueType() : chunk.getElementType();

    return isa<storage::MapType, storage::AnyType>(read);
}

Type mlir::nl::getEdgeIteratorType(MLIRContext* context, TypeRange carriedChunkTypes) {
    const Type sources = getNodeIDChunkType(context);
    const Type edgeIDs = getEdgeIDChunkType(context);
    const Type edgeTypeIDs = getEdgeTypeIDChunkType(context);
    const Type targets = getNodeIDChunkType(context);

    // The four fixed edge chunks, then the carry set comes back with the same
    // chunk types, mirroring db.get_out_edges' filtered_columns.
    llvm::SmallVector<Type> chunkTypes {sources, edgeIDs, edgeTypeIDs, targets};
    chunkTypes.append(carriedChunkTypes.begin(), carriedChunkTypes.end());

    return IteratorType::get(context, chunkTypes);
}

LogicalResult mlir::nl::verifyReadAsCells(Operation* op, TypeRange chunkTypes) {
    if (!llvm::all_of(chunkTypes, isReadAsCellChunk)) {
        return op->emitOpError("reads a cell per row out of a chunk of entities, paths, lists, maps, tagged cells "
                               "or nullable values, never out of a plain value chunk");
    }

    return success();
}

LogicalResult mlir::nl::verifyCellWrites(Operation* op, Type writtenChunkType, TypeRange chunkTypes) {
    if (!isa<storage::AnyType>(valueElementOf(writtenChunkType))) {
        return success();
    }

    return verifyReadAsCells(op, chunkTypes);
}

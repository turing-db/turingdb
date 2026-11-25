#include "NLOps.h"

#include <optional>

#include "llvm/ADT/SmallVector.h"

#include "NLChunkTypes.h"

using namespace mlir;
using namespace mlir::nl;

namespace storage = mlir::storage;

namespace {

Type getNullableChunkType(MLIRContext* context, Type valueType) {
    return ChunkType::get(context, storage::NullableType::get(context, valueType));
}

// A value an nl.vector_search reads when its loop starts: the element it searches with, null
// where an expression made it one, or a cell the loop checks when it starts
LogicalResult verifySearchedValue(Operation* op, llvm::StringRef name, Value bound, Type element) {
    const Type boundElement = bound ? valueElementOf(bound.getType()) : Type();
    if (boundElement && boundElement != element && !isa<storage::AnyType>(boundElement)) {
        return op->emitOpError("requires a bound ") << name << " of type " << element << " or !storage.any, got " << boundElement;
    }

    return success();
}

}

// A vector search produces the two fixed neighbour chunks per step - the nodes the index
// holds the vectors under and the distances they scored - and reads no input, so the
// iterator has exactly those two chunks and no carried tail. The neighbours ride the same
// node ID chunk a scan binds, the distances a nullable value chunk.
LogicalResult VectorSearch::inferReturnTypes(MLIRContext* context,
                                             std::optional<Location> location,
                                             VectorSearch::Adaptor adaptor,
                                             SmallVectorImpl<Type>& inferredReturnTypes) {
    const Type ids = getNodeIDChunkType(context);
    const Type scores = getNullableChunkType(context, Float64Type::get(context));

    inferredReturnTypes.push_back(IteratorType::get(context, {ids, scores}));
    return success();
}

LogicalResult LoadCSV::verify() {
    const Value boundPath = getBoundPath();
    if (getPath().has_value() == static_cast<bool>(boundPath)) {
        return emitOpError("requires either a path or a bound path");
    }

    const Type boundType = boundPath ? cast<ChunkType>(boundPath.getType()).getElementType() : Type();
    if (boundType && !isa<storage::StringType>(boundType)) {
        return emitOpError("requires a bound path of type !storage.string, got ") << boundType;
    }

    return success();
}

// The parameters db.vector_search rejects reach the index through this op unchanged: a
// search reporting no neighbour sizes its result buffers at zero, and a query vector of
// no dimension has nothing to score the neighbours against.
LogicalResult VectorSearch::verify() {
    const Value boundIndexName = getBoundIndexName();
    if (getIndexName().has_value() == static_cast<bool>(boundIndexName)) {
        return emitOpError("requires either an index name or a bound one");
    }

    const std::optional<uint64_t> neighbourCount = getK();
    const Value boundCount = getBoundK();
    if (neighbourCount.has_value() == static_cast<bool>(boundCount)) {
        return emitOpError("requires either a neighbour count or a bound one");
    } else if (neighbourCount == 0) {
        return emitOpError("must report at least one neighbour");
    }

    const std::optional<llvm::ArrayRef<float>> queryVector = getQueryVector();
    const Value boundQueryVector = getBoundQueryVector();
    if (queryVector.has_value() == static_cast<bool>(boundQueryVector)) {
        return emitOpError("requires either a query vector or a bound one");
    } else if (queryVector && queryVector->empty()) {
        return emitOpError("must search for a vector of at least one dimension");
    }

    Operation* const op = getOperation();
    MLIRContext* const context = getContext();

    const bool boundValuesVerify = succeeded(verifySearchedValue(op, "index name", boundIndexName, storage::StringType::get(context)))
                                && succeeded(verifySearchedValue(op, "neighbour count", boundCount, IntegerType::get(context, 64)))
                                && succeeded(verifySearchedValue(op, "query vector", boundQueryVector, storage::EmbeddingType::get(context)));

    return success(boundValuesVerify);
}

// The names in `yields` are checked against the procedure's declared return values
// during translation, which is where the registry is available; there is nothing to
// check here. An empty list is legal and means the call binds no return value at all -
// a procedure declaring none, driven for what it does rather than for rows.
LogicalResult Procedure::verify() {
    return success();
}

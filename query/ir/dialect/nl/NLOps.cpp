#include "NLOps.h"
#include "NLDialect.h"

#include "llvm/ADT/STLExtras.h"
#include "llvm/ADT/SmallVector.h"

#include "StorageEnums.h"

using namespace mlir;
using namespace mlir::nl;

namespace storage = mlir::storage;

#define GET_OP_CLASSES
#include "NLOps.cpp.inc"

namespace {

Type getNodeIDChunkType(MLIRContext* context) {
    return ChunkType::get(context, storage::NodeIDType::get(context));
}

Type getEdgeIDChunkType(MLIRContext* context) {
    return ChunkType::get(context, storage::EdgeIDType::get(context));
}

Type getEdgeTypeIDChunkType(MLIRContext* context) {
    return ChunkType::get(context, storage::EdgeTypeIDType::get(context));
}

// The value type an aggregate reduces: the T in a !nl.chunk<!storage.nullable<T>>.
// Aggregates fold property values, so an update's input and a result's output are
// always such chunks. Returns a null Type for anything else (an ID chunk, say), so
// the caller can reject it.
Type aggregateValueType(Type chunkType) {
    const auto chunk = dyn_cast<ChunkType>(chunkType);
    if (!chunk) {
        return Type();
    }

    const auto nullable = dyn_cast<storage::NullableType>(chunk.getElementType());
    if (!nullable) {
        return Type();
    }

    return nullable.getValueType();
}

// The nl.aggregate that produced this state handle, or a null op if the handle is
// not the result of one (a block argument, say). nl.aggregate_update /
// nl.aggregate_result use it to reconcile their kind with the accumulator's reset.
Aggregate producingAggregate(Value state) {
    return state.getDefiningOp<Aggregate>();
}

// Out- and in-edge fetches expose the same row of chunks - source node IDs,
// edge IDs, edge type IDs and target node IDs - followed by one filtered chunk
// per carried column, so a loop body written for one direction works unchanged
// for the other.
Type getEdgeIteratorType(MLIRContext* context, TypeRange carriedChunkTypes) {
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

}

// A node scan always produces one chunk of node IDs per step
LogicalResult ScanNodes::inferReturnTypes(MLIRContext* context,
                                          std::optional<Location> location,
                                          ScanNodes::Adaptor adaptor,
                                          SmallVectorImpl<Type>& inferredReturnTypes) {
    inferredReturnTypes.push_back(IteratorType::get(context, {getNodeIDChunkType(context)}));
    return success();
}

// A label scan filters the rows a plain scan would produce, never their shape,
// so it produces the same single chunk of node IDs per step - the label list is
// a filter, not a result type.
LogicalResult ScanNodesByLabel::inferReturnTypes(MLIRContext* context,
                                                 std::optional<Location> location,
                                                 ScanNodesByLabel::Adaptor adaptor,
                                                 SmallVectorImpl<Type>& inferredReturnTypes) {
    inferredReturnTypes.push_back(IteratorType::get(context, {getNodeIDChunkType(context)}));
    return success();
}

// An edge scan produces the same four fixed edge chunks per step as an
// out-edges fetch, but reads no input and carries nothing, so the iterator has
// exactly those four chunks and no carried tail.
LogicalResult ScanEdges::inferReturnTypes(MLIRContext* context,
                                         std::optional<Location> location,
                                         ScanEdges::Adaptor adaptor,
                                         SmallVectorImpl<Type>& inferredReturnTypes) {
    inferredReturnTypes.push_back(getEdgeIteratorType(context, {}));
    return success();
}

// An out-edges fetch produces one row of edge chunks per step, then one
// filtered chunk per carried column, mirroring db.get_out_edges
LogicalResult GetOutEdges::inferReturnTypes(MLIRContext* context,
                                            std::optional<Location> location,
                                            GetOutEdges::Adaptor adaptor,
                                            SmallVectorImpl<Type>& inferredReturnTypes) {
    inferredReturnTypes.push_back(getEdgeIteratorType(context, adaptor.getColumnsToFilter()));
    return success();
}

// An in-edges fetch produces the same row of edge chunks as out-edges, plus the
// same carried chunks; the input chunk names the nodes whose in-edges are gathered
LogicalResult GetInEdges::inferReturnTypes(MLIRContext* context,
                                           std::optional<Location> location,
                                           GetInEdges::Adaptor adaptor,
                                           SmallVectorImpl<Type>& inferredReturnTypes) {
    inferredReturnTypes.push_back(getEdgeIteratorType(context, adaptor.getColumnsToFilter()));
    return success();
}

// Filtering out-edges by type narrows only the rows, never their shape, so a
// by-type fetch produces the same iterator as GetOutEdges - the edge type name is
// a filter, not a result type.
LogicalResult GetOutEdgesByType::inferReturnTypes(MLIRContext* context,
                                                  std::optional<Location> location,
                                                  GetOutEdgesByType::Adaptor adaptor,
                                                  SmallVectorImpl<Type>& inferredReturnTypes) {
    inferredReturnTypes.push_back(getEdgeIteratorType(context, adaptor.getColumnsToFilter()));
    return success();
}

// The in-edge by-type sibling: same iterator shape as GetInEdges, narrowed by type.
LogicalResult GetInEdgesByType::inferReturnTypes(MLIRContext* context,
                                                 std::optional<Location> location,
                                                 GetInEdgesByType::Adaptor adaptor,
                                                 SmallVectorImpl<Type>& inferredReturnTypes) {
    inferredReturnTypes.push_back(getEdgeIteratorType(context, adaptor.getColumnsToFilter()));
    return success();
}

// A cross product yields one chunk per crossed column - the outer columns
// followed by the inner - each keeping its input chunk's element type, since
// the broadcast only changes the row count, not the element kind.
LogicalResult CrossProduct::inferReturnTypes(MLIRContext* context,
                                             std::optional<Location> location,
                                             CrossProduct::Adaptor adaptor,
                                             SmallVectorImpl<Type>& inferredReturnTypes) {
    for (const Type columnType : adaptor.getOuterColumns().getTypes()) {
        inferredReturnTypes.push_back(columnType);
    }

    for (const Type columnType : adaptor.getInnerColumns().getTypes()) {
        inferredReturnTypes.push_back(columnType);
    }

    return success();
}

LogicalResult Constant::inferReturnTypes(MLIRContext* context,
                                         std::optional<Location> location,
                                         Constant::Adaptor adaptor,
                                         SmallVectorImpl<Type>& inferredReturnTypes) {
    const TypedAttr value = cast<TypedAttr>(adaptor.getValue());
    inferredReturnTypes.push_back(ChunkType::get(context, value.getType()));
    return success();
}

// A truncate passes its columns through unchanged - only the row count is cut -
// so each result keeps its input column's chunk type.
LogicalResult LimitTruncate::inferReturnTypes(MLIRContext* context,
                                              std::optional<Location> location,
                                              LimitTruncate::Adaptor adaptor,
                                              SmallVectorImpl<Type>& inferredReturnTypes) {
    for (const Type columnType : adaptor.getColumns().getTypes()) {
        inferredReturnTypes.push_back(columnType);
    }

    return success();
}

// A skip truncate, like a limit truncate, passes its columns through unchanged -
// only the dropped prefix is removed - so each result keeps its input column's
// chunk type.
LogicalResult SkipTruncate::inferReturnTypes(MLIRContext* context,
                                             std::optional<Location> location,
                                             SkipTruncate::Adaptor adaptor,
                                             SmallVectorImpl<Type>& inferredReturnTypes) {
    for (const Type columnType : adaptor.getColumns().getTypes()) {
        inferredReturnTypes.push_back(columnType);
    }

    return success();
}

// An nl.sort_buffer needs at least one key, and one direction per key: the two
// arrays describe the same list of sort keys. The buffers' column count is not
// known here (the types live on the feeding nl.sort_collect), so key indices are
// range-checked during translation rather than at the buffer op.
LogicalResult SortBuffer::verify() {
    const ArrayRef<int64_t> keyColumns = getKeyColumns();
    const ArrayRef<bool> keyAscending = getKeyAscending();

    if (keyColumns.empty()) {
        return emitOpError("requires at least one sort key");
    }

    if (keyColumns.size() != keyAscending.size()) {
        return emitOpError("expects one ascending flag per key, but has ")
               << keyColumns.size() << " keys and " << keyAscending.size() << " flags";
    }

    return success();
}

// An nl.sort_collect must append at least one column - an empty append could not
// size the accumulated row set and there would be nothing to sort.
LogicalResult SortCollect::verify() {
    if (getColumns().empty()) {
        return emitOpError("requires at least one column to collect");
    }

    return success();
}

// A distinct filter passes its columns through unchanged - only duplicate rows
// are removed - so each result keeps its input column's chunk type, the same as
// nl.limit_truncate.
LogicalResult DistinctFilter::inferReturnTypes(MLIRContext* context,
                                              std::optional<Location> location,
                                              DistinctFilter::Adaptor adaptor,
                                              SmallVectorImpl<Type>& inferredReturnTypes) {
    for (const Type columnType : adaptor.getColumns().getTypes()) {
        inferredReturnTypes.push_back(columnType);
    }

    return success();
}

// An nl.distinct_filter must filter at least one column - the columns together
// are the dedup key, and an empty filter could not size the row set to dedup.
LogicalResult DistinctFilter::verify() {
    if (getColumns().empty()) {
        return emitOpError("requires at least one column to filter");
    }

    return success();
}

void For::build(OpBuilder& builder, OperationState& state, Value iterator) {
    For::build(builder, state, iterator, Value());
}

// Build a loop from the iterator value and an optional limit handle: the
// iterator type carries the chunk types, which become the loop variables (the
// body block arguments); a non-null limit handle is added as the second
// operand, so the loop early-exits once that budget is spent. The body is
// created with its implicit nl.yield terminator, ready for the caller to set
// the insertion point into it.
void For::build(OpBuilder& builder, OperationState& state, Value iterator, Value limit) {
    const OpBuilder::InsertionGuard guard(builder);

    state.addOperands(iterator);

    // The limit handle is optional; a null value leaves the loop unbounded.
    if (limit) {
        state.addOperands(limit);
    }

    const auto iteratorType = cast<IteratorType>(iterator.getType());
    const ArrayRef<Type> chunkTypes = iteratorType.getChunkTypes();
    const llvm::SmallVector<Location> chunkLocations(chunkTypes.size(), state.location);

    Region* bodyRegion = state.addRegion();
    builder.createBlock(bodyRegion, bodyRegion->end(), chunkTypes, chunkLocations);
    For::ensureTerminator(*bodyRegion, builder, state.location);
}

// Custom syntax, in the style of scf.for / LingoDB's dsa.for:
//
//   nl.for %chunk0, %chunk1, ... in %iterator : !nl.iter<...> { body }
//
// The loop variables are the entry block arguments of the body region. Their
// types are not spelled out in the text: each variable takes the corresponding
// chunk type of the iterator.
ParseResult For::parse(OpAsmParser& parser, OperationState& result) {
    llvm::SmallVector<OpAsmParser::Argument, 4> chunkArguments;
    OpAsmParser::UnresolvedOperand iterator;

    // Loop variables, `in` keyword, iterator operand
    const bool headerFailed = parser.parseArgumentList(chunkArguments)
                              || parser.parseKeyword("in")
                              || parser.parseOperand(iterator);
    if (headerFailed) {
        return failure();
    }

    // Optional `limit %handle` between the iterator and its type, naming the
    // loop's row-limit counter so it stops once the budget is spent
    OpAsmParser::UnresolvedOperand limit;
    const bool hasLimit = succeeded(parser.parseOptionalKeyword("limit"));
    if (hasLimit && parser.parseOperand(limit)) {
        return failure();
    }

    if (parser.parseColon()) {
        return failure();
    }

    const llvm::SMLoc typeLocation = parser.getCurrentLocation();
    Type iteratorType;
    if (parser.parseType(iteratorType)) {
        return failure();
    }

    const auto chunkIteratorType = dyn_cast<IteratorType>(iteratorType);
    if (!chunkIteratorType) {
        return parser.emitError(typeLocation, "expected an !nl.iter iterator type");
    }

    // Bind one loop variable per chunk produced by an iterator step
    const ArrayRef<Type> chunkTypes = chunkIteratorType.getChunkTypes();
    if (chunkArguments.size() != chunkTypes.size()) {
        return parser.emitError(typeLocation, "expected one loop variable per iterator chunk");
    }

    for (size_t chunkIndex = 0; chunkIndex < chunkArguments.size(); chunkIndex++) {
        chunkArguments[chunkIndex].type = chunkTypes[chunkIndex];
    }

    if (parser.resolveOperand(iterator, iteratorType, result.operands)) {
        return failure();
    }

    // Resolve the optional limit operand after the iterator, so it lands at
    // operand index 1; its type is the buildable !nl.limit_state handle.
    if (hasLimit) {
        const Type limitType = LimitStateType::get(parser.getContext());
        if (parser.resolveOperand(limit, limitType, result.operands)) {
            return failure();
        }
    }

    // The body region, with the loop variables as entry block arguments
    Region* bodyRegion = result.addRegion();
    if (parser.parseRegion(*bodyRegion, chunkArguments)) {
        return failure();
    } else if (parser.parseOptionalAttrDict(result.attributes)) {
        return failure();
    }

    // The nl.yield terminator may be left out in the text; insert it
    For::ensureTerminator(*bodyRegion, parser.getBuilder(), result.location);

    return success();
}

void For::print(OpAsmPrinter& printer) {
    // Loop variables: the entry block arguments of the body region
    printer << " ";
    llvm::interleaveComma(getBody()->getArguments(), printer, [&](BlockArgument chunkArgument) {
        printer << chunkArgument;
    });

    const Value iterator = getIterator();
    printer << " in " << iterator;

    // The optional limit handle prints between the iterator and its type,
    // mirroring the parse order: `%vars in %iter limit %h : !nl.iter<...>`.
    if (const Value limit = getLimit()) {
        printer << " limit " << limit;
    }

    printer << " : " << iterator.getType() << " ";

    // The loop variables are already printed in the loop header, and the
    // implicit nl.yield terminator carries nothing: elide both.
    printer.printRegion(getBodyRegion(), /*printEntryBlockArgs=*/false, /*printBlockTerminators=*/false);
    printer.printOptionalAttrDict((*this)->getAttrs());
}

LogicalResult For::verify() {
    const auto iteratorType = cast<IteratorType>(getIterator().getType());
    const ArrayRef<Type> chunkTypes = iteratorType.getChunkTypes();
    Block* body = getBody();

    if (body->getNumArguments() != chunkTypes.size()) {
        return emitOpError("expects the body to take one block argument per iterator chunk");
    }

    for (size_t chunkIndex = 0; chunkIndex < chunkTypes.size(); chunkIndex++) {
        const Type argumentType = body->getArgument(static_cast<unsigned>(chunkIndex)).getType();
        if (argumentType != chunkTypes[chunkIndex]) {
            return emitOpError("body argument ") << chunkIndex
                                                 << " must have the iterator chunk type "
                                                 << chunkTypes[chunkIndex];
        }
    }

    return success();
}

// A folded output reads the budgeted prefix (limit) or the surviving suffix
// (skip) off a single handle - the truncate adjacent to it - so at most one may
// be present. The interpreter reads them as an if(skip)/else if(limit) chain,
// which would silently ignore the limit if both were set; reject that here.
LogicalResult Output::verify() {
    if (getLimit() && getSkip()) {
        return emitOpError("carries both a limit and a skip handle; a folded output takes at most one");
    }

    return success();
}

// avg accumulates a running sum as f64, so its accumulator state must be an f64;
// sum/min/max accumulate in the value's own type, so any value element type is
// fine here (the update and result ops check the input/output against it).
LogicalResult Aggregate::verify() {
    if (getKind() == storage::AggregateKind::Avg) {
        const auto stateType = cast<AggregateStateType>(getState().getType());
        if (!isa<Float64Type>(stateType.getElementType())) {
            return emitOpError("avg must produce an f64 accumulator state");
        }
    }

    return success();
}

// The fold handler is selected from this op's kind and the input's value type, and
// it reads the accumulator as its own type; the accumulator was reset by the
// producing nl.aggregate's kind. Reconcile all three so a mismatch is rejected
// rather than folding into a wrongly-typed or wrongly-initialized accumulator.
LogicalResult AggregateUpdate::verify() {
    const storage::AggregateKind kind = getKind();

    // The input is a property value chunk; its value type is what the fold reads.
    const Type inputValueType = aggregateValueType(getRows().getType());
    if (!inputValueType) {
        return emitOpError("input must be a nullable value chunk (a property column)");
    }

    // avg folds any numeric input into an f64 accumulator; sum/min/max fold into an
    // accumulator of the input's own value type.
    const Type accumulatorType = cast<AggregateStateType>(getState().getType()).getElementType();
    if (kind == storage::AggregateKind::Avg) {
        if (!isa<Float64Type>(accumulatorType)) {
            return emitOpError("avg must fold into an f64 accumulator state");
        }
    } else if (inputValueType != accumulatorType) {
        return emitOpError("sum/min/max must fold into an accumulator of the input's value type");
    }

    // The accumulator's reset depends on the producer's kind (a present zero for
    // sum/avg, null for min/max), so a differing kind here folds into the wrong one.
    if (Aggregate producer = producingAggregate(getState())) {
        if (producer.getKind() != kind) {
            return emitOpError("kind must match the nl.aggregate that produced the state");
        }
    }

    return success();
}

// The result sibling of AggregateUpdate::verify: the emit handler copies the
// accumulator into the result (or divides by the count for avg), both read as the
// result's value type, so the result must be a nullable value chunk whose value
// type matches the reduction, and the kind must match the producer.
LogicalResult AggregateResult::verify() {
    const storage::AggregateKind kind = getKind();

    const Type resultValueType = aggregateValueType(getResult().getType());
    if (!resultValueType) {
        return emitOpError("result must be a nullable value chunk");
    }

    // avg emits an f64 result from an f64 accumulator; sum/min/max emit a result of
    // the accumulator's own value type.
    const Type accumulatorType = cast<AggregateStateType>(getState().getType()).getElementType();
    if (kind == storage::AggregateKind::Avg) {
        const bool bothF64 = isa<Float64Type>(accumulatorType) && isa<Float64Type>(resultValueType);
        if (!bothF64) {
            return emitOpError("avg must emit an f64 result from an f64 accumulator state");
        }
    } else if (resultValueType != accumulatorType) {
        return emitOpError("sum/min/max must emit a result of the accumulator's value type");
    }

    if (Aggregate producer = producingAggregate(getState())) {
        if (producer.getKind() != kind) {
            return emitOpError("kind must match the nl.aggregate that produced the state");
        }
    }

    return success();
}

// A grouped accumulator needs at least one grouping key and one aggregate (with
// no key it is a whole-stream aggregate, with no aggregate a projection), and
// every kind must be a valid GroupAggregateKind. The column count is not known
// here (the types live on the feeding nl.group_aggregate_update), so keyCount vs
// column count is reconciled during translation rather than at the buffer op.
LogicalResult GroupAggregateBuffer::verify() {
    if (getKeyCount() == 0) {
        return emitOpError("requires at least one grouping key");
    }

    const ArrayRef<int64_t> kinds = getKinds();
    if (kinds.empty()) {
        return emitOpError("requires at least one aggregate");
    }

    for (const int64_t kind : kinds) {
        if (!storage::symbolizeGroupAggregateKind(kind)) {
            return emitOpError("has an unknown aggregate kind ") << kind;
        }
    }

    return success();
}

// A grouped update must collect at least one column - the grouping keys and
// aggregate inputs together - since an empty collect could neither size the row
// set nor build a group key.
LogicalResult GroupAggregateUpdate::verify() {
    if (getColumns().empty()) {
        return emitOpError("requires at least one column to collect");
    }

    return success();
}


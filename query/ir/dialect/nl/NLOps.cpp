#include "NLOps.h"

#include "NLDialect.h"

#include <optional>

#include "llvm/ADT/STLExtras.h"
#include "llvm/ADT/SmallVector.h"

#include "IRValueTypes.h"
#include "StorageEnums.h"
#include "ColumnIndicesFormat.h"
#include "EdgeDirectionsFormat.h"
#include "PathStepsFormat.h"
#include "LabelAlternativesFormat.h"
#include "StorageAttributes.h"
#include "MergePatternShape.h"
#include "NullLiteralType.h"
#include "GroupAggregateKindsFormat.h"
#include "PropertyScanLiteral.h"
#include "BoundEntityIDs.h"
#include "BoundValueFormat.h"
#include "NLChunkTypes.h"

using namespace mlir;
using namespace mlir::nl;

namespace storage = mlir::storage;

#define GET_OP_CLASSES
#include "NLOps.cpp.inc"

namespace {

bool writesAMapPerRow(Type chunkType) {
    const auto chunk = dyn_cast<ChunkType>(chunkType);

    return holdsAMapPerRow(chunkType) || (chunk && isNullLiteralType(chunk.getElementType()));
}

// The value type an aggregate reduces: the T in a !nl.chunk<!storage.nullable<T>>, or
// !storage.any for a chunk of tagged cells, which holds its own null. Aggregates fold
// property values, so an update's input and a result's output are always such chunks.
// Returns a null Type for anything else (an ID chunk, say), so the caller can reject it.
Type aggregateValueType(Type chunkType) {
    const auto chunk = dyn_cast<ChunkType>(chunkType);
    if (!chunk) {
        return Type();
    }

    const Type element = chunk.getElementType();
    if (isa<storage::AnyType>(element)) {
        return element;
    }

    const auto nullable = dyn_cast<storage::NullableType>(element);
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

// A count bound when the query runs is an integer, null where an expression made it one, or a
// cell the loop checks holds one when it starts
LogicalResult verifyBoundCount(Operation* op, Value boundCount) {
    const Type element = boundCount ? valueElementOf(boundCount.getType()) : Type();
    if (element && !element.isSignlessInteger(64) && !isa<storage::AnyType>(element)) {
        return op->emitOpError("requires a bound count of type i64 or !storage.any, got ") << element;
    }

    return success();
}

}

LogicalResult EntityIDOf::verify() {
    const Type input = valueElementOf(getOperand().getType());
    if (!storage::isEntityIDOfInputType(input)) {
        return emitOpError("requires a value, a node or an edge, not a path, got ") << input;
    }

    const Type result = cast<ChunkType>(getResult().getType()).getElementType();
    if (!storage::isEntityIDType(result)) {
        return emitOpError("requires a node or an edge result, got ") << result;
    }

    return success();
}

LogicalResult CreateVectorIndex::verify() {
    if (getDimension() == 0) {
        return emitOpError("must hold vectors of at least one dimension");
    }

    return success();
}

void Yield::build(OpBuilder& builder, OperationState& state) {
}

// A cross product yields one chunk per crossed column - the outer columns
// followed by the inner - each keeping its input chunk's element type, since
// the broadcast only changes the row count, not the element kind.
LogicalResult CrossProduct::inferReturnTypes(MLIRContext* context,
                                             std::optional<Location> location,
                                             CrossProduct::Adaptor adaptor,
                                             SmallVectorImpl<Type>& inferredReturnTypes) {
    SmallVector<Type, 8> chunkTypes;

    for (const Type columnType : adaptor.getOuterColumns().getTypes()) {
        chunkTypes.push_back(columnType);
    }

    for (const Type columnType : adaptor.getInnerColumns().getTypes()) {
        chunkTypes.push_back(columnType);
    }

    inferredReturnTypes.push_back(IteratorType::get(context, chunkTypes));

    return success();
}

LogicalResult EachRow::inferReturnTypes(MLIRContext* context,
                                        std::optional<Location> location,
                                        EachRow::Adaptor adaptor,
                                        SmallVectorImpl<Type>& inferredReturnTypes) {
    SmallVector<Type, 8> chunkTypes;

    for (const Type columnType : adaptor.getColumns().getTypes()) {
        chunkTypes.push_back(columnType);
    }

    inferredReturnTypes.push_back(IteratorType::get(context, chunkTypes));

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

LogicalResult ExistsMark::verify() {
    if (getTag() && getRow()) {
        return emitOpError("marks either the rows a tag names or the one row `row` names, not both");
    }

    return success();
}

LogicalResult CountSubqueryTally::verify() {
    const bool hasTag = getTag() != nullptr;
    const bool hasRows = getRows() != nullptr;

    if (hasTag == hasRows) {
        return emitOpError("counts either the rows of a tag or a row count, exactly one of the two");
    } else if (getRow() && !hasRows) {
        return emitOpError("names the row a row count answers for, which a tagged tally has none of");
    }

    return success();
}

LogicalResult RowListsCollect::verify() {
    if (getTag() && getRow()) {
        return emitOpError("stages under either the rows a tag names or the one row `row` names, not both");
    }

    return verifyReadAsCells(getOperation(), getValue().getType());
}

LogicalResult Limit::verify() {
    const Value boundCount = getBoundCount();
    if (getCount().has_value() == static_cast<bool>(boundCount)) {
        return emitOpError("requires either a count or a bound one");
    }

    return verifyBoundCount(getOperation(), boundCount);
}

LogicalResult Skip::verify() {
    const Value boundCount = getBoundCount();
    if (getCount().has_value() == static_cast<bool>(boundCount)) {
        return emitOpError("requires either a count or a bound one");
    }

    return verifyBoundCount(getOperation(), boundCount);
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

    const Value boundTopK = getBoundTopK();
    if (getTopK() && boundTopK) {
        return emitOpError("requires at most one of a top-K count and a bound one");
    }

    return verifyBoundCount(getOperation(), boundTopK);
}

// An nl.sort_collect must append at least one column - an empty append could not
// size the accumulated row set and there would be nothing to sort.
LogicalResult SortCollect::verify() {
    if (getColumns().empty()) {
        return emitOpError("requires at least one column to collect");
    }

    return success();
}

// An nl.row_collect must append at least one column: the buffers' row count is read
// from the first of them.
LogicalResult RowCollect::verify() {
    if (getColumns().empty()) {
        return emitOpError("requires at least one column to collect");
    }

    return success();
}

// An nl.hash_join_collect must append at least one column: the build side's row
// count is read from the first buffer, and one of the columns is the key the
// buffer op names.
LogicalResult HashJoinCollect::verify() {
    if (getColumns().empty()) {
        return emitOpError("requires at least one column to collect");
    }

    return success();
}

// An nl.hash_join_probe reads at least one probe column - one of them is the key -
// and emits the probe columns unchanged followed by the build ones, so it has at
// least as many results as columns and the first of them keep the probe types.
LogicalResult HashJoinProbe::verify() {
    const OperandRange columns = getColumns();
    if (columns.empty()) {
        return emitOpError("requires at least one column to probe with");
    }

    const auto iteratorType = cast<IteratorType>(getResult().getType());
    const ArrayRef<Type> chunkTypes = iteratorType.getChunkTypes();
    if (chunkTypes.size() < columns.size()) {
        return emitOpError("expects a chunk per probe column and per build column, but has ")
               << chunkTypes.size() << " chunks for " << columns.size() << " probe columns";
    }

    for (size_t columnIndex = 0; columnIndex < columns.size(); columnIndex++) {
        if (columns[columnIndex].getType() != chunkTypes[columnIndex]) {
            return emitOpError("chunk ") << columnIndex
                                         << " must have the same type as probe column "
                                         << columnIndex;
        }
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

// A count read off the graph's node counts is the same single unsigned tally
// nl.count_result materializes, whatever it counted, so the chunk type never varies
LogicalResult CountScanRows::inferReturnTypes(MLIRContext* context,
                                              std::optional<Location> location,
                                              CountScanRows::Adaptor adaptor,
                                              SmallVectorImpl<Type>& inferredReturnTypes) {
    inferredReturnTypes.push_back(ChunkType::get(context, IntegerType::get(context, 64, IntegerType::Unsigned)));
    return success();
}

// A count with no scan listed has nothing to count; the empty product it would stand for
// is an nl.constant instead. A property is read from one listed scan, as in db.count_scan_rows.
LogicalResult CountScanRows::verify() {
    if (getLabels().empty()) {
        return emitOpError("requires at least one scan to count");
    }

    const std::optional<llvm::StringRef> property = getProperty();
    if (property && property->empty()) {
        return emitOpError("requires a non-empty property name");
    }

    if (!property && getPropertyScan()) {
        return emitOpError("names the scan of a property it does not read");
    }

    if (getPropertyScan().value_or(0) >= getLabels().size()) {
        return emitOpError("reads a property from a scan it does not list");
    }

    return success();
}

LogicalResult Split::verify() {
    const bool readsAString = llvm::isa<storage::StringType, storage::AnyType>(valueElementOf(getString().getType()));
    const bool readsDelimiters = llvm::isa<storage::StringType, storage::ListType, storage::AnyType>(
                                     valueElementOf(getDelimiter().getType()));
    if (!readsAString || !readsDelimiters) {
        return emitOpError("operands must be a string chunk and a string or list chunk, or chunks of tagged cells");
    }

    const storage::ListType listType = llvm::dyn_cast<storage::ListType>(valueElementOf(getResult().getType()));
    if (!listType || !llvm::isa<storage::StringType>(listType.getElementType())) {
        return emitOpError("result must be a chunk of string lists");
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

void Output::build(OpBuilder& builder,
                   OperationState& state,
                   ValueRange columns,
                   Value limit,
                   Value skip,
                   Value cardinality) {
    Output::build(builder, state, columns, limit, skip, cardinality, ArrayAttr());
}

// A folded output reads the budgeted prefix (limit) or the surviving suffix
// (skip) off a single handle - the truncate adjacent to it - so at most one may
// be present. The interpreter reads them as an if(skip)/else if(limit) chain,
// which would silently ignore the limit if both were set; reject that here.
LogicalResult Output::verify() {
    if (getLimit() && getSkip()) {
        return emitOpError("carries both a limit and a skip handle; a folded output takes at most one");
    }

    const ArrayAttr columnNames = getColumnNamesAttr();
    if (columnNames && columnNames.size() != getColumns().size()) {
        return emitOpError("names must give one name per output column, but has ")
               << columnNames.size() << " names for " << getColumns().size() << " columns";
    }

    return success();
}

// stdev and avg of numbers accumulate as f64, so their accumulator state must be an f64;
// sum/min/max and avg of durations or of tagged cells accumulate in the value's own type,
// so any value element type is fine there (the update and result ops check the
// input/output against it).
LogicalResult Aggregate::verify() {
    const Type stateElement = cast<AggregateStateType>(getState().getType()).getElementType();
    const bool foldsIntoDouble = storage::reducesToADouble(getKind(), stateElement);
    const bool holdsADouble = isa<Float64Type>(stateElement);

    if (foldsIntoDouble && !holdsADouble) {
        return emitOpError("avg of numbers and stdev must produce an f64 accumulator state");
    }

    return success();
}

// The fold handler is selected from this op's kind and the input's value type, and
// it reads the accumulator as its own type; the accumulator was reset by the
// producing nl.aggregate's kind. Reconcile all three so a mismatch is rejected
// rather than folding into a wrongly-typed or wrongly-initialized accumulator.
LogicalResult AggregateUpdate::verify() {
    const storage::AggregateKind kind = getKind();
    const Type accumulatorType = cast<AggregateStateType>(getState().getType()).getElementType();

    // A type-erased column of tagged cells has no one value type: every cell is read
    // through its own tag. stdev folds one into an f64, sum and avg into a tagged cell
    // holding an integer, a float or a duration, and min/max into a tagged cell holding
    // the winning one.
    const auto rowsChunk = cast<ChunkType>(getRows().getType());
    const Type rowsElement = rowsChunk.getElementType();
    const auto rowsNullable = dyn_cast<storage::NullableType>(rowsElement);
    const Type reducedElement = rowsNullable ? rowsNullable.getValueType() : rowsElement;
    const bool foldsIntoDouble = storage::reducesToADouble(kind, reducedElement);
    const bool taggedCells = isa<storage::AnyType>(rowsElement);
    if (taggedCells && !foldsIntoDouble && !isa<storage::AnyType>(accumulatorType)) {
        return emitOpError("a sum, avg, min or max over type-erased cells must fold into a tagged cell accumulator state");
    }

    // Every other input is a property value chunk; its value type is what the fold
    // reads, and sum/min/max and avg of durations fold into an accumulator of that type.
    if (!taggedCells) {
        const Type declaredValueType = aggregateValueType(getRows().getType());
        if (!declaredValueType) {
            return emitOpError("input must be a nullable value chunk (a property column)");
        }

        // The null literal names no value type of its own and is carried as a null
        // integer, which is the column the fold reads and folds nothing out of
        const bool untypedNull = isa<NoneType>(declaredValueType);
        const Type inputValueType = untypedNull ? IntegerType::get(getContext(), 64) : declaredValueType;

        if (!foldsIntoDouble && inputValueType != accumulatorType) {
            return emitOpError("sum/min/max and avg of durations or points must fold into an accumulator of the input's value type");
        }
    }

    // stdev folds any numeric input into an f64 accumulator, tagged cells included, and
    // avg folds numbers into one.
    if (foldsIntoDouble && !isa<Float64Type>(accumulatorType)) {
        return emitOpError("avg of numbers and stdev must fold into an f64 accumulator state");
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
        return emitOpError("result must be a nullable value chunk or a chunk of tagged cells");
    }

    // stdev and avg of numbers emit an f64 result from an f64 accumulator; sum/min/max
    // and avg of durations or of tagged cells emit a result of the accumulator's own
    // value type.
    const Type accumulatorType = cast<AggregateStateType>(getState().getType()).getElementType();
    if (storage::reducesToADouble(kind, accumulatorType)) {
        const bool bothF64 = isa<Float64Type>(accumulatorType) && isa<Float64Type>(resultValueType);
        if (!bothF64) {
            return emitOpError("avg of numbers and stdev must emit an f64 result from an f64 accumulator state");
        }
    } else if (resultValueType != accumulatorType) {
        return emitOpError("sum/min/max and avg of durations must emit a result of the accumulator's value type");
    }

    if (Aggregate producer = producingAggregate(getState())) {
        if (producer.getKind() != kind) {
            return emitOpError("kind must match the nl.aggregate that produced the state");
        }
    }

    return success();
}

LogicalResult CreateNode::verify() {
    if (getLabels().empty()) {
        return emitOpError("requires at least one label");
    }

    if (getPropNames().size() != getPropValues().size()) {
        return emitOpError("prop_names and prop_values must have the same count, but has ")
               << getPropNames().size() << " names and " << getPropValues().size() << " values";
    }

    return success();
}

LogicalResult CreateEdge::verify() {
    const StringAttr edgeType = dyn_cast<StringAttr>(getEdgeType());
    if (edgeType && edgeType.getValue().empty()) {
        return emitOpError("requires a non-empty edge type");
    }

    if (getPropNames().size() != getPropValues().size()) {
        return emitOpError("prop_names and prop_values must have the same count, but has ")
               << getPropNames().size() << " names and " << getPropValues().size() << " values";
    }

    return success();
}

LogicalResult Merge::verify() {
    const LogicalResult pattern = verifyMergePattern(getOperation(),
                                                     getNodeLabels(),
                                                     getNodePropNames(),
                                                     getEdgeTypes(),
                                                     getEdgePropNames(),
                                                     getEdgeDirectionsAttr(),
                                                     getPendingNodesAttr(),
                                                     getBoundNodes().size(),
                                                     getBoundPending().size(),
                                                     getNodePropValues().size(),
                                                     getEdgePropValues().size());
    if (failed(pattern)) {
        return pattern;
    }

    const size_t expectedResults = mergeResultCount(getNodeLabels(), getCarriedColumns().size());
    if (getResults().size() != expectedResults) {
        return emitOpError("must produce one chunk per chain entity, the created mask and one "
                           "per carried chunk - ")
               << expectedResults << " in all, but produces " << getResults().size();
    }

    return success();
}

LogicalResult SetNodeProperty::verify() {
    if (getProperty().empty()) {
        return emitOpError("requires a non-empty property name");
    }

    return success();
}

LogicalResult SetEdgeProperty::verify() {
    if (getProperty().empty()) {
        return emitOpError("requires a non-empty property name");
    }

    return success();
}

LogicalResult SetNodeProperties::verify() {
    if (!writesAMapPerRow(getValue().getType())) {
        return emitOpError("writes a chunk of maps or of tagged cells, optionally nullable, or the null literal");
    } else if (!::db::namesPropertyElementTypes(getCreatedTypesAttr())) {
        return emitOpError("creates each property with the element type of a stored value");
    }

    return success();
}

LogicalResult SetEdgeProperties::verify() {
    if (!writesAMapPerRow(getValue().getType())) {
        return emitOpError("writes a chunk of maps or of tagged cells, optionally nullable, or the null literal");
    } else if (!::db::namesPropertyElementTypes(getCreatedTypesAttr())) {
        return emitOpError("creates each property with the element type of a stored value");
    }

    return success();
}

LogicalResult ImportGraph::verify() {
    if (!::db::namesImportedPropertyTypes(getFormat(), getTypesAttr())) {
        return emitOpError("types each property as a datetime, duration or point a jsonl import reads, or a duration a parquet import reads");
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

// The columns split into the buffer's grouping keys, the collected values and one input
// per reduction the buffer names, which is the split db.collect's verifier makes over its
// own operands: the buffer holds the counts and the update holds the columns, so this is
// where the nl side re-establishes it.
LogicalResult CollectUpdate::verify() {
    const OperandRange columns = getColumns();
    if (columns.empty()) {
        return emitOpError("requires at least one column to collect");
    }

    CollectBuffer buffer = getState().getDefiningOp<CollectBuffer>();
    if (!buffer) {
        return emitOpError("state must come from an nl.collect_buffer");
    }

    // Bound keyCount by the real column count first: summing keyCount + aggregateCount
    // could wrap in unsigned 64-bit and let a pathological keyCount slip past into
    // out-of-bounds translation.
    const size_t columnCount = columns.size();
    const uint64_t keyCount = buffer.getKeyCount();
    const size_t aggregateCount = buffer.getKinds().value_or(llvm::ArrayRef<int64_t> {}).size();
    if (keyCount >= columnCount || columnCount - keyCount <= aggregateCount) {
        return emitOpError("expects ") << keyCount
                                       << " grouping-key columns, at least one collected column and "
                                       << aggregateCount
                                       << " aggregate columns, but has "
                                       << columnCount;
    }

    const size_t valueCount = columnCount - keyCount - aggregateCount;

    // Every deduplicating value names one of the collected columns.
    if (const std::optional<llvm::ArrayRef<int64_t>> distinctValues = buffer.getDistinctValues()) {
        for (const int64_t valueIndex : *distinctValues) {
            if (valueIndex < 0 || static_cast<size_t>(valueIndex) >= valueCount) {
                return emitOpError("distinct value index ") << valueIndex
                                                            << " is out of range for "
                                                            << valueCount
                                                            << " collected columns";
            }
        }
    }

    return success();
}

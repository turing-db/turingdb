#include "NLOps.h"

#include <optional>

#include "llvm/ADT/STLExtras.h"
#include "llvm/ADT/SmallVector.h"

#include "StorageEnums.h"

using namespace mlir;
using namespace mlir::nl;

namespace storage = mlir::storage;

namespace {

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

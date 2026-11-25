#include "DBLowering.h"

#include <mlir/IR/Location.h>
#include <optional>

#include "mlir/IR/Block.h"
#include "mlir/IR/Builders.h"
#include "mlir/IR/Types.h"

#include "NLOps.h"

#include "DBColumnElement.h"
#include "NullLiteralType.h"

#include "IRValueTypes.h"

#include "IRException.h"
#include "llvm/Support/Casting.h"

using namespace db;

namespace nl = mlir::nl;
namespace storage = mlir::storage;

namespace {

// The accumulator (and result) element type of an aggregate over inputElement, the value
// type its nullable chunk wraps or the tagged cell itself: stdev is an f64, avg is one over
// numbers, and the rest keep the input's type. Throws for a value type the reduction cannot
// take, so a string sum or an embedding min is rejected as Cypher rejects it.
mlir::Type aggregateResultElementType(mlir::OpBuilder& builder,
                                      storage::AggregateKind kind,
                                      mlir::Type inputElement) {
    const bool isFloat = mlir::isa<mlir::Float64Type>(inputElement);
    const auto integerType = mlir::dyn_cast<mlir::IntegerType>(inputElement);
    const bool isBool = integerType && integerType.getWidth() == 1;
    const bool isInteger = integerType && !isBool;
    const bool isNumeric = isFloat || isInteger;
    const bool isString = mlir::isa<storage::StringType>(inputElement);
    const bool isDateTime = mlir::isa<storage::DateTimeType>(inputElement);
    const bool isDuration = mlir::isa<storage::DurationType>(inputElement);
    const bool isPoint = mlir::isa<storage::PointType>(inputElement);
    const bool isTaggedCell = mlir::isa<storage::AnyType>(inputElement);

    // An untyped null holds no value to reduce - a name no property in the graph carries,
    // or the null literal - so every reduction over it sees nothing: min, max and avg
    // answer null, sum answers 0 and stdev 0.0. It names no value type either, so the
    // answer rides the integer column an untyped null is laid out over anywhere else,
    // except avg's and stdev's, which are floats whatever they reduced.
    if (mlir::isa<mlir::NoneType>(inputElement)) {
        if (storage::reducesToADouble(kind, inputElement)) {
            return builder.getF64Type();
        }

        return builder.getIntegerType(64);
    }

    switch (kind) {
        case storage::AggregateKind::Sum: {
            if (!isNumeric && !isDuration && !isTaggedCell) {
                throw IRException("db.sum requires a numeric or a duration column");
            }
            return inputElement;
        }
        break;

        case storage::AggregateKind::Avg: {
            if (!isNumeric && !isDuration && !isPoint && !isTaggedCell) {
                throw IRException("db.avg requires a numeric, a duration or a point column");
            }
            return storage::reducesToADouble(kind, inputElement) ? builder.getF64Type() : inputElement;
        }
        break;

        case storage::AggregateKind::StDev:
        case storage::AggregateKind::StDevP: {
            if (!isNumeric && !isTaggedCell) {
                throw IRException("db.stdev/db.stdevp requires a numeric column");
            }
            return builder.getF64Type();
        }
        break;

        case storage::AggregateKind::Min:
        case storage::AggregateKind::Max: {
            // min/max order the values, so anything Cypher orders is fine - numbers,
            // strings, bools, instants, points, lists, maps, and cells of different types -
            // but an embedding has no order.
            const bool isList = mlir::isa<storage::ListType>(inputElement);
            const bool isMap = mlir::isa<storage::MapType>(inputElement);
            const bool isOrderable = isNumeric || isString || isBool || isDateTime || isDuration
                                  || isPoint || isList || isMap || isTaggedCell;
            if (!isOrderable) {
                throw IRException("db.min/db.max requires an orderable column");
            }
            return inputElement;
        }
        break;
    }

    throw IRException("Unhandled aggregate kind");
}

// The value-reduction AggregateKind matching a grouped aggregate's kind, so the
// grouped result-type resolution reuses aggregateResultElementType. count and
// count_distinct tally rows rather than reducing values, so they have no
// value-reduction kind and never reach here (the caller resolves their result type -
// a ui64 - directly).
storage::AggregateKind groupKindToAggregateKind(storage::GroupAggregateKind kind) {
    switch (kind) {
        case storage::GroupAggregateKind::Sum:
        case storage::GroupAggregateKind::SumDistinct:
            return storage::AggregateKind::Sum;
        break;

        case storage::GroupAggregateKind::Min:
            return storage::AggregateKind::Min;
        break;

        case storage::GroupAggregateKind::Max:
            return storage::AggregateKind::Max;
        break;

        case storage::GroupAggregateKind::Avg:
        case storage::GroupAggregateKind::AvgDistinct:
            return storage::AggregateKind::Avg;
        break;

        case storage::GroupAggregateKind::StDev:
        case storage::GroupAggregateKind::StDevDistinct:
            return storage::AggregateKind::StDev;
        break;

        case storage::GroupAggregateKind::StDevP:
        case storage::GroupAggregateKind::StDevPDistinct:
            return storage::AggregateKind::StDevP;
        break;

        case storage::GroupAggregateKind::Count:
        case storage::GroupAggregateKind::CountDistinct:
        case storage::GroupAggregateKind::CountRows:
            throw IRException("count has no value-reduction kind");
        break;

        case storage::GroupAggregateKind::First:
            throw IRException("first has no value-reduction kind");
        break;
    }

    throw IRException("Unhandled group aggregate kind");
}

// Whether a grouped aggregate reduces the values of its input column rather than tallying
// its rows: the reductions read a nullable value chunk, while the counts read the chunk
// they are anchored on as it comes - an ID chunk for count(*).
bool reducesValues(storage::GroupAggregateKind kind) {
    switch (kind) {
        case storage::GroupAggregateKind::Sum:
        case storage::GroupAggregateKind::SumDistinct:
        case storage::GroupAggregateKind::Min:
        case storage::GroupAggregateKind::Max:
        case storage::GroupAggregateKind::Avg:
        case storage::GroupAggregateKind::AvgDistinct:
        case storage::GroupAggregateKind::StDev:
        case storage::GroupAggregateKind::StDevP:
        case storage::GroupAggregateKind::StDevDistinct:
        case storage::GroupAggregateKind::StDevPDistinct:
        case storage::GroupAggregateKind::First:
            return true;
        break;

        case storage::GroupAggregateKind::Count:
        case storage::GroupAggregateKind::CountDistinct:
        case storage::GroupAggregateKind::CountRows:
            return false;
        break;
    }

    throw IRException("Unhandled group aggregate kind");
}

// The nl chunk type of one grouped aggregate's result column, resolved from its
// kind and input chunk. A switch (not an if/else) over every GroupAggregateKind so
// a new kind is a compile error here rather than silently taking the value-reduction
// path: count and count_distinct are a single non-null unsigned i64 per group;
// sum/min/max/avg reduce the input's values, so they require a nullable value column
// (an ID column is rejected) and follow aggregateResultElementType - sum/min/max keep
// the value type, avg widens numbers to f64.
nl::ChunkType groupAggregateResultChunkType(mlir::OpBuilder& builder,
                                            storage::GroupAggregateKind kind,
                                            mlir::Value inputChunk,
                                            nl::ChunkType countChunkType) {
    mlir::MLIRContext* const context = builder.getContext();

    switch (kind) {
        case storage::GroupAggregateKind::Count:
        case storage::GroupAggregateKind::CountDistinct:
        case storage::GroupAggregateKind::CountRows:
            return countChunkType;
        break;

        case storage::GroupAggregateKind::Sum:
        case storage::GroupAggregateKind::SumDistinct:
        case storage::GroupAggregateKind::Min:
        case storage::GroupAggregateKind::Max:
        case storage::GroupAggregateKind::Avg:
        case storage::GroupAggregateKind::AvgDistinct:
        case storage::GroupAggregateKind::StDev:
        case storage::GroupAggregateKind::StDevP:
        case storage::GroupAggregateKind::StDevDistinct:
        case storage::GroupAggregateKind::StDevPDistinct:
        case storage::GroupAggregateKind::First: {
            const nl::ChunkType inputChunkType = mlir::cast<nl::ChunkType>(inputChunk.getType());
            const mlir::Type inputElement = inputChunkType.getElementType();
            const auto inputNullable = mlir::dyn_cast<storage::NullableType>(inputElement);
            const bool taggedCells = mlir::isa<storage::AnyType>(inputElement);
            if (!inputNullable && !taggedCells) {
                throw IRException("db.group_aggregate sum/min/max/avg/stdev/first requires a property value column");
            }

            const mlir::Type inputValue = taggedCells ? inputElement : inputNullable.getValueType();
            const bool keepsTheValue = kind == storage::GroupAggregateKind::First;
            const mlir::Type resultElement = keepsTheValue ? inputValue
                                                           : aggregateResultElementType(builder, groupKindToAggregateKind(kind), inputValue);
            return nl::ChunkType::get(context, nullableElementType(resultElement));
        }
        break;
    }

    throw IRException("Unhandled group aggregate kind");
}

}

void DBLowering::lowerCount(mlir::db::Count count) {
    // The nl chunk the counted column lowered to; the update reads its per-step
    // non-null row count. A constant column counts the rows it stands for, not the
    // one row it is, so it is laid out over the driving relation first.
    const mlir::Value inputChunk = rowAlignedChunk(mapValue(count.getInput()), rowsInFlight());

    const mlir::Location loc = _builder.getUnknownLoc();

    // The tally is hoisted to the top of the root block, above every loop of this
    // dataflow, so it is reset once per step of that block and dominates the update
    // placed in the producing loop body and the emit that reads it after the loop.
    _builder.setInsertionPointToStart(_rootBlock);
    const mlir::Value state = _builder.create<nl::Count>(loc).getState();

    // count(DISTINCT x) feeds the tally the survivors of a DISTINCT instead of the raw
    // column. A null does survive the filter - all nulls share one key - but is still
    // never charged, since nl.count_update counts only non-null rows, so the tally
    // comes out as the distinct non-null value count Cypher asks for.
    mlir::Value distinctState;
    if (count.getDistinct()) {
        distinctState = _builder.create<nl::Distinct>(loc).getState();
    }

    // The update sits in the innermost producing loop body, where the counted
    // column is bound (the same block db.output would emit from), and charges each
    // step's non-null rows against the tally.
    mlir::Block* const producingBlock = accumulatorUpdateBlock(ownerBlock(inputChunk));
    setInsertionInto(producingBlock);

    mlir::Value countedChunk = inputChunk;
    if (distinctState) {
        nl::DistinctFilter filter = _builder.create<nl::DistinctFilter>(loc, distinctState, mlir::ValueRange {inputChunk});
        countedChunk = filter.getResults().front();
    }

    _builder.create<nl::CountUpdate>(loc, state, countedChunk, count.getRows());

    // COUNT is a pipeline breaker: the tally is final only once every row has been
    // seen. Since it collapses to exactly one row there is nothing to iterate, so -
    // unlike db.sort - it opens no emit loop: nl.count_result materializes the tally
    // chunk in place at function scope, right after the producing loop, and db.output
    // consumes it there. The chunk is the single-row count as an unsigned i64
    // (!nl.chunk<ui64>) - a non-negative tally that is never null, so no nullable
    // wrapper.
    mlir::MLIRContext* const context = _builder.getContext();
    const mlir::Type countElementType = _builder.getIntegerType(64, /*isSigned=*/false);
    const nl::ChunkType countChunkType = nl::ChunkType::get(context, countElementType);

    setInsertionAfterProducingLoop(producingBlock);
    nl::CountResult result = _builder.create<nl::CountResult>(loc, countChunkType, state);

    // db.count's result maps to that chunk, so the db.output that follows lowers
    // into a function-scope nl.output reading it - the block that holds the chunk is
    // the entry block, so lowerOutput places nl.output there.
    _valueMap[count.getResult()] = result.getResult();

    // The count collapses the rows in flight to its one row, so from here on that row is
    // what sizes a projection of constants alone
    _innermostCardinality = result.getResult();
}

void DBLowering::lowerCountScanRows(mlir::db::CountScanRows countScanRows) {
    // The tally reads no column, but it is the rows in flight, so it goes at the top of the
    // root and not of the entry block: in a body run once per row, the ops over its row run
    // once per run.
    _builder.setInsertionPointToStart(_rootBlock);

    nl::CountScanRows rows = _builder.create<nl::CountScanRows>(_builder.getUnknownLoc(),
                                                                countScanRows.getLabelsAttr(),
                                                                countScanRows.getPropertyAttr(),
                                                                countScanRows.getPropertyScanAttr());

    _valueMap[countScanRows.getResult()] = rows.getResult();
    _innermostCardinality = rows.getResult();
}

void DBLowering::lowerAggregate(mlir::Value input, mlir::Value result, storage::AggregateKind kind, bool distinct) {
    // The nl chunk the aggregated column lowered to; the update folds its per-step
    // non-null values into the accumulator. A constant column is reduced over the
    // rows it stands for, so it is laid out over the driving relation first.
    const mlir::Value alignedChunk = rowAlignedChunk(mapValue(input), rowsInFlight());

    const mlir::Value inputChunk = reducedValueChunk(alignedChunk);

    mlir::MLIRContext* const context = _builder.getContext();
    const mlir::Location loc = _builder.getUnknownLoc();

    // A type-erased column is read as the tagged cell it holds; every other input is a
    // nullable value chunk, and the reduction is resolved from the value type it wraps.
    const mlir::Type inputChunkElement = mlir::cast<nl::ChunkType>(inputChunk.getType()).getElementType();
    const bool taggedCells = mlir::isa<storage::AnyType>(inputChunkElement);
    const mlir::Type inputElement = taggedCells
        ? inputChunkElement
        : mlir::cast<storage::NullableType>(inputChunkElement).getValueType();

    // The accumulator (and result) element type: avg widens numbers to f64, the rest
    // keep the input type. This also validates the reduction against the value type.
    const mlir::Type resultElement = aggregateResultElementType(_builder, kind, inputElement);

    // The accumulator is hoisted to the top of the root block, above every loop of
    // this dataflow, so it is reset once per step of that block and dominates the
    // update in the producing loop body and the emit that reads it after the loop.
    _builder.setInsertionPointToStart(_rootBlock);
    const nl::AggregateStateType stateType = nl::AggregateStateType::get(context, resultElement);
    const mlir::Value state = _builder.create<nl::Aggregate>(loc, stateType, kind).getState();

    // sum(DISTINCT x) folds the survivors of a DISTINCT instead of the raw column, so a
    // value repeated across rows is charged once. A null does survive the filter - all
    // nulls share one key - but the update skips nulls as it always does.
    mlir::Value distinctState;
    if (distinct) {
        distinctState = _builder.create<nl::Distinct>(loc).getState();
    }

    // The update sits in the innermost producing loop body, where the aggregated
    // column is bound (the same block db.output would emit from), and folds each
    // step's non-null values into the accumulator.
    mlir::Block* const producingBlock = accumulatorUpdateBlock(ownerBlock(inputChunk));
    setInsertionInto(producingBlock);

    mlir::Value reducedChunk = inputChunk;
    if (distinctState) {
        nl::DistinctFilter filter = _builder.create<nl::DistinctFilter>(loc, distinctState, mlir::ValueRange {inputChunk});
        reducedChunk = filter.getResults().front();
    }

    _builder.create<nl::AggregateUpdate>(loc, state, reducedChunk, kind);

    // Like db.count, an aggregate is a pipeline breaker that collapses to one row,
    // so it opens no emit loop: nl.aggregate_result materializes the reduced value
    // in place at function scope, right after the producing loop. The result is a
    // single-row nullable value chunk - an aggregate can be null (min/max/avg of no
    // non-null row), and sum rides the same representation.
    const nl::ChunkType resultChunkType = nl::ChunkType::get(context, nullableElementType(resultElement));

    setInsertionAfterProducingLoop(producingBlock);
    nl::AggregateResult aggregateResult = _builder.create<nl::AggregateResult>(loc, resultChunkType, state, kind);

    // The db aggregate's result maps to that chunk, so the db.output that follows
    // lowers into a function-scope nl.output reading it, exactly as db.count does.
    _valueMap[result] = aggregateResult.getResult();
    _innermostCardinality = aggregateResult.getResult();
}

void DBLowering::lowerGroupAggregate(mlir::db::GroupAggregate groupAggregate) {
    const mlir::OperandRange columns = groupAggregate.getColumns();
    const uint64_t keyCount = groupAggregate.getKeyCount();
    const llvm::ArrayRef<int64_t> kinds = groupAggregate.getKinds();

    // The nl chunks the columns lowered to: the grouping keys first, then the
    // aggregate inputs. nl.group_aggregate_update appends these to the per-group
    // state, and the emit loop yields the group rows back.
    llvm::SmallVector<mlir::Value, 4> chunks;
    for (const mlir::Value column : columns) {
        chunks.push_back(mapValue(column));
    }

    // GroupAggregate::verify guarantees keyCount >= 1, kinds.size() >= 1 and
    // columns.size() == keyCount + kinds.size(), so reaching an empty column set
    // here means unverified IR - a defensive backstop, as in lowerSort.
    if (chunks.empty()) {
        throw IRException("db.group_aggregate requires at least one column");
    }

    // The group assignment reads one row of every key per row of the step, which a constant
    // key - one value standing for every row - has none of. The keys are laid out first, so
    // that the aggregate inputs behind them are laid out over a key chunk holding rows.
    rowAlignBufferedChunks(llvm::MutableArrayRef<mlir::Value>(chunks).take_front(keyCount));

    // A constant aggregate input is reduced over the rows of the group it falls in,
    // not over the single row it is, so it is laid out over the chunk the grouping
    // keys are read from - the same rows the group assignment is computed for. A
    // reduction then reads its input as a nullable value chunk, so a column carrying
    // no null is laid out as one; a count reads the chunk it is anchored on as it comes.
    for (size_t inputIndex = keyCount; inputIndex < chunks.size(); inputIndex++) {
        chunks[inputIndex] = rowAlignedChunk(chunks[inputIndex], chunks.front());
    }

    for (size_t aggregateIndex = 0; aggregateIndex < kinds.size(); aggregateIndex++) {
        const auto kind = static_cast<storage::GroupAggregateKind>(kinds[aggregateIndex]);
        const size_t chunkIndex = keyCount + aggregateIndex;

        if (reducesValues(kind)) {
            chunks[chunkIndex] = reducedValueChunk(chunks[chunkIndex]);
        }
    }

    mlir::MLIRContext* const context = _builder.getContext();
    const mlir::Location loc = _builder.getUnknownLoc();

    // The accumulator - with its keyCount / aggregate-kind spec - is hoisted to the
    // top of the root block, above every loop of this dataflow, so the group table
    // exists before the producing loop fills it and the handle dominates the update
    // and the emit loop.
    // The grouped sibling of lowerSort's nl.sort_buffer.
    _builder.setInsertionPointToStart(_rootBlock);
    nl::GroupAggregateBuffer bufferOp = _builder.create<nl::GroupAggregateBuffer>(loc,
                                                                                 keyCount,
                                                                                 groupAggregate.getKindsAttr());
    const mlir::Value state = bufferOp.getState();

    // The update folds each step's chunk of every column into the per-group state.
    // It sits in the innermost producing loop body, where all columns are bound
    // together (the same block db.output would emit from), so the group assignment
    // and the per-group folds stay row-aligned.
    const mlir::Value representative = chunks.front();
    setInsertionInto(ownerBlock(representative));
    _builder.create<nl::GroupAggregateUpdate>(loc, state, chunks);

    // The emit iterator yields one chunk per output column: the grouping-key columns
    // (same chunk types as the key inputs) followed by the aggregate results. A count
    // result is a single non-null unsigned i64 per group; sum/min/max keep the
    // input's value type and avg widens numbers to f64 - all resolved here from each
    // kind and its input chunk.
    llvm::SmallVector<mlir::Type, 4> chunkTypes;
    for (size_t keyIndex = 0; keyIndex < keyCount; keyIndex++) {
        chunkTypes.push_back(chunks[keyIndex].getType());
    }

    const mlir::Type ui64Element = _builder.getIntegerType(64, /*isSigned=*/false);
    const nl::ChunkType countChunkType = nl::ChunkType::get(context, ui64Element);

    for (size_t aggregateIndex = 0; aggregateIndex < kinds.size(); aggregateIndex++) {
        const auto kind = static_cast<storage::GroupAggregateKind>(kinds[aggregateIndex]);
        const mlir::Value inputChunk = chunks[keyCount + aggregateIndex];

        chunkTypes.push_back(groupAggregateResultChunkType(_builder, kind, inputChunk, countChunkType));
    }

    const nl::IteratorType iteratorType = nl::IteratorType::get(context, chunkTypes);

    // The emit phase is an nl.group_aggregate source iterator plus its nl.for, placed
    // after the producing loop (before the func.return) so every row has been folded
    // when the loop first steps. buildLoopForSource binds one loop variable per
    // output column and maps db.group_aggregate's results to them, so the db.output
    // that follows lowers into the emit loop body reading the group rows. As for a
    // sort, this emit loop is the only loop of this aggregation a limit may bound -
    // assignProducerLoops stops at the breaker and hands the handle to it, never to
    // the producing loops, which had to fold every row.
    setInsertionInto(_rootBlock);
    nl::GroupAggregate groupOp = _builder.create<nl::GroupAggregate>(loc, iteratorType, state);
    buildLoopForSource(groupOp.getResult(), groupAggregate.getOperation());
}

void DBLowering::lowerCollect(mlir::db::Collect collect) {
    const mlir::OperandRange columns = collect.getColumns();
    const uint64_t keyCount = collect.getKeyCount();
    const llvm::ArrayRef<int64_t> kinds = collect.getKinds().value_or(llvm::ArrayRef<int64_t> {});

    // The nl chunks the columns lowered to: the grouping keys first, then the collected
    // value columns, then the aggregate inputs. nl.collect_update appends these to the
    // per-group lists.
    llvm::SmallVector<mlir::Value, 4> chunks;
    for (const mlir::Value column : columns) {
        chunks.push_back(mapValue(column));
    }

    // Collect::verify guarantees at least one value column after the keys, so an empty
    // column set here means unverified IR - a defensive backstop, as in
    // lowerGroupAggregate.
    if (chunks.empty()) {
        throw IRException("db.collect requires at least one column");
    }

    const size_t valueCount = chunks.size() - keyCount - kinds.size();

    // The grouping keys are laid out first, as lowerGroupAggregate lays its own: the group
    // assignment reads a row of every key per row of the step, which a constant key has none of.
    rowAlignBufferedChunks(llvm::MutableArrayRef<mlir::Value>(chunks).take_front(keyCount));

    // A constant collected column is folded over the rows of the group it falls in, not
    // over the single row it is, so it is laid out over the chunk the grouping keys are
    // read from - the driving relation when the collect has no keys. The aggregate
    // inputs beside it are laid out the same way, as lowerGroupAggregate lays its own.
    const mlir::Value cardinality = keyCount > 0 ? chunks.front() : rowsInFlight();
    for (size_t inputIndex = keyCount; inputIndex < chunks.size(); inputIndex++) {
        chunks[inputIndex] = rowAlignedChunk(chunks[inputIndex], cardinality);
    }

    for (size_t valueIndex = 0; valueIndex < valueCount; valueIndex++) {
        const size_t chunkIndex = keyCount + valueIndex;
        chunks[chunkIndex] = listItemChunk(chunks[chunkIndex]);
    }

    // A reduction beside the lists reads its input as it does under a grouped
    // aggregation; a count tallies rows and takes the chunk as it is.
    for (size_t aggregateIndex = 0; aggregateIndex < kinds.size(); aggregateIndex++) {
        const auto kind = static_cast<storage::GroupAggregateKind>(kinds[aggregateIndex]);
        const size_t chunkIndex = keyCount + valueCount + aggregateIndex;

        if (reducesValues(kind)) {
            chunks[chunkIndex] = reducedValueChunk(chunks[chunkIndex]);
        }
    }

    const mlir::Location loc = _builder.getUnknownLoc();

    // The accumulator is hoisted to the top of the root block, above every loop of
    // this dataflow, so the group table exists before the producing loop fills it and
    // the handle dominates the update. The collect sibling of lowerGroupAggregate's
    // buffer.
    _builder.setInsertionPointToStart(_rootBlock);
    nl::CollectBuffer bufferOp = _builder.create<nl::CollectBuffer>(loc,
                                                                    keyCount,
                                                                    collect.getKindsAttr(),
                                                                    collect.getDistinctValuesAttr());
    const mlir::Value state = bufferOp.getState();

    // The update folds each step's chunk of every column into the per-group lists. It
    // sits in the innermost producing loop body, where all columns are bound together
    // (the same block db.output would emit from), so the group assignment and the
    // per-group appends stay row-aligned.
    const mlir::Value representative = chunks.front();
    setInsertionInto(accumulatorUpdateBlock(ownerBlock(representative)));
    _builder.create<nl::CollectUpdate>(loc, state, chunks);

    // The emit phase: an nl.collect source iterator yielding one row per group - the
    // key columns then one per-group list cell per collected column - drained by an
    // nl.for after the producing loop. Each list chunk's element is the resolved value
    // type (unwrapped from the collected column's nullable) wrapped in a storage list;
    // the key chunks keep their input types.
    mlir::MLIRContext* const context = _builder.getContext();

    llvm::SmallVector<mlir::Type, 4> chunkTypes;
    for (uint64_t keyIndex = 0; keyIndex < keyCount; keyIndex++) {
        chunkTypes.push_back(chunks[keyIndex].getType());
    }

    for (size_t valueIndex = 0; valueIndex < valueCount; valueIndex++) {
        const mlir::Type valueElement = mlir::cast<nl::ChunkType>(chunks[keyCount + valueIndex].getType()).getElementType();

        mlir::Type listElement = valueElement;
        if (const auto nullable = mlir::dyn_cast<storage::NullableType>(valueElement)) {
            listElement = nullable.getValueType();
        }

        chunkTypes.push_back(nl::ChunkType::get(context, storage::ListType::get(context, listElement)));
    }

    const mlir::Type ui64Element = _builder.getIntegerType(64, /*isSigned=*/false);
    const nl::ChunkType countChunkType = nl::ChunkType::get(context, ui64Element);

    for (size_t aggregateIndex = 0; aggregateIndex < kinds.size(); aggregateIndex++) {
        const storage::GroupAggregateKind kind = static_cast<storage::GroupAggregateKind>(kinds[aggregateIndex]);
        const mlir::Value inputChunk = chunks[keyCount + valueCount + aggregateIndex];

        chunkTypes.push_back(groupAggregateResultChunkType(_builder, kind, inputChunk, countChunkType));
    }

    const nl::IteratorType iteratorType = nl::IteratorType::get(context, chunkTypes);

    setInsertionInto(_rootBlock);
    nl::Collect collectOp = _builder.create<nl::Collect>(loc, iteratorType, state);
    buildLoopForSource(collectOp.getResult(), collect.getOperation());
}

void DBLowering::lowerUnwindCollect(mlir::db::UnwindCollect unwindCollect) {
    const mlir::OperandRange columns = unwindCollect.getColumns();
    const uint64_t keyCount = unwindCollect.getKeyCount();

    // The nl chunks the columns lowered to: the grouping keys first, then the single
    // collected value column.
    llvm::SmallVector<mlir::Value, 4> chunks;
    for (const mlir::Value column : columns) {
        chunks.push_back(mapValue(column));
    }

    // UnwindCollect::verify guarantees columns.size() == keyCount + 1, so an empty
    // column set here means unverified IR - a defensive backstop.
    if (chunks.empty()) {
        throw IRException("db.unwind_collect requires at least one column");
    }

    mlir::MLIRContext* const context = _builder.getContext();
    const mlir::Location loc = _builder.getUnknownLoc();

    // The accumulate phase is identical to lowerCollect: a hoisted nl.collect_buffer
    // and an nl.collect_update in the producing loop body.
    _builder.setInsertionPointToStart(_rootBlock);
    nl::CollectBuffer bufferOp = _builder.create<nl::CollectBuffer>(loc,
                                                                    keyCount,
                                                                    mlir::DenseI64ArrayAttr {},
                                                                    mlir::DenseI64ArrayAttr {});
    const mlir::Value state = bufferOp.getState();

    const mlir::Value representative = chunks.front();
    setInsertionInto(accumulatorUpdateBlock(ownerBlock(representative)));
    _builder.create<nl::CollectUpdate>(loc, state, chunks);

    // The emit phase: an nl.unwind_collect source iterator yielding one row per element - the
    // key columns then the unwound value - drained by an nl.for. The value chunk keeps
    // the collected column's type (a nullable value chunk); the keys keep theirs.
    llvm::SmallVector<mlir::Type, 4> chunkTypes;
    for (uint64_t keyIndex = 0; keyIndex < keyCount; keyIndex++) {
        chunkTypes.push_back(chunks[keyIndex].getType());
    }

    chunkTypes.push_back(chunks[keyCount].getType());

    const nl::IteratorType iteratorType = nl::IteratorType::get(context, chunkTypes);

    setInsertionInto(_rootBlock);
    nl::UnwindCollect unwindCollectOp = _builder.create<nl::UnwindCollect>(loc, iteratorType, state);
    buildLoopForSource(unwindCollectOp.getResult(), unwindCollect.getOperation());
}

// A type-erased column of tagged cells - what a list mixing types, holding a null or
// holding nothing unwinds into - is read as it stands, every cell carrying its own tag. A
// list or a map, which min and max order as a whole, rides a nullable column of its own type.
mlir::Value DBLowering::reducedValueChunk(mlir::Value chunk) {
    const mlir::Type element = mlir::cast<nl::ChunkType>(chunk.getType()).getElementType();

    if (mlir::isa<storage::AnyType>(element)) {
        return chunk;
    } else if (mlir::isa<storage::ListType, storage::MapType>(element)) {
        return toNullableChunk(chunk, element);
    } else {
        return nullableValueChunk(chunk);
    }
}

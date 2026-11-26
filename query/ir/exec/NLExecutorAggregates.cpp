#include "NLExecutor.h"

#include <stdint.h>
#include <algorithm>
#include <span>
#include <type_traits>

#include "ID.h"
#include "columns/ColumnOptVector.h"
#include "columns/ColumnVector.h"
#include "metadata/PropertyType.h"

#include "reader/GraphReader.h"

#include "NLProgram.h"
#include "NLRuntimeProperty.h"
#include "NLAggregateStates.h"
#include "NLTaggedCells.h"

#include "IRException.h"
#include "BioAssert.h"

using namespace db;

namespace {

// Count the non-null cells of a type-erased column of tagged scalars, so count(x) over a
// heterogeneous unwind charges the same rows a nullable value column would.
size_t countNonNullElementsColumn(const Column* column) {
    const auto& raw = static_cast<const ColumnVector<CellView>*>(column)->getRaw();
    return std::count_if(raw.begin(), raw.end(), [](const CellView element) {
        return element.getTag() != CellTag::Null;
    });
}

// An entity an OPTIONAL MATCH did not match is an invalid ID, which is a null, so an ID
// chunk charges only the rows holding a valid one - Cypher's count(x) counts the non-null
// rows. The ID sibling of countPresentColumn, for the one column family whose null is not
// a missing optional.
template <typename ID>
size_t countValidIDs(const Column* column) {
    const std::vector<ID>& raw = static_cast<const ColumnVector<ID>*>(column)->getRaw();

    return std::ranges::count_if(raw, [](const ID id) { return id.isValid(); });
}

size_t countPresentPaths(const Column* column) {
    const std::vector<GraphPath>& raw = static_cast<const ColumnVector<GraphPath>*>(column)->getRaw();

    return std::ranges::count_if(raw, [](const GraphPath& path) { return !path.isNull(); });
}

// Count the present (non-null) values of a nullable value column, so Cypher count(x)
// charges only the rows in which x is not null.
template <typename Primitive>
size_t countPresentColumn(const Column* column) {
    const ColumnOptVector<Primitive>* typedColumn = static_cast<const ColumnOptVector<Primitive>*>(column);
    return typedColumn->size() - typedColumn->getNullCount();
}

// Fold values[first, last) into a running min (IsMax false) or max (IsMax true), noting in
// @param replaced whether any value replaced it. A number's order is a strict weak order, so
// a value replaced the seed exactly when the result replaces it.
template <typename Primitive, bool IsMax>
void foldExtremum(const Primitive* values, size_t first, size_t last, Primitive& extreme, bool& replaced) {
    Primitive running = extreme;

    if constexpr (std::is_arithmetic_v<Primitive>) {
        for (size_t row = first; row < last; row++) {
            running = replacesExtremum<Primitive, IsMax>(running, values[row]) ? values[row] : running;
        }

        replaced |= replacesExtremum<Primitive, IsMax>(extreme, running);
    } else {
        for (size_t row = first; row < last; row++) {
            if (replacesExtremum<Primitive, IsMax>(running, values[row])) {
                running = values[row];
                replaced = true;
            }
        }
    }

    extreme = running;
}

template <typename State>
State& placedState(std::byte* place) {
    return *std::launder(reinterpret_cast<State*>(place));
}

// Fold a chunk's values into a keyless sum. A null row adds the zero its slot holds.
template <typename Primitive>
void aggregateFoldSum(std::byte* place, const Column* input, NLCompactingValueStore& values) {
    Primitive& sum = placedState<Primitive>(place);
    const ColumnOptVector<Primitive>* typedInput = static_cast<const ColumnOptVector<Primitive>*>(input);

    Primitive running = sum;
    for (const Primitive value : typedInput->getValues()) {
        running = numericAdd(running, value);
    }

    sum = running;
}

// Fold a chunk's present values into a keyless average: a total widened to f64, or the running
// mean of points, and the count
template <typename Primitive>
void aggregateFoldAvg(std::byte* place, const Column* input, NLCompactingValueStore& values) {
    using State = NLAverageState<AverageTotal<Primitive>>;

    State& average = placedState<State>(place);
    const ColumnOptVector<Primitive>* typedInput = static_cast<const ColumnOptVector<Primitive>*>(input);

    const std::vector<Primitive>& inputValues = typedInput->getValues();

    AverageTotal<Primitive> total = average._total;
    uint64_t count = average._count;

    if constexpr (std::is_same_v<Primitive, types::Point::Primitive>) {
        typedInput->forEachPresentRun(0, inputValues.size(), [&](size_t first, size_t last) {
            for (size_t row = first; row < last; row++) {
                total = addToAverageTotal(total, inputValues[row], count);
                count++;
            }
        });
    } else {
        for (const Primitive value : inputValues) {
            total = addToAverageTotal(total, value, count);
        }

        count += inputValues.size() - typedInput->getNullCount();
    }

    average._total = total;
    average._count = count;
}

// Fold a chunk's present durations into the 128-bit total of a keyless sum, or of an average
// beside its count
template <bool Averages>
void aggregateFoldDurationTotal(std::byte* place, const Column* input, NLCompactingValueStore& values) {
    const ColumnOptVector<types::Duration::Primitive>* typedInput =
        static_cast<const ColumnOptVector<types::Duration::Primitive>*>(input);

    __int128 total = 0;
    for (const types::Duration::Primitive value : typedInput->getValues()) {
        total += value.getMicroseconds();
    }

    const uint64_t count = typedInput->size() - typedInput->getNullCount();

    if constexpr (Averages) {
        NLAverageState<__int128>& average = placedState<NLAverageState<__int128>>(place);
        average._total += total;
        average._count += count;
    } else {
        placedState<__int128>(place) += total;
    }
}

template <typename InputColumn>
void aggregateFoldDeviation(std::byte* place, const Column* input, NLCompactingValueStore& values) {
    NLDeviationState& state = placedState<NLDeviationState>(place);
    const InputColumn* typedInput = static_cast<const InputColumn*>(input);

    uint64_t count = state._count;
    double mean = state._mean;
    double squaredDeviations = state._squaredDeviations;

    const size_t rowCount = typedInput->size();
    for (size_t row = 0; row < rowCount; row++) {
        const auto value = presentRow(typedInput, row);
        if (value.has_value()) {
            foldDeviation(numberOf(*value), mean, squaredDeviations, count);
        }
    }

    state._count = count;
    state._mean = mean;
    state._squaredDeviations = squaredDeviations;
}

// Fold a chunk's present values into a keyless min (IsMax false) or max (IsMax true). The
// chunk's extreme is found first, then kept once if it replaces the one kept before, so the
// store holds one copy of a view rather than one per row.
template <typename Primitive, bool IsMax, typename InputColumn = ColumnOptVector<Primitive>>
void aggregateFoldMinMax(std::byte* place, const Column* input, NLCompactingValueStore& values) {
    NLKeptValue<Primitive>& kept = placedState<NLKeptValue<Primitive>>(place);
    const InputColumn* typedInput = static_cast<const InputColumn*>(input);

    std::optional<Primitive> extreme;
    if (kept.isKept()) {
        extreme = kept.getValue();
    }

    bool replaced = false;
    const size_t rowCount = typedInput->size();

    if constexpr (std::is_same_v<InputColumn, ColumnOptVector<Primitive>>) {
        const Primitive* inputValues = typedInput->getValues().data();

        typedInput->forEachPresentRun(0, rowCount, [&](size_t first, size_t last) {
            if (!extreme.has_value()) {
                extreme = inputValues[first];
                replaced = true;
                first++;
            }

            foldExtremum<Primitive, IsMax>(inputValues, first, last, *extreme, replaced);
        });
    } else {
        for (size_t row = 0; row < rowCount; row++) {
            const std::optional<Primitive> value = presentRow(typedInput, row);
            if (!value.has_value()) {
                continue;
            }

            if (!extreme.has_value() || replacesExtremum<Primitive, IsMax>(*extreme, *value)) {
                extreme = *value;
                replaced = true;
            }
        }
    }

    if (replaced) {
        kept.keep(*extreme, values.getSpare());
        values.flip();
    }
}

// Fold a chunk of tagged cells into a keyless sum, or an average beside its count. A cell tagged
// null is skipped, as a null value column row is.
template <bool Averages>
void aggregateFoldTaggedSum(std::byte* place, const Column* input, NLCompactingValueStore& values) {
    using State = std::conditional_t<Averages, NLAverageState<NLTaggedSum>, NLTaggedSum>;

    State& state = placedState<State>(place);
    const std::vector<CellView>& inputRaw = static_cast<const ColumnVector<CellView>*>(input)->getRaw();

    State running = state;
    for (const CellView& cell : inputRaw) {
        const std::optional<CellView> value = presentCell(cell);
        if (!value) {
            continue;
        }

        if constexpr (Averages) {
            running._total.addToAverage(*value, running._count);
            running._count++;
        } else {
            running.add(*value);
        }
    }

    state = running;
}

// The fold of a keyless sum over a column of this value type. sum adds the values, so only a
// numeric or a duration column is valid.
NLAggregateFoldFunction selectSumFold(ValueType inputType) {
    switch (inputType) {
        case ValueType::Int64:
            return &aggregateFoldSum<types::Int64::Primitive>;
        break;

        case ValueType::UInt64:
            return &aggregateFoldSum<types::UInt64::Primitive>;
        break;

        case ValueType::Double:
            return &aggregateFoldSum<types::Double::Primitive>;
        break;

        case ValueType::Duration:
            return &aggregateFoldDurationTotal</*Averages=*/false>;
        break;

        default:
            throw IRException("sum requires a numeric or a duration column");
        break;
    }

    return nullptr;
}

NLAggregateFoldFunction selectAvgFold(ValueType inputType) {
    switch (inputType) {
        case ValueType::Int64:
            return &aggregateFoldAvg<types::Int64::Primitive>;
        break;

        case ValueType::UInt64:
            return &aggregateFoldAvg<types::UInt64::Primitive>;
        break;

        case ValueType::Double:
            return &aggregateFoldAvg<types::Double::Primitive>;
        break;

        case ValueType::Duration:
            return &aggregateFoldDurationTotal</*Averages=*/true>;
        break;

        case ValueType::Point:
            return &aggregateFoldAvg<types::Point::Primitive>;
        break;

        default:
            throw IRException("avg requires a numeric, a duration or a point column");
        break;
    }

    return nullptr;
}

NLAggregateFoldFunction selectDeviationFold(ValueType inputType) {
    switch (inputType) {
        case ValueType::Int64:
            return &aggregateFoldDeviation<ColumnOptVector<types::Int64::Primitive>>;
        break;

        case ValueType::UInt64:
            return &aggregateFoldDeviation<ColumnOptVector<types::UInt64::Primitive>>;
        break;

        case ValueType::Double:
            return &aggregateFoldDeviation<ColumnOptVector<types::Double::Primitive>>;
        break;

        default:
            throw IRException("stdev requires a numeric column");
        break;
    }

    return nullptr;
}

template <bool IsMax>
NLAggregateFoldFunction selectMinMaxFold(ValueType inputType) {
    switch (inputType) {
        case ValueType::Int64:
            return &aggregateFoldMinMax<types::Int64::Primitive, IsMax>;
        break;

        case ValueType::UInt64:
            return &aggregateFoldMinMax<types::UInt64::Primitive, IsMax>;
        break;

        case ValueType::Double:
            return &aggregateFoldMinMax<types::Double::Primitive, IsMax>;
        break;

        case ValueType::Bool:
            return &aggregateFoldMinMax<types::Bool::Primitive, IsMax>;
        break;

        case ValueType::String:
            return &aggregateFoldMinMax<types::String::Primitive, IsMax>;
        break;

        case ValueType::DateTime:
            return &aggregateFoldMinMax<types::DateTime::Primitive, IsMax>;
        break;

        case ValueType::Duration:
            return &aggregateFoldMinMax<types::Duration::Primitive, IsMax>;
        break;

        case ValueType::Point:
            return &aggregateFoldMinMax<types::Point::Primitive, IsMax>;
        break;

        case ValueType::List:
            return &aggregateFoldMinMax<types::List::Primitive, IsMax>;
        break;

        case ValueType::Map:
            return &aggregateFoldMinMax<types::Map::Primitive, IsMax>;
        break;

        default:
            throw IRException("min/max requires an orderable column");
        break;
    }

    return nullptr;
}

NLAggregateFoldFunction taggedAggregateFoldFor(AggregateKind kind) {
    switch (kind) {
        case AggregateKind::Sum:
            return &aggregateFoldTaggedSum</*Averages=*/false>;
        break;

        case AggregateKind::Avg:
            return &aggregateFoldTaggedSum</*Averages=*/true>;
        break;

        case AggregateKind::StDev:
        case AggregateKind::StDevP:
            return &aggregateFoldDeviation<ColumnVector<CellView>>;
        break;

        case AggregateKind::Min:
            return &aggregateFoldMinMax<CellView, /*IsMax=*/false, ColumnVector<CellView>>;
        break;

        case AggregateKind::Max:
            return &aggregateFoldMinMax<CellView, /*IsMax=*/true, ColumnVector<CellView>>;
        break;
    }

    bioassert(false, "Unhandled aggregate kind");
    return nullptr;
}

}

void NLExecutor::runCountReset(NLExecutionContext* context, NLFunctionData* data) {
    const NLCountResetData* reset = static_cast<NLCountResetData*>(data);
    reset->getState()->reset();
}

void NLExecutor::runCountUpdate(NLExecutionContext* context, NLFunctionData* data) {
    const NLCountUpdateData* update = static_cast<NLCountUpdateData*>(data);

    // Charge this step's non-null rows: the handle returns all rows for an ID
    // chunk, the present values for a nullable value chunk.
    const NLCountFunction count = update->getCount();
    update->getState()->add(count(update->getRows()));
}

void NLExecutor::runCountResult(NLExecutionContext* context, NLFunctionData* data) {
    const NLCountResultData* result = static_cast<NLCountResultData*>(data);

    // The aggregate collapses every counted row to a single result row: write the
    // final tally into the output chunk as one unsigned i64 (the !nl.chunk<ui64>
    // count column). Runs after the producing loop, so the counter holds the whole
    // dataflow's count; nl.output emits this chunk at function scope.
    const size_t count = result->getState()->getCount();
    ColumnVector<uint64_t>* output = static_cast<ColumnVector<uint64_t>*>(result->getOutput());
    std::vector<uint64_t>& raw = output->getRaw();
    raw.assign(1, static_cast<uint64_t>(count));
}

void NLExecutor::runCountScanRows(NLExecutionContext* context, NLFunctionData* data) {
    const NLCountScanRowsData* scanRows = static_cast<NLCountScanRowsData*>(data);

    ColumnVector<uint64_t>* output = static_cast<ColumnVector<uint64_t>*>(scanRows->getOutput());
    std::vector<uint64_t>& raw = output->getRaw();

    // A conjunction naming a label, or a property, the schema never had matches no node, so
    // the scan it stands for contributes no row and the product is empty.
    if (!scanRows->isMatchable()) {
        raw.assign(1, 0);
        return;
    }

    // Each scan contributes the nodes carrying its labels, and crossed together they make
    // the product of those counts. An empty conjunction filters nothing, so its scan is
    // every node of the graph; a named property narrows the one scan it is read from to the
    // nodes that hold it, which is what count(b.name) tallies over a product.
    const GraphReader reader(*context->getView());
    const std::span<const LabelSet> labelSets = scanRows->getLabelSets();
    const PropertyTypeID propertyTypeID = scanRows->getPropertyTypeID();
    const size_t propertyScan = scanRows->getPropertyScan();

    size_t rows = 1;
    for (size_t scanIndex = 0; scanIndex < labelSets.size(); scanIndex++) {
        const LabelSet& labelset = labelSets[scanIndex];
        const LabelSetHandle labelsetHandle {labelset};

        if (propertyTypeID.isValid() && scanIndex == propertyScan) {
            rows *= reader.getNodeCountWithProperty(labelsetHandle, propertyTypeID);
        } else if (labelset.empty()) {
            rows *= reader.getNodeCount();
        } else {
            rows *= reader.getNodeCountMatchingLabelset(labelsetHandle);
        }
    }

    raw.assign(1, static_cast<uint64_t>(rows));
}

void NLExecutor::runAggregateReset(NLExecutionContext* context, NLFunctionData* data) {
    const NLAggregateResetData* reset = static_cast<NLAggregateResetData*>(data);
    reset->getState()->reset();
}

void NLExecutor::runAggregateUpdate(NLExecutionContext* context, NLFunctionData* data) {
    const NLAggregateUpdateData* update = static_cast<NLAggregateUpdateData*>(data);
    NLAggregateState* state = update->getState();

    // Fold this step's non-null values into the state the way its kind and value type
    // demand (add for sum/avg, keep the extreme for min/max).
    update->getFold()(state->getPlace(), update->getInput(), state->getValues());
}

void NLExecutor::runAggregateResult(NLExecutionContext* context, NLFunctionData* data) {
    const NLAggregateResultData* result = static_cast<NLAggregateResultData*>(data);

    // The aggregate collapses every folded row to a single result row: write the
    // reduced value into the output chunk's one nullable row. Runs after the
    // producing loop, so the state holds the whole dataflow's reduction;
    // nl.output emits this chunk at function scope.
    NLStepBuffers* const stepBuffers = result->getStepBuffers();
    stepBuffers->clear();

    result->getState()->emit(result->getOutput(), stepBuffers);
}

NLCountFunction NLExecutor::selectCellCountFunction() {
    return &countNonNullElementsColumn;
}

NLCountFunction NLExecutor::selectOptListCountFunction() {
    return &countPresentColumn<ListView>;
}

// count(x) reads has_value(), never the value's bytes, so unlike
// selectOptKeyAppendFunction it counts an embedding
NLCountFunction NLExecutor::selectOptCountFunction(NLChunkKind kind) {
    NLCountFunction count = nullptr;
    dispatchOptChunkKind(kind, [&]<typename Element>() { count = &countPresentColumn<Element>; });

    return count;
}

// An ID chunk (node/edge/edge-type IDs) has no null rows, so every row counts,
// regardless of kind. The count sibling of selectKeyAppendFunction, but kind is
// irrelevant here - the row count is just the column size.
size_t NLExecutor::countAllRows(const Column* column) {
    return column->size();
}

// Selected per column from its kind, so count(n) tallies only the rows in which the node
// or edge n is not null. A kind whose rows are never null - a label or edge-type ID, or a
// scalar a procedure yielded - has no invalid value to skip and gets no handle here.
NLCountFunction NLExecutor::selectIDCountFunction(NLChunkKind kind) {
    switch (kind) {
        case NLChunkKind::NodeID:
            return &countValidIDs<NodeID>;
        break;

        case NLChunkKind::EdgeID:
            return &countValidIDs<EdgeID>;
        break;

        default:
            return nullptr;
        break;
    }

    return nullptr;
}

NLCountFunction NLExecutor::selectPathCountFunction() {
    return &countPresentPaths;
}

NLAggregateFoldFunction NLExecutor::selectAggregateFold(AggregateKind kind, ValueType inputType) {
    switch (kind) {
        case AggregateKind::Sum:
            return selectSumFold(inputType);
        break;

        case AggregateKind::Avg:
            return selectAvgFold(inputType);
        break;

        case AggregateKind::StDev:
        case AggregateKind::StDevP:
            return selectDeviationFold(inputType);
        break;

        case AggregateKind::Min:
            return selectMinMaxFold</*IsMax=*/false>(inputType);
        break;

        case AggregateKind::Max:
            return selectMinMaxFold</*IsMax=*/true>(inputType);
        break;
    }

    bioassert(false, "Unhandled aggregate kind");
    return nullptr;
}

NLAggregateFoldFunction NLExecutor::selectTaggedAggregateFold(AggregateKind kind) {
    return taggedAggregateFoldFor(kind);
}

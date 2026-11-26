#include "NLExecutor.h"

#include <stdint.h>
#include <algorithm>
#include <type_traits>

#include "ID.h"
#include "columns/ColumnOptVector.h"
#include "columns/ColumnVector.h"
#include "metadata/PropertyType.h"

#include "NLProgram.h"
#include "NLRuntimeProperty.h"
#include "NLAggregateStates.h"
#include "NLLoopSteps.h"
#include "NLTaggedCells.h"

#include "IRException.h"
#include "BioAssert.h"

using namespace db;

namespace {

// stDev(DISTINCT x) moves a group's state once per distinct value it sees
template <typename InputColumn, bool Distinct>
void groupFoldDeviation(NLGroupStatePlaces places,
                        const Column* input,
                        const std::vector<size_t>& groups,
                        size_t beginRow,
                        size_t endRow,
                        const NLGroupDistinctTally* distinct,
                        NLValueStore& values) {
    const InputColumn* typedInput = static_cast<const InputColumn*>(input);

    for (size_t row = beginRow; row < endRow; row++) {
        const auto value = presentRow(typedInput, row);
        if (!value.has_value()) {
            continue;
        }

        const size_t group = groups[row];

        if constexpr (Distinct) {
            if (!distinct->isNew(row)) {
                continue;
            }
        }

        NLDeviationState& state = places.getState<NLDeviationState>(group);
        foldDeviation(numberOf(*value), state._mean, state._squaredDeviations, state._count);
    }
}

// The fold of a standard deviation over a column of this value type: like avg's, a
// numeric column only
template <bool Distinct>
NLGroupAggregateFoldFunction selectGroupDeviationFold(ValueType inputType) {
    switch (inputType) {
        case ValueType::Int64:
            return &groupFoldDeviation<ColumnOptVector<types::Int64::Primitive>, Distinct>;
        break;

        case ValueType::UInt64:
            return &groupFoldDeviation<ColumnOptVector<types::UInt64::Primitive>, Distinct>;
        break;

        case ValueType::Double:
            return &groupFoldDeviation<ColumnOptVector<types::Double::Primitive>, Distinct>;
        break;

        default:
            throw IRException("stdev requires a numeric column");
        break;
    }

    return nullptr;
}

// Fold a chunk's values into per-group sums. Each new group starts at zero, so the value is
// added into its running sum, and a null row adds the zero its slot holds.
template <typename Primitive>
void groupFoldSum(NLGroupStatePlaces places,
                  const Column* input,
                  const std::vector<size_t>& groups,
                  size_t beginRow,
                  size_t endRow,
                  const NLGroupDistinctTally* distinct,
                  NLValueStore& values) {
    const std::vector<Primitive>& inputValues = static_cast<const ColumnOptVector<Primitive>*>(input)->getValues();

    for (size_t row = beginRow; row < endRow; row++) {
        Primitive& running = places.getState<Primitive>(groups[row]);
        running = numericAdd(running, inputValues[row]);
    }
}

// Fold a chunk's distinct present values into per-group sums (sum(DISTINCT x)): a value its
// group has already been charged is skipped, so each distinct value is added once however
// many rows carry it.
template <typename Primitive>
void groupFoldSumDistinct(NLGroupStatePlaces places,
                          const Column* input,
                          const std::vector<size_t>& groups,
                          size_t beginRow,
                          size_t endRow,
                          const NLGroupDistinctTally* distinct,
                          NLValueStore& values) {
    const ColumnOptVector<Primitive>* typedInput = static_cast<const ColumnOptVector<Primitive>*>(input);

    for (size_t row = beginRow; row < endRow; row++) {
        if (!typedInput->isValid(row)) {
            continue;
        }

        const size_t group = groups[row];

        if (!distinct->isNew(row)) {
            continue;
        }

        Primitive& running = places.getState<Primitive>(group);
        running = numericAdd(running, typedInput->getValue(row));
    }
}

// Fold a chunk's present durations into per-group 128-bit totals, once per distinct value
// of a group for sum(DISTINCT x) and avg(DISTINCT x), tallying the ones an average folds
template <bool Distinct, bool Averages>
void groupFoldDurationTotal(NLGroupStatePlaces places,
                            const Column* input,
                            const std::vector<size_t>& groups,
                            size_t beginRow,
                            size_t endRow,
                            const NLGroupDistinctTally* distinct,
                            NLValueStore& values) {
    const ColumnOptVector<types::Duration::Primitive>* typedInput =
        static_cast<const ColumnOptVector<types::Duration::Primitive>*>(input);

    for (size_t row = beginRow; row < endRow; row++) {
        if (!typedInput->isValid(row)) {
            continue;
        }

        const size_t group = groups[row];

        if constexpr (Distinct) {
            if (!distinct->isNew(row)) {
                continue;
            }
        }

        const __int128 microseconds = typedInput->getValue(row).getMicroseconds();

        if constexpr (Averages) {
            NLAverageState<__int128>& average = places.getState<NLAverageState<__int128>>(group);
            average._total += microseconds;
            average._count++;
        } else {
            places.getState<__int128>(group) += microseconds;
        }
    }
}

// Fold a group's tagged cells into its running sum in place, tallying the ones an average
// folds. The type-erased sibling of groupFoldSum and groupFoldAvg.
template <bool Distinct, bool Averages>
void groupFoldTaggedSum(NLGroupStatePlaces places,
                        const Column* input,
                        const std::vector<size_t>& groups,
                        size_t beginRow,
                        size_t endRow,
                        const NLGroupDistinctTally* distinct,
                        NLValueStore& values) {
    const std::vector<CellView>& inputRaw = static_cast<const ColumnVector<CellView>*>(input)->getRaw();

    for (size_t row = beginRow; row < endRow; row++) {
        const std::optional<CellView> value = presentCell(inputRaw[row]);
        if (!value) {
            continue;
        }

        const size_t group = groups[row];

        if constexpr (Distinct) {
            if (!distinct->isNew(row)) {
                continue;
            }
        }

        if constexpr (Averages) {
            NLAverageState<NLTaggedSum>& average = places.getState<NLAverageState<NLTaggedSum>>(group);
            average._total.addToAverage(*value, average._count);
            average._count++;
        } else {
            places.getState<NLTaggedSum>(group).add(*value);
        }
    }
}

// Fold a chunk's present values into per-group min (IsMax false) or max (IsMax
// true) states. The first present value of a group seeds it; a later value
// replaces it when more extreme. A group with no present value stays null.
template <typename Primitive, bool IsMax, typename InputColumn = ColumnOptVector<Primitive>>
void groupFoldMinMax(NLGroupStatePlaces places,
                     const Column* input,
                     const std::vector<size_t>& groups,
                     size_t beginRow,
                     size_t endRow,
                     const NLGroupDistinctTally* distinct,
                     NLValueStore& values) {
    const InputColumn* typedInput = static_cast<const InputColumn*>(input);

    if constexpr (std::is_same_v<InputColumn, ColumnOptVector<Primitive>>) {
        const std::vector<Primitive>& inputValues = typedInput->getValues();

        typedInput->forEachPresentRun(beginRow, endRow, [&](size_t first, size_t last) {
            for (size_t row = first; row < last; row++) {
                const Primitive& value = inputValues[row];
                NLKeptValue<Primitive>& current = places.getState<NLKeptValue<Primitive>>(groups[row]);
                if (!current.isKept() || replacesExtremum<Primitive, IsMax>(current.getValue(), value)) {
                    current.keep(value, values);
                }
            }
        });
    } else {
        for (size_t row = beginRow; row < endRow; row++) {
            const std::optional<Primitive> value = presentRow(typedInput, row);
            if (!value.has_value()) {
                continue;
            }

            NLKeptValue<Primitive>& current = places.getState<NLKeptValue<Primitive>>(groups[row]);
            if (!current.isKept() || replacesExtremum<Primitive, IsMax>(current.getValue(), *value)) {
                current.keep(*value, values);
            }
        }
    }
}

// Fold a chunk's present values into per-group first states: the first present value of a
// group is kept and every later one passed over.
template <typename Primitive, typename InputColumn = ColumnOptVector<Primitive>>
void groupFoldFirst(NLGroupStatePlaces places,
                    const Column* input,
                    const std::vector<size_t>& groups,
                    size_t beginRow,
                    size_t endRow,
                    const NLGroupDistinctTally* distinct,
                    NLValueStore& values) {
    const InputColumn* typedInput = static_cast<const InputColumn*>(input);

    for (size_t row = beginRow; row < endRow; row++) {
        NLKeptValue<Primitive>& current = places.getState<NLKeptValue<Primitive>>(groups[row]);
        if (current.isKept()) {
            continue;
        }

        const std::optional<Primitive> value = presentRow(typedInput, row);
        if (value.has_value()) {
            current.keep(*value, values);
        }
    }
}

NLGroupAggregateFoldFunction taggedGroupAggregateFoldFor(GroupAggregateKind kind) {
    switch (kind) {
        case GroupAggregateKind::Sum:
            return &groupFoldTaggedSum</*Distinct=*/false, /*Averages=*/false>;
        break;

        case GroupAggregateKind::SumDistinct:
            return &groupFoldTaggedSum</*Distinct=*/true, /*Averages=*/false>;
        break;

        case GroupAggregateKind::Avg:
            return &groupFoldTaggedSum</*Distinct=*/false, /*Averages=*/true>;
        break;

        case GroupAggregateKind::AvgDistinct:
            return &groupFoldTaggedSum</*Distinct=*/true, /*Averages=*/true>;
        break;

        case GroupAggregateKind::StDev:
        case GroupAggregateKind::StDevP:
            return &groupFoldDeviation<ColumnVector<CellView>, /*Distinct=*/false>;
        break;

        case GroupAggregateKind::StDevDistinct:
        case GroupAggregateKind::StDevPDistinct:
            return &groupFoldDeviation<ColumnVector<CellView>, /*Distinct=*/true>;
        break;

        case GroupAggregateKind::Min:
            return &groupFoldMinMax<CellView, /*IsMax=*/false, ColumnVector<CellView>>;
        break;

        case GroupAggregateKind::Max:
            return &groupFoldMinMax<CellView, /*IsMax=*/true, ColumnVector<CellView>>;
        break;

        case GroupAggregateKind::First:
            return &groupFoldFirst<CellView, ColumnVector<CellView>>;
        break;

        case GroupAggregateKind::Count:
        case GroupAggregateKind::CountDistinct:
        case GroupAggregateKind::CountRows:
            bioassert(false, "A tally of type-erased cells has its own fold");
        break;
    }

    bioassert(false, "Unhandled group aggregate kind");
    return nullptr;
}

// Fold a chunk's present values into per-group averages: a running total (a number widened
// to f64, or the running mean of points) plus the count of the values folded. avg divides
// the two at the emit step.
template <typename Primitive>
void groupFoldAvg(NLGroupStatePlaces places,
                  const Column* input,
                  const std::vector<size_t>& groups,
                  size_t beginRow,
                  size_t endRow,
                  const NLGroupDistinctTally* distinct,
                  NLValueStore& values) {
    using State = NLAverageState<AverageTotal<Primitive>>;

    const ColumnOptVector<Primitive>* typedInput = static_cast<const ColumnOptVector<Primitive>*>(input);
    const std::vector<Primitive>& inputValues = typedInput->getValues();

    typedInput->forEachPresentRun(beginRow, endRow, [&](size_t first, size_t last) {
        for (size_t row = first; row < last; row++) {
            State& average = places.getState<State>(groups[row]);
            average._total = addToAverageTotal(average._total, inputValues[row], average._count);
            average._count++;
        }
    });
}

// Fold a chunk's distinct present values into per-group averages (avg(DISTINCT x)): a value
// its group has already been charged moves neither the total nor the count, so the mean is
// taken over the distinct values.
template <typename Primitive>
void groupFoldAvgDistinct(NLGroupStatePlaces places,
                          const Column* input,
                          const std::vector<size_t>& groups,
                          size_t beginRow,
                          size_t endRow,
                          const NLGroupDistinctTally* distinct,
                          NLValueStore& values) {
    using State = NLAverageState<AverageTotal<Primitive>>;

    const ColumnOptVector<Primitive>* typedInput = static_cast<const ColumnOptVector<Primitive>*>(input);

    for (size_t row = beginRow; row < endRow; row++) {
        if (!typedInput->isValid(row)) {
            continue;
        }

        const size_t group = groups[row];

        if (!distinct->isNew(row)) {
            continue;
        }

        State& average = places.getState<State>(group);
        average._total = addToAverageTotal(average._total, typedInput->getValue(row), average._count);
        average._count++;
    }
}

// Tally every row into its group (count(*) over a never-null column): each row
// charges its group regardless of value, so the input values are never read.
void groupFoldCountAll(NLGroupStatePlaces places,
                       const Column* input,
                       const std::vector<size_t>& groups,
                       size_t beginRow,
                       size_t endRow,
                       const NLGroupDistinctTally* distinct,
                       NLValueStore& values) {
    for (size_t row = beginRow; row < endRow; row++) {
        places.getState<uint64_t>(groups[row])++;
    }
}

// Tally each group's present (non-null) rows (count(x)): a null row is not charged,
// matching Cypher's count(x).
template <typename Primitive>
void groupFoldCountPresent(NLGroupStatePlaces places,
                           const Column* input,
                           const std::vector<size_t>& groups,
                           size_t beginRow,
                           size_t endRow,
                           const NLGroupDistinctTally* distinct,
                           NLValueStore& values) {
    const ColumnOptVector<Primitive>* typedInput = static_cast<const ColumnOptVector<Primitive>*>(input);

    for (size_t row = beginRow; row < endRow; row++) {
        if (typedInput->isValid(row)) {
            places.getState<uint64_t>(groups[row])++;
        }
    }
}

// Tally each group's rows holding a valid ID (count(n) over a node/edge column): an
// invalid ID is a null, so it is not charged - the ID sibling of groupFoldCountPresent.
template <typename ID>
void groupFoldCountValidID(NLGroupStatePlaces places,
                           const Column* input,
                           const std::vector<size_t>& groups,
                           size_t beginRow,
                           size_t endRow,
                           const NLGroupDistinctTally* distinct,
                           NLValueStore& values) {
    const std::vector<ID>& inputRaw = static_cast<const ColumnVector<ID>*>(input)->getRaw();

    for (size_t row = beginRow; row < endRow; row++) {
        if (inputRaw[row].isValid()) {
            places.getState<uint64_t>(groups[row])++;
        }
    }
}

void groupFoldCountPresentPath(NLGroupStatePlaces places,
                               const Column* input,
                               const std::vector<size_t>& groups,
                               size_t beginRow,
                               size_t endRow,
                               const NLGroupDistinctTally* distinct,
                               NLValueStore& values) {
    const std::vector<GraphPath>& inputRaw = static_cast<const ColumnVector<GraphPath>*>(input)->getRaw();

    for (size_t row = beginRow; row < endRow; row++) {
        if (!inputRaw[row].isNull()) {
            places.getState<uint64_t>(groups[row])++;
        }
    }
}

// A row of a chunk an expression or a procedure filled is never null. An ID or a path is
// null on the rows an OPTIONAL MATCH missed.
template <typename ElementType>
bool presentChunkRow(const ElementType& value) {
    return true;
}

template <IntegralType T, int tag>
bool presentChunkRow(const ID<T, tag>& id) {
    return id.isValid();
}

bool presentChunkRow(const GraphPath& path) {
    return !path.isNull();
}

// Tally each group's distinct present values of a chunk. The tally keys a row the way a
// whole-row DISTINCT keys it, so two rows count once exactly when DISTINCT would fold
// them together.
template <typename ElementType>
void groupFoldCountDistinctChunk(NLGroupStatePlaces places,
                                 const Column* input,
                                 const std::vector<size_t>& groups,
                                 size_t beginRow,
                                 size_t endRow,
                                 const NLGroupDistinctTally* distinct,
                                 NLValueStore& values) {
    const std::vector<ElementType>& inputRaw = static_cast<const ColumnVector<ElementType>*>(input)->getRaw();

    for (size_t row = beginRow; row < endRow; row++) {
        if (!presentChunkRow(inputRaw[row])) {
            continue;
        }

        const size_t group = groups[row];

        if (distinct->isNew(row)) {
            places.getState<uint64_t>(group)++;
        }
    }
}

// Tally each group's distinct present values (count(DISTINCT x)): a null row is not
// charged, and a value repeated within its group is charged once. The value bytes are
// the DISTINCT serializer's, so two rows count as one exactly when a whole-row DISTINCT
// would fold them together.
template <typename Primitive>
void groupFoldCountDistinctPresent(NLGroupStatePlaces places,
                                   const Column* input,
                                   const std::vector<size_t>& groups,
                                   size_t beginRow,
                                   size_t endRow,
                                   const NLGroupDistinctTally* distinct,
                                   NLValueStore& values) {
    const ColumnOptVector<Primitive>* typedInput = static_cast<const ColumnOptVector<Primitive>*>(input);

    for (size_t row = beginRow; row < endRow; row++) {
        if (!typedInput->isValid(row)) {
            continue;
        }

        const size_t group = groups[row];

        if (distinct->isNew(row)) {
            places.getState<uint64_t>(group)++;
        }
    }
}

// Tally each group's present cells of a type-erased column of tagged scalars, so a
// grouped count(x) over a heterogeneous unwind charges the same rows a nullable value
// column would.
void groupFoldCountPresentCell(NLGroupStatePlaces places,
                               const Column* input,
                               const std::vector<size_t>& groups,
                               size_t beginRow,
                               size_t endRow,
                               const NLGroupDistinctTally* distinct,
                               NLValueStore& values) {
    const std::vector<CellView>& inputRaw = static_cast<const ColumnVector<CellView>*>(input)->getRaw();

    for (size_t row = beginRow; row < endRow; row++) {
        if (presentCell(inputRaw[row])) {
            places.getState<uint64_t>(groups[row])++;
        }
    }
}

// Tally each group's distinct present cells of a type-erased column of tagged scalars.
// The key carries the tag as well as the value, so cells of different types are told
// apart the way a whole-row DISTINCT tells them apart.
void groupFoldCountDistinctCell(NLGroupStatePlaces places,
                                const Column* input,
                                const std::vector<size_t>& groups,
                                size_t beginRow,
                                size_t endRow,
                                const NLGroupDistinctTally* distinct,
                                NLValueStore& values) {
    const std::vector<CellView>& inputRaw = static_cast<const ColumnVector<CellView>*>(input)->getRaw();

    for (size_t row = beginRow; row < endRow; row++) {
        const std::optional<CellView> element = presentCell(inputRaw[row]);
        if (!element) {
            continue;
        }

        const size_t group = groups[row];

        if (distinct->isNew(row)) {
            places.getState<uint64_t>(group)++;
        }
    }
}

// The grouped sum fold for a column of this value type. sum adds the values, so
// only a numeric or a duration column is valid - the same restriction as the scalar
// selectSumUpdate.
NLGroupAggregateFoldFunction selectGroupSumFold(ValueType inputType) {
    switch (inputType) {
        case ValueType::Int64:
            return &groupFoldSum<types::Int64::Primitive>;
        break;

        case ValueType::UInt64:
            return &groupFoldSum<types::UInt64::Primitive>;
        break;

        case ValueType::Double:
            return &groupFoldSum<types::Double::Primitive>;
        break;

        case ValueType::Duration:
            return &groupFoldDurationTotal</*Distinct=*/false, /*Averages=*/false>;
        break;

        default:
            throw IRException("sum requires a numeric or a duration column");
        break;
    }

    return nullptr;
}

// The grouped sum(DISTINCT x) fold for a column of this value type: sum's domain,
// reduced over each group's distinct values.
NLGroupAggregateFoldFunction selectGroupSumDistinctFold(ValueType inputType) {
    switch (inputType) {
        case ValueType::Int64:
            return &groupFoldSumDistinct<types::Int64::Primitive>;
        break;

        case ValueType::UInt64:
            return &groupFoldSumDistinct<types::UInt64::Primitive>;
        break;

        case ValueType::Double:
            return &groupFoldSumDistinct<types::Double::Primitive>;
        break;

        case ValueType::Duration:
            return &groupFoldDurationTotal</*Distinct=*/true, /*Averages=*/false>;
        break;

        default:
            throw IRException("sum requires a numeric or a duration column");
        break;
    }

    return nullptr;
}

// The grouped avg fold for a column of this value type. avg widens each number to
// f64 and sums durations as durations, so - like sum - only a numeric or a duration
// column is valid.
NLGroupAggregateFoldFunction selectGroupAvgFold(ValueType inputType) {
    switch (inputType) {
        case ValueType::Int64:
            return &groupFoldAvg<types::Int64::Primitive>;
        break;

        case ValueType::UInt64:
            return &groupFoldAvg<types::UInt64::Primitive>;
        break;

        case ValueType::Double:
            return &groupFoldAvg<types::Double::Primitive>;
        break;

        case ValueType::Duration:
            return &groupFoldDurationTotal</*Distinct=*/false, /*Averages=*/true>;
        break;

        case ValueType::Point:
            return &groupFoldAvg<types::Point::Primitive>;
        break;

        default:
            throw IRException("avg requires a numeric, a duration or a point column");
        break;
    }

    return nullptr;
}

// The grouped avg(DISTINCT x) fold for a column of this value type: avg's domain,
// averaged over each group's distinct values.
NLGroupAggregateFoldFunction selectGroupAvgDistinctFold(ValueType inputType) {
    switch (inputType) {
        case ValueType::Int64:
            return &groupFoldAvgDistinct<types::Int64::Primitive>;
        break;

        case ValueType::UInt64:
            return &groupFoldAvgDistinct<types::UInt64::Primitive>;
        break;

        case ValueType::Double:
            return &groupFoldAvgDistinct<types::Double::Primitive>;
        break;

        case ValueType::Duration:
            return &groupFoldDurationTotal</*Distinct=*/true, /*Averages=*/true>;
        break;

        case ValueType::Point:
            return &groupFoldAvgDistinct<types::Point::Primitive>;
        break;

        default:
            throw IRException("avg requires a numeric, a duration or a point column");
        break;
    }

    return nullptr;
}

// The grouped min (IsMax false) / max (IsMax true) fold for a column of this value
// type. min/max order the values, so any orderable type is valid - numbers, bools,
// strings, lists and maps - but an embedding has no order, so it is rejected.
template <bool IsMax>
NLGroupAggregateFoldFunction selectGroupMinMaxFold(ValueType inputType) {
    switch (inputType) {
        case ValueType::Int64:
            return &groupFoldMinMax<types::Int64::Primitive, IsMax>;
        break;

        case ValueType::UInt64:
            return &groupFoldMinMax<types::UInt64::Primitive, IsMax>;
        break;

        case ValueType::Double:
            return &groupFoldMinMax<types::Double::Primitive, IsMax>;
        break;

        case ValueType::Bool:
            return &groupFoldMinMax<types::Bool::Primitive, IsMax>;
        break;

        case ValueType::String:
            return &groupFoldMinMax<types::String::Primitive, IsMax>;
        break;

        case ValueType::DateTime:
            return &groupFoldMinMax<types::DateTime::Primitive, IsMax>;
        break;

        case ValueType::Duration:
            return &groupFoldMinMax<types::Duration::Primitive, IsMax>;
        break;

        case ValueType::Point:
            return &groupFoldMinMax<types::Point::Primitive, IsMax>;
        break;

        case ValueType::List:
            return &groupFoldMinMax<types::List::Primitive, IsMax>;
        break;

        case ValueType::Map:
            return &groupFoldMinMax<types::Map::Primitive, IsMax>;
        break;

        default:
            throw IRException("min/max requires an orderable column");
        break;
    }

    return nullptr;
}

// The grouped first fold for a column of this value type. The kept value outlives its
// step as an extremum does, so first holds the types min/max hold.
NLGroupAggregateFoldFunction selectGroupFirstFold(ValueType inputType) {
    switch (inputType) {
        case ValueType::Int64:
            return &groupFoldFirst<types::Int64::Primitive>;
        break;

        case ValueType::UInt64:
            return &groupFoldFirst<types::UInt64::Primitive>;
        break;

        case ValueType::Double:
            return &groupFoldFirst<types::Double::Primitive>;
        break;

        case ValueType::Bool:
            return &groupFoldFirst<types::Bool::Primitive>;
        break;

        case ValueType::String:
            return &groupFoldFirst<types::String::Primitive>;
        break;

        case ValueType::DateTime:
            return &groupFoldFirst<types::DateTime::Primitive>;
        break;

        case ValueType::Duration:
            return &groupFoldFirst<types::Duration::Primitive>;
        break;

        case ValueType::Point:
            return &groupFoldFirst<types::Point::Primitive>;
        break;

        case ValueType::List:
            return &groupFoldFirst<types::List::Primitive>;
        break;

        case ValueType::Map:
            return &groupFoldFirst<types::Map::Primitive>;
        break;

        default:
            throw IRException("first requires a column of scalars, lists or maps");
        break;
    }

    return nullptr;
}

}

void NLExecutor::runGroupAggregateReset(NLExecutionContext* context, NLFunctionData* data) {
    const NLGroupAggregateResetData* reset = static_cast<NLGroupAggregateResetData*>(data);
    reset->getState()->reset();
}

void NLExecutor::runGroupAggregateUpdate(NLExecutionContext* context, NLFunctionData* data) {
    NLGroupAggregateUpdateData* update = static_cast<NLGroupAggregateUpdateData*>(data);
    NLGroupAggregateState* state = update->getState();

    NLGroupTable& groupTable = state->groupTable();
    const std::vector<NLGroupTable::KeyColumn>& keyColumns = groupTable.keyColumns();
    NLGroupAggregates& aggregates = state->aggregates();
    bioassert(!keyColumns.empty(), "nl.group_aggregate_update needs at least one grouping key");

    // Every column is row-aligned, so the first grouping key sizes this step's rows.
    const size_t rowCount = keyColumns.front()._input->size();

    // That alignment is this step's precondition, not a property of the op's types:
    // the group assignment is computed once per key row, then indexed by each fold as
    // it walks its own input, so a column of another length folds rows into the wrong
    // group - or, when it is the longer one, reads past the assignments entirely.
    // Generated IR places the update where every column is bound together, so a
    // mismatch here means malformed IR.
    for (const NLGroupTable::KeyColumn& keyColumn : keyColumns) {
        bioassert(keyColumn._input->size() == rowCount,
                  "nl.group_aggregate_update grouping keys must be row-aligned");
    }

    for (const NLGroupAggregates::Aggregate& aggregate : aggregates.getAggregates()) {
        bioassert(aggregate._input->size() == rowCount,
                  "nl.group_aggregate_update aggregate inputs must be row-aligned with the grouping keys");
    }

    if (rowCount == 0) {
        return;
    }

    groupTable.assign(rowCount);

    aggregates.grow(groupTable.getGroupCount());
    aggregates.fold(groupTable.getRowGroupColumn());
    aggregates.compactValues();
}

void NLExecutor::runGroupAggregateLoop(NLExecutionContext* context, NLFunctionData* data) {
    NLGroupAggregateLoopData* loopData = static_cast<NLGroupAggregateLoopData*>(data);
    NLGroupAggregateState* state = loopData->getState();

    NLGroupTable& groupTable = state->groupTable();
    const size_t totalGroups = groupTable.getGroupCount();
    const size_t chunkSize = context->getChunkSize();
    const NLStmtContainer* loopBody = loopData->getStmts();

    NLGroupAggregates& aggregates = state->aggregates();

    const NLLimitState* limit = loopData->getLimit();
    NLStepBuffers* const stepBuffers = loopData->getStepBuffers();

    // Re-chunk the accumulated groups: each step materializes the next chunk of
    // group rows - the key values sliced from the buffers and each aggregate
    // finalized from the per-group state - into the loop variables, then runs the
    // body (the nl.output) per chunk. An empty result (no group) runs the body zero
    // times, so a grouped aggregate over no row emits nothing.
    const auto runIteration = [&](size_t offset) {
        const size_t stepGroups = std::min(chunkSize, totalGroups - offset);

        groupTable.emitKeys(offset, stepGroups);

        stepBuffers->clear();
        aggregates.emit(offset, stepGroups, stepBuffers);

        runBody(context, loopBody);
    };

    if (limit) {
        for (size_t offset = 0; offset < totalGroups && limit->getRemaining() > 0; offset += chunkSize) {
            runIteration(offset);
        }
    } else {
        for (size_t offset = 0; offset < totalGroups; offset += chunkSize) {
            runIteration(offset);
        }
    }
}

NLGroupAggregateFoldFunction NLExecutor::selectGroupCountOptListFold() {
    return &groupFoldCountPresent<ListView>;
}

NLGroupAggregateFoldFunction NLExecutor::selectGroupCountDistinctOptListFold() {
    return &groupFoldCountDistinctPresent<ListView>;
}

NLGroupAggregateFoldFunction NLExecutor::selectGroupCountOptFold(NLChunkKind kind) {
    NLGroupAggregateFoldFunction fold = nullptr;
    dispatchOptChunkKind(kind, [&]<typename Element>() { fold = &groupFoldCountPresent<Element>; });

    return fold;
}

// The grouped sibling of selectIDCountFunction, for count(n) inside a GROUP BY.
NLGroupAggregateFoldFunction NLExecutor::selectGroupCountValidIDFold(NLChunkKind kind) {
    switch (kind) {
        case NLChunkKind::NodeID:
            return &groupFoldCountValidID<NodeID>;
        break;

        case NLChunkKind::EdgeID:
            return &groupFoldCountValidID<EdgeID>;
        break;

        default:
            return nullptr;
        break;
    }

    return nullptr;
}

NLGroupAggregateFoldFunction NLExecutor::selectGroupCountPresentPathFold() {
    return &groupFoldCountPresentPath;
}

NLGroupAggregateFoldFunction NLExecutor::selectTaggedGroupAggregateFold(GroupAggregateKind kind) {
    return taggedGroupAggregateFoldFor(kind);
}

NLGroupAggregateFoldFunction NLExecutor::selectGroupAggregateFold(GroupAggregateKind kind, ValueType inputType) {
    switch (kind) {
        case GroupAggregateKind::CountRows:
            return selectGroupCountAllFold();
        break;

        case GroupAggregateKind::CountDistinct:
            return selectGroupCountDistinctOptFold(chunkKindFromValueType(inputType));
        break;

        case GroupAggregateKind::Count:
            return selectGroupCountOptFold(chunkKindFromValueType(inputType));
        break;

        case GroupAggregateKind::Sum:
            return selectGroupSumFold(inputType);
        break;

        case GroupAggregateKind::SumDistinct:
            return selectGroupSumDistinctFold(inputType);
        break;

        case GroupAggregateKind::Avg:
            return selectGroupAvgFold(inputType);
        break;

        case GroupAggregateKind::AvgDistinct:
            return selectGroupAvgDistinctFold(inputType);
        break;

        case GroupAggregateKind::StDev:
        case GroupAggregateKind::StDevP:
            return selectGroupDeviationFold</*Distinct=*/false>(inputType);
        break;

        case GroupAggregateKind::StDevDistinct:
        case GroupAggregateKind::StDevPDistinct:
            return selectGroupDeviationFold</*Distinct=*/true>(inputType);
        break;

        case GroupAggregateKind::Min:
            return selectGroupMinMaxFold</*IsMax=*/false>(inputType);
        break;

        case GroupAggregateKind::Max:
            return selectGroupMinMaxFold</*IsMax=*/true>(inputType);
        break;

        case GroupAggregateKind::First:
            return selectGroupFirstFold(inputType);
        break;
    }

    bioassert(false, "Unhandled group aggregate kind");
    return nullptr;
}

NLGroupAggregateFoldFunction NLExecutor::selectGroupCountCellFold() {
    return &groupFoldCountPresentCell;
}

NLGroupAggregateFoldFunction NLExecutor::selectGroupCountDistinctCellFold() {
    return &groupFoldCountDistinctCell;
}

NLGroupAggregateFoldFunction NLExecutor::selectGroupCountAllFold() {
    // count(*) over an ID chunk (never null): every row charges its group.
    return &groupFoldCountAll;
}

NLGroupAggregateFoldFunction NLExecutor::selectGroupCountDistinctOptFold(NLChunkKind kind) {
    switch (kind) {
        case NLChunkKind::EdgeTypeID:
            return &groupFoldCountDistinctPresent<EdgeTypeID>;
        break;

        case NLChunkKind::LabelID:
            return &groupFoldCountDistinctPresent<LabelID>;
        break;

        case NLChunkKind::PropertyTypeID:
            return &groupFoldCountDistinctPresent<PropertyTypeID>;
        break;

        case NLChunkKind::ValueTypeCode:
            return &groupFoldCountDistinctPresent<ValueType>;
        break;

        case NLChunkKind::Int64:
            return &groupFoldCountDistinctPresent<types::Int64::Primitive>;
        break;

        case NLChunkKind::UInt64:
            return &groupFoldCountDistinctPresent<types::UInt64::Primitive>;
        break;

        case NLChunkKind::Double:
            return &groupFoldCountDistinctPresent<types::Double::Primitive>;
        break;

        case NLChunkKind::Bool:
            return &groupFoldCountDistinctPresent<types::Bool::Primitive>;
        break;

        case NLChunkKind::String:
            return &groupFoldCountDistinctPresent<types::String::Primitive>;
        break;

        case NLChunkKind::Embedding:
            return &groupFoldCountDistinctPresent<types::Embedding::Primitive>;
        break;

        case NLChunkKind::List:
            return &groupFoldCountDistinctPresent<ListView>;
        break;

        case NLChunkKind::Map:
            return &groupFoldCountDistinctPresent<MapView>;
        break;

        case NLChunkKind::DateTime:
            return &groupFoldCountDistinctPresent<types::DateTime::Primitive>;
        break;

        case NLChunkKind::Duration:
            return &groupFoldCountDistinctPresent<types::Duration::Primitive>;
        break;

        case NLChunkKind::Point:
            return &groupFoldCountDistinctPresent<types::Point::Primitive>;
        break;

        case NLChunkKind::NodeID:
        case NLChunkKind::EdgeID:
        case NLChunkKind::Path:
        case NLChunkKind::PathRef:
            throw IRException("A node, an edge or a path column holds its own null, so no nullable chunk wraps one");
        break;

        case NLChunkKind::LabelSetID:
            throw IRException("A label set column is read as rows, so no nullable chunk wraps one");
        break;
    }

    bioassert(false, "Unknown NLChunkKind");
    return nullptr;
}

NLGroupAggregateFoldFunction NLExecutor::selectGroupCountDistinctChunkFold(NLChunkKind kind) {
    NLGroupAggregateFoldFunction fold = nullptr;
    dispatchChunkKind(kind, [&]<typename ElementType>() { fold = &groupFoldCountDistinctChunk<ElementType>; });

    return fold;
}

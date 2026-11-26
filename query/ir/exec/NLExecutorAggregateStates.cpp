#include "NLExecutor.h"

#include <stdint.h>
#include <cmath>
#include <type_traits>

#include "TypeUtils.h"
#include "columns/ColumnOptVector.h"
#include "metadata/PropertyType.h"

#include "NLProgram.h"
#include "NLRuntimeProperty.h"
#include "NLAggregateStates.h"
#include "NLColumnKernels.h"

#include "IRException.h"
#include "BioAssert.h"

using namespace db;

namespace {

template <typename Select>
void dispatchAverageTotal(ValueType totalType, Select&& select) {
    switch (totalType) {
        case ValueType::Double:
            select.template operator()<types::Double::Primitive>();
        break;

        case ValueType::Duration:
            select.template operator()<types::Duration::Primitive>();
        break;

        case ValueType::Point:
            select.template operator()<types::Point::Primitive>();
        break;

        default:
            throw IRException("avg answers a double, a duration or a point");
        break;
    }
}

// Too few values to spread - none, or the one a sample has no second for - spread by 0
template <bool Population>
double deviation(double squaredDeviations, uint64_t count) {
    if constexpr (Population) {
        if (count == 0) {
            return 0.0;
        } else {
            return std::sqrt(squaredDeviations / static_cast<double>(count));
        }
    } else {
        if (count < 2) {
            return 0.0;
        } else {
            return std::sqrt(squaredDeviations / static_cast<double>(count - 1));
        }
    }
}

// Construct the identity of a state - a zero tally, sum or spread, no value kept yet - in the
// blocks of new groups
template <typename State>
void groupInitStates(NLGroupStatePlaces places, size_t begin, size_t count) {
    for (size_t group = begin; group < begin + count; group++) {
        new (places.getPlace(group)) State {};
    }
}

// The state type of an aggregate each group keeps a State for, finalized by emit
template <typename State>
void describeGroupState(NLGroupAggregateEmitFunction emit, NLGroupAggregateStateType& stateType) {
    static_assert(std::is_trivially_copyable_v<State>, "The blocks move a state as bytes and never destroy it");
    static_assert(alignof(State) <= __STDCPP_DEFAULT_NEW_ALIGNMENT__, "The blocks are as aligned as operator new makes them");

    stateType._size = sizeof(State);
    stateType._alignment = alignof(State);
    stateType._init = &groupInitStates<State>;
    stateType._emit = emit;
    stateType._reown = nullptr;
}

template <typename Primitive>
void groupReownKeptValues(NLGroupStatePlaces places, size_t groupCount, NLValueStore& values) {
    for (size_t group = 0; group < groupCount; group++) {
        places.getState<NLKeptValue<Primitive>>(group).reown(values);
    }
}

// Emit the value a group keeps, null for a group that kept none
template <typename Primitive>
void groupEmitKeptValue(NLGroupStatePlaces places, size_t begin, size_t count, Column* output, NLStepBuffers* stepBuffers);

// The state type of a min, max or first
template <typename Primitive>
void describeGroupKeptValueState(NLGroupAggregateStateType& stateType) {
    describeGroupState<NLKeptValue<Primitive>>(&groupEmitKeptValue<Primitive>, stateType);

    if constexpr (NLViewElement<Primitive>) {
        stateType._reown = &groupReownKeptValues<Primitive>;
    }
}

// Finalize the states of groups [begin, begin + count) into the rows of an output column,
// one row per group
template <typename State, typename OutputColumn, typename Finalize>
void finalizeGroupStates(NLGroupStatePlaces places, size_t begin, size_t count, Column* output, Finalize finalize) {
    OutputColumn* typedOutput = static_cast<OutputColumn*>(output);
    resetForRowWrites(typedOutput, count);

    for (size_t index = 0; index < count; index++) {
        const typename OutputColumn::ValueType value = finalize(places.getState<State>(begin + index));
        writeRow(typedOutput, index, value);
    }
}

// Emit what a state holds as it is: a count's tally or a sum's present total
template <typename State, typename OutputColumn>
void groupEmitCopy(NLGroupStatePlaces places, size_t begin, size_t count, Column* output, NLStepBuffers* stepBuffers) {
    finalizeGroupStates<State, OutputColumn>(places, begin, count, output, [](const State& state) {
        return state;
    });
}

template <typename Primitive>
using KeptValueColumn = std::conditional_t<std::is_same_v<Primitive, CellView>,
                                           ColumnVector<CellView>,
                                           ColumnOptVector<Primitive>>;

template <typename Primitive>
void groupEmitKeptValue(NLGroupStatePlaces places, size_t begin, size_t count, Column* output, NLStepBuffers* stepBuffers) {
    using State = NLKeptValue<Primitive>;
    using Element = TypeUtils::nullable_t<Primitive>;

    finalizeGroupStates<State, KeptValueColumn<Primitive>>(places, begin, count, output, [](const State& kept) {
        if (!kept.isKept()) {
            return TypeUtils::nullValue<Element>();
        }

        return Element(kept.getValue());
    });
}

// Emit an average: the total divided by the count, a double over numbers and a duration over
// durations. A group with no value averages to null, matching Cypher.
template <typename Total>
void groupEmitAverage(NLGroupStatePlaces places, size_t begin, size_t count, Column* output, NLStepBuffers* stepBuffers) {
    using Average = decltype(averageOf(Total {}, uint64_t {0}));
    using State = NLAverageState<Total>;

    finalizeGroupStates<State, ColumnOptVector<Average>>(places, begin, count, output, [](const State& average) {
        if (average._count == 0) {
            return std::optional<Average>();
        } else {
            return std::optional<Average>(averageOf(average._total, average._count));
        }
    });
}

void groupEmitDurationSum(NLGroupStatePlaces places, size_t begin, size_t count, Column* output, NLStepBuffers* stepBuffers) {
    using OutputColumn = ColumnOptVector<types::Duration::Primitive>;

    finalizeGroupStates<__int128, OutputColumn>(places, begin, count, output, [](__int128 total) {
        return std::optional<types::Duration::Primitive>(durationOfTotal(total));
    });
}

template <bool Population>
void groupEmitDeviation(NLGroupStatePlaces places, size_t begin, size_t count, Column* output, NLStepBuffers* stepBuffers) {
    finalizeGroupStates<NLDeviationState, ColumnOptVector<double>>(places, begin, count, output, [](const NLDeviationState& state) {
        return std::optional<double>(deviation<Population>(state._squaredDeviations, state._count));
    });
}

void groupEmitTaggedSum(NLGroupStatePlaces places, size_t begin, size_t count, Column* output, NLStepBuffers* stepBuffers) {
    QueryListBuffer& cells = stepBuffers->listBuffer();

    finalizeGroupStates<NLTaggedSum, ColumnVector<CellView>>(places, begin, count, output, [&](const NLTaggedSum& sum) {
        return sum.sum(cells);
    });
}

void groupEmitTaggedAverage(NLGroupStatePlaces places, size_t begin, size_t count, Column* output, NLStepBuffers* stepBuffers) {
    using State = NLAverageState<NLTaggedSum>;

    QueryListBuffer& cells = stepBuffers->listBuffer();

    finalizeGroupStates<State, ColumnVector<CellView>>(places, begin, count, output, [&](const State& average) {
        return average._total.average(average._count, cells);
    });
}

template <typename Primitive>
void describeGroupNumberSumState(NLGroupAggregateStateType& stateType) {
    describeGroupState<Primitive>(&groupEmitCopy<Primitive, ColumnOptVector<Primitive>>, stateType);
}

// The state of a grouped sum over a column of this value type: the running sum in the
// column's own type, or the 128-bit total of microseconds of durations
void describeGroupSumState(ValueType accumulatorType, NLGroupAggregateStateType& stateType) {
    switch (accumulatorType) {
        case ValueType::Int64:
            describeGroupNumberSumState<types::Int64::Primitive>(stateType);
        break;

        case ValueType::UInt64:
            describeGroupNumberSumState<types::UInt64::Primitive>(stateType);
        break;

        case ValueType::Double:
            describeGroupNumberSumState<types::Double::Primitive>(stateType);
        break;

        case ValueType::Duration:
            describeGroupState<__int128>(&groupEmitDurationSum, stateType);
        break;

        default:
            throw IRException("sum requires a numeric or a duration column");
        break;
    }
}

}

void NLExecutor::selectGroupAggregateState(GroupAggregateKind kind,
                                           ValueType accumulatorType,
                                           NLGroupAggregateStateType& stateType) {
    // count keeps only a tally, so it ignores the accumulator type; avg carries a running
    // total - an f64 over numbers, a 128-bit total over durations - and its count. A switch
    // (not an if/else) over every kind so a new one is a compile error here rather than
    // silently taking the sum/min/max path.
    switch (kind) {
        case GroupAggregateKind::Count:
        case GroupAggregateKind::CountDistinct:
        case GroupAggregateKind::CountRows:
            describeGroupState<uint64_t>(&groupEmitCopy<uint64_t, ColumnVector<uint64_t>>, stateType);
        break;

        case GroupAggregateKind::Avg:
        case GroupAggregateKind::AvgDistinct:
            dispatchAverageTotal(accumulatorType, [&]<typename Total>() {
                if constexpr (std::is_same_v<Total, types::Duration::Primitive>) {
                    describeGroupState<NLAverageState<__int128>>(&groupEmitAverage<__int128>, stateType);
                } else {
                    describeGroupState<NLAverageState<Total>>(&groupEmitAverage<Total>, stateType);
                }
            });
        break;

        case GroupAggregateKind::StDev:
        case GroupAggregateKind::StDevDistinct:
            describeGroupState<NLDeviationState>(&groupEmitDeviation</*Population=*/false>, stateType);
        break;

        case GroupAggregateKind::StDevP:
        case GroupAggregateKind::StDevPDistinct:
            describeGroupState<NLDeviationState>(&groupEmitDeviation</*Population=*/true>, stateType);
        break;

        case GroupAggregateKind::Sum:
        case GroupAggregateKind::SumDistinct:
            describeGroupSumState(accumulatorType, stateType);
        break;

        case GroupAggregateKind::Min:
        case GroupAggregateKind::Max:
        case GroupAggregateKind::First: {
            // min/max/first keep the value itself, null until a group sees one. The kept
            // state compiles for any value type, and lowering has already validated the kind
            // / type pairing (and the fold selector re-checks it), so one dispatch over the
            // accumulator type suffices - the grouped sibling of selectAggregateReset.
            const auto select = [&]<SupportedType T>() {
                describeGroupKeptValueState<typename T::Primitive>(stateType);
            };
            ValueTypeDispatcher(accumulatorType).execute(select);
        }
        break;
    }
}

void NLExecutor::selectTaggedGroupAggregateState(GroupAggregateKind kind, NLGroupAggregateStateType& stateType) {
    switch (kind) {
        case GroupAggregateKind::Sum:
        case GroupAggregateKind::SumDistinct:
            describeGroupState<NLTaggedSum>(&groupEmitTaggedSum, stateType);
        break;

        case GroupAggregateKind::Avg:
        case GroupAggregateKind::AvgDistinct:
            describeGroupState<NLAverageState<NLTaggedSum>>(&groupEmitTaggedAverage, stateType);
        break;

        case GroupAggregateKind::Min:
        case GroupAggregateKind::Max:
        case GroupAggregateKind::First:
            describeGroupKeptValueState<CellView>(stateType);
        break;

        case GroupAggregateKind::Count:
        case GroupAggregateKind::CountDistinct:
        case GroupAggregateKind::CountRows:
        case GroupAggregateKind::StDev:
        case GroupAggregateKind::StDevP:
        case GroupAggregateKind::StDevDistinct:
        case GroupAggregateKind::StDevPDistinct:
            bioassert(false, "A tally or a spread of tagged cells keeps no cell");
        break;
    }
}

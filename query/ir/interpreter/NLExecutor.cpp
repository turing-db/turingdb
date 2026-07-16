#include "NLExecutor.h"

#include <algorithm>
#include <cmath>
#include <limits>
#include <string>
#include <string_view>
#include <type_traits>

#include "iterators/GetInEdgesIterator.h"
#include "iterators/GetInEdgesByTypeIterator.h"
#include "iterators/GetOutEdgesIterator.h"
#include "iterators/GetOutEdgesByTypeIterator.h"
#include "iterators/GetPropertiesWithNullIterator.h"
#include "iterators/ScanEdgesIterator.h"
#include "iterators/ScanNodesIterator.h"
#include "iterators/ScanNodesByLabelIterator.h"
#include "columns/ColumnOptVector.h"
#include "columns/ColumnMask.h"
#include "columns/ColumnConst.h"
#include "columns/ColumnCombinations.h"
#include "columns/ColumnOperator.h"
#include "columns/ColumnOperators.h"
#include "columns/ColumnOperatorDispatcher.h"
#include "columns/AllowedKinds.h"
#include "columns/UnaryPredicates.h"
#include "metadata/PropertyType.h"

#include "NLProgram.h"
#include "NLOutputSink.h"

#include "LocalMemory.h"
#include "IRException.h"
#include "BioAssert.h"

using namespace db;

namespace {

template <ColumnOperator Op>
struct BinaryOpTraits;

template <>
struct BinaryOpTraits<OP_ADD> {
    using Functor = Add;

    template <typename ResCol, typename LhsCol, typename RhsCol>
    static void exec(ResCol* result, const LhsCol* lhs, const RhsCol* rhs) {
        BinaryOperators::exec<Functor>(result, lhs, rhs);
    }
};

template <>
struct BinaryOpTraits<OP_SUB> {
    using Functor = Sub;

    template <typename ResCol, typename LhsCol, typename RhsCol>
    static void exec(ResCol* result, const LhsCol* lhs, const RhsCol* rhs) {
        BinaryOperators::exec<Functor>(result, lhs, rhs);
    }
};

template <>
struct BinaryOpTraits<OP_MUL> {
    using Functor = Mul;

    template <typename ResCol, typename LhsCol, typename RhsCol>
    static void exec(ResCol* result, const LhsCol* lhs, const RhsCol* rhs) {
        BinaryOperators::exec<Functor>(result, lhs, rhs);
    }
};

template <>
struct BinaryOpTraits<OP_EQUAL> {
    using Functor = Eq;

    template <typename ResCol, typename LhsCol, typename RhsCol>
    static void exec(ResCol* result, const LhsCol* lhs, const RhsCol* rhs) {
        BinaryPredicates::exec<Functor>(result, lhs, rhs);
    }
};

template <>
struct BinaryOpTraits<OP_DIV> {
    using Functor = Div;

    template <typename ResCol, typename LhsCol, typename RhsCol>
    static void exec(ResCol* result, const LhsCol* lhs, const RhsCol* rhs) {
        BinaryOperators::exec<Functor>(result, lhs, rhs);
    }
};

template <>
struct BinaryOpTraits<OP_GREATER_THAN> {
    using Functor = Gt;

    template <typename ResCol, typename LhsCol, typename RhsCol>
    static void exec(ResCol* result, const LhsCol* lhs, const RhsCol* rhs) {
        BinaryPredicates::exec<Functor>(result, lhs, rhs);
    }
};

template <>
struct BinaryOpTraits<OP_LESS_THAN> {
    using Functor = Lt;

    template <typename ResCol, typename LhsCol, typename RhsCol>
    static void exec(ResCol* result, const LhsCol* lhs, const RhsCol* rhs) {
        BinaryPredicates::exec<Functor>(result, lhs, rhs);
    }
};

template <>
struct BinaryOpTraits<OP_GREATER_THAN_OR_EQUAL> {
    using Functor = Gte;

    template <typename ResCol, typename LhsCol, typename RhsCol>
    static void exec(ResCol* result, const LhsCol* lhs, const RhsCol* rhs) {
        BinaryPredicates::exec<Functor>(result, lhs, rhs);
    }
};

template <>
struct BinaryOpTraits<OP_LESS_THAN_OR_EQUAL> {
    using Functor = Lte;

    template <typename ResCol, typename LhsCol, typename RhsCol>
    static void exec(ResCol* result, const LhsCol* lhs, const RhsCol* rhs) {
        BinaryPredicates::exec<Functor>(result, lhs, rhs);
    }
};

template <>
struct BinaryOpTraits<OP_AND> {
    using Functor = And;

    template <typename ResCol, typename LhsCol, typename RhsCol>
    static void exec(ResCol* result, const LhsCol* lhs, const RhsCol* rhs) {
        BinaryPredicates::exec<Functor>(result, lhs, rhs);
    }
};

template <>
struct BinaryOpTraits<OP_OR> {
    using Functor = Or;

    template <typename ResCol, typename LhsCol, typename RhsCol>
    static void exec(ResCol* result, const LhsCol* lhs, const RhsCol* rhs) {
        BinaryPredicates::exec<Functor>(result, lhs, rhs);
    }
};

template <ColumnOperator Op, typename ResCol, typename LhsCol, typename RhsCol>
void applyBinaryOp(Column* result, const Column* lhs, const Column* rhs) {
    BinaryOpTraits<Op>::exec(static_cast<ResCol*>(result),
                             static_cast<const LhsCol*>(lhs),
                             static_cast<const RhsCol*>(rhs));
}

template <ColumnOperator Op>
struct BinaryOpSelector {
    LocalMemory* _memory {nullptr};
    Column* _result {nullptr};
    NLBinaryFn _fn {nullptr};

    template <typename LhsCol, typename RhsCol>
    void operator()(const LhsCol*, const RhsCol*) {
        using OpType = typename BinaryOpTraits<Op>::Functor;
        using ResCol = ColumnCombination<OpType, LhsCol, RhsCol>;
        using ResColType = ResCol::ResultColumnType;

        _result = _memory->alloc<ResColType>();
        _fn = &applyBinaryOp<Op, ResColType, LhsCol, RhsCol>;
    }
};

// Execute a body of statements
void runBody(NLExecutionContext* context, const NLStmtContainer* body) {
    for (const NLFunctionDescriptor& descriptor : body->stmts()) {
        const auto func = descriptor.getFunction();
        NLFunctionData* funcData = descriptor.getData();
        func(context, funcData);
    }
}

// Gather rows of a carried column by applying indices
template <typename ElementType>
void gatherColumn(const Column* input,
                  const ColumnVector<size_t>* indices,
                  Column* output) {
    const ColumnVector<ElementType>* typedInput = static_cast<const ColumnVector<ElementType>*>(input);
    ColumnVector<ElementType>* typedOutput = static_cast<ColumnVector<ElementType>*>(output);

    typedOutput->clear();
    typedOutput->reserve(indices->size());
    const auto& indicesRaw = indices->getRaw();
    auto& typedInputRaw = typedInput->getRaw();

    for (const size_t index : indicesRaw) {
        typedOutput->push_back(typedInputRaw[index]);
    }
}

void collectMaskSurvivors(const Column* mask, ColumnVector<size_t>* indices) {
    const ColumnMask* typedMask = static_cast<const ColumnMask*>(mask);
    const std::vector<ColumnMask::Bool_t>& maskRaw = typedMask->getRaw();
    std::vector<size_t>& survivingRaw = indices->getRaw();

    for (size_t row = 0; row < maskRaw.size(); row++) {
        if (maskRaw[row]) {
            survivingRaw.push_back(row);
        }
    }
}

void collectOptMaskSurvivors(const Column* mask, ColumnVector<size_t>* indices) {
    const ColumnOptMask* typedMask = static_cast<const ColumnOptMask*>(mask);
    const std::vector<std::optional<CustomBool>>& maskRaw = typedMask->getRaw();
    std::vector<size_t>& survivingRaw = indices->getRaw();

    for (size_t row = 0; row < maskRaw.size(); row++) {
        if (maskRaw[row].has_value() && maskRaw[row].value()) {
            survivingRaw.push_back(row);
        }
    }
}

bool isMaskConstant(const Column* mask) {
    return mask->getContainerKind() == ContainerKind::code<ColumnConst<CustomBool>>();
}

void applyNotOnMask(Column* result, const Column* operand) {
    UnaryPredicateExecutor<Not, ColumnMask::Bool_t>::apply(static_cast<ColumnMask*>(result),
                                                          static_cast<const ColumnMask*>(operand));
}

void applyNotOnOptMask(Column* result, const Column* operand) {
    using OptBool = std::optional<CustomBool>;
    UnaryPredicateExecutor<Not, OptBool>::apply(static_cast<ColumnOptMask*>(result),
                                               static_cast<const ColumnOptMask*>(operand));
}

void applyNotOnConst(Column* result, const Column* operand) {
    const ColumnConst<CustomBool>* typedOperand = static_cast<const ColumnConst<CustomBool>*>(operand);
    ColumnConst<CustomBool>* typedResult = static_cast<ColumnConst<CustomBool>*>(result);
    UnaryPredicateExecutor<Not, CustomBool>::apply(typedResult, typedOperand);
}

// Block-repeat: each input row is emitted `factor` times in a row, so input row
// i lands at output indices [i*factor, (i+1)*factor). This lays out an outer
// column of a cross product, where each outer row pairs with the whole inner
// chunk. The fill stops at `outputRowCount` rows (min(N*factor, remaining) under
// a limit), so the last block may be partial and later input rows are skipped.
template <typename ElementType>
void blockRepeatColumn(const Column* input, size_t factor, size_t outputRowCount, Column* output) {
    const ColumnVector<ElementType>* typedInput = static_cast<const ColumnVector<ElementType>*>(input);
    ColumnVector<ElementType>* typedOutput = static_cast<ColumnVector<ElementType>*>(output);

    const auto& inputRaw = typedInput->getRaw();
    auto& outputRaw = typedOutput->getRaw();
    outputRaw.resize(outputRowCount);

    auto outputIt = outputRaw.begin();
    size_t rowsLeft = outputRowCount;
    for (const ElementType& value : inputRaw) {
        if (rowsLeft == 0) {
            break;
        }

        const size_t count = std::min(factor, rowsLeft);
        std::fill_n(outputIt, count, value);
        outputIt += count;
        rowsLeft -= count;
    }
}

// Tile: the whole input chunk is emitted back to back, so input row j lands at
// output indices j, j+M, j+2M, ... This lays out an inner column of a cross
// product, where the inner chunk repeats once per outer row. The fill stops at
// `outputRowCount` rows (min(M*N, remaining) under a limit), which alone bounds
// the repeats, so the row count drives it rather than the `factor` (N) the outer
// side uses. The last tile may be partial.
template <typename ElementType>
void tileColumn(const Column* input, size_t factor, size_t outputRowCount, Column* output) {
    const ColumnVector<ElementType>* typedInput = static_cast<const ColumnVector<ElementType>*>(input);
    ColumnVector<ElementType>* typedOutput = static_cast<ColumnVector<ElementType>*>(output);

    const auto& inputRaw = typedInput->getRaw();
    auto& outputRaw = typedOutput->getRaw();
    outputRaw.resize(outputRowCount);

    const size_t tileLength = inputRaw.size();
    auto outputIt = outputRaw.begin();
    size_t rowsLeft = outputRowCount;
    while (rowsLeft > 0) {
        const size_t count = std::min(tileLength, rowsLeft);
        outputIt = std::copy(inputRaw.begin(), inputRaw.begin() + count, outputIt);
        rowsLeft -= count;
    }
}

// Range copy: rows [inputOffset, inputOffset + rowCount) of the input land at
// output indices [0, rowCount). This lifts a skip's surviving suffix to the front
// of a fresh chunk - nl.skip_truncate passes inputOffset = skipThisStep and
// rowCount = emitThisStep. std::copy of a contiguous range is lowered to memcpy.
template <typename ElementType>
void copyRangeColumn(const Column* input, size_t inputOffset, size_t rowCount, Column* output) {
    const ColumnVector<ElementType>* typedInput = static_cast<const ColumnVector<ElementType>*>(input);
    ColumnVector<ElementType>* typedOutput = static_cast<ColumnVector<ElementType>*>(output);

    const auto& inputRaw = typedInput->getRaw();
    auto& outputRaw = typedOutput->getRaw();
    outputRaw.resize(rowCount);

    const auto first = inputRaw.begin() + inputOffset;
    std::copy(first, first + rowCount, outputRaw.begin());
}

// Append every row of an input chunk onto the tail of a growing buffer of the
// same element type. nl.sort_collect calls this once per producing-loop step, so
// the buffer accumulates every row across all chunks, row-aligned with the other
// buffers of the same accumulator.
template <typename ElementType>
void appendColumn(const Column* input, Column* buffer) {
    const ColumnVector<ElementType>* typedInput = static_cast<const ColumnVector<ElementType>*>(input);
    ColumnVector<ElementType>* typedBuffer = static_cast<ColumnVector<ElementType>*>(buffer);

    const auto& inputRaw = typedInput->getRaw();
    auto& bufferRaw = typedBuffer->getRaw();

    bufferRaw.insert(bufferRaw.end(), inputRaw.begin(), inputRaw.end());
}

// 3-way compare two rows of a non-null orderable column (an ID column): negative
// if row a sorts before row b, positive if after, zero if they are equal.
template <typename ElementType>
int compareColumn(const Column* column, size_t a, size_t b) {
    const auto& raw = static_cast<const ColumnVector<ElementType>*>(column)->getRaw();
    const ElementType& valueA = raw[a];
    const ElementType& valueB = raw[b];

    if (valueA < valueB) {
        return -1;
    } else if (valueB < valueA) {
        return 1;
    }

    return 0;
}

// 3-way compare two rows of a nullable value column. A null sorts after every
// value, so an ascending order places nulls last (matching Cypher's ORDER BY),
// and two nulls tie; non-null values compare by their natural order.
template <typename Primitive>
int compareOptColumn(const Column* column, size_t a, size_t b) {
    const auto& raw = static_cast<const ColumnVector<std::optional<Primitive>>*>(column)->getRaw();
    const std::optional<Primitive>& valueA = raw[a];
    const std::optional<Primitive>& valueB = raw[b];

    const bool aNull = !valueA.has_value();
    const bool bNull = !valueB.has_value();
    if (aNull || bNull) {
        if (aNull && bNull) {
            return 0;
        }

        return aNull ? 1 : -1;
    }

    if (*valueA < *valueB) {
        return -1;
    } else if (*valueB < *valueA) {
        return 1;
    }

    return 0;
}

// Append the raw bytes of a present property value to a distinct row key. The key
// is a std::string used purely as a growable byte buffer - not text - so the value
// is appended verbatim: a trivially-copyable primitive copies its object bytes; a
// string copies a length prefix then its characters, so two rows never collide by
// concatenation (so "a"+"b" and "ab"+"" get distinct keys).
template <typename Primitive>
void distinctAppendValueBytes(std::string& key, const Primitive& value) {
    key.append(reinterpret_cast<const char*>(&value), sizeof(Primitive));
}

// A double is normalized before its bytes are appended so keys compare by Cypher
// value equality, not bit pattern: -0.0 collapses to +0.0 (0.0 == -0.0 in Cypher)
// and every NaN payload maps to one canonical NaN, so two distinct bit patterns of
// the same value never split into separate groups (or survive DISTINCT as two rows).
void distinctAppendValueBytes(std::string& key, double value) {
    double normalized = value;

    if (normalized == 0.0) {
        // -0.0 == +0.0, so assigning +0.0 canonicalizes the sign of zero.
        normalized = 0.0;
    } else if (std::isnan(normalized)) {
        normalized = std::numeric_limits<double>::quiet_NaN();
    }

    key.append(reinterpret_cast<const char*>(&normalized), sizeof(normalized));
}

void distinctAppendValueBytes(std::string& key, std::string_view value) {
    const size_t length = value.size();
    key.append(reinterpret_cast<const char*>(&length), sizeof(length));
    key.append(value.data(), value.size());
}

// Serialize one row of an ID column (node/edge/edge-type IDs) into the row key -
// a std::string used as a byte buffer, not text - as the ID's underlying integer
// value, byte for byte.
template <typename ElementType>
void distinctKeyAppendColumn(const Column* column, size_t row, std::string& key) {
    const auto& raw = static_cast<const ColumnVector<ElementType>*>(column)->getRaw();
    const auto value = raw[row].getValue();
    distinctAppendValueBytes(key, value);
}

// Serialize one row of a nullable value column into the row key - a std::string
// used as a byte buffer, not text - as a tag byte telling null from present, then,
// when present, the value's bytes. A null serializes to the tag alone, so all
// nulls share a key and DISTINCT dedups them together, matching Cypher.
template <typename Primitive>
void distinctKeyAppendOptColumn(const Column* column, size_t row, std::string& key) {
    const auto& raw = static_cast<const ColumnVector<std::optional<Primitive>>*>(column)->getRaw();
    const std::optional<Primitive>& value = raw[row];

    if (!value.has_value()) {
        key.push_back('\0');
        return;
    }

    key.push_back('\1');
    distinctAppendValueBytes(key, *value);
}

// Count the present (non-null) values of a nullable value column - a
// ColumnVector<std::optional<Primitive>> - so Cypher count(x) charges only the
// rows in which x is not null.
template <typename OptType>
size_t countPresentColumn(const Column* column) {
    const auto& raw = static_cast<const ColumnVector<OptType>*>(column)->getRaw();
    return std::count_if(raw.begin(), raw.end(), [](const OptType& value) {
        return value.has_value();
    });
}

// Reset a sum/avg accumulator to a present zero (its identity): a running sum
// starts at zero and stays present regardless of nulls, so an empty input sums
// to zero, matching Cypher.
template <typename Primitive>
void aggregateResetZero(NLAggregateState* state) {
    auto* accumulator = static_cast<ColumnOptVector<Primitive>*>(state->getAccumulator());
    accumulator->getRaw().assign(1, std::optional<Primitive>(Primitive {}));
    state->setCount(0);
}

// Reset a min/max accumulator to null: with no non-null row seen yet there is no
// extreme, and an all-null (or empty) input reduces to null, matching Cypher.
template <typename Primitive>
void aggregateResetNull(NLAggregateState* state) {
    auto* accumulator = static_cast<ColumnOptVector<Primitive>*>(state->getAccumulator());
    accumulator->getRaw().assign(1, std::nullopt);
    state->setCount(0);
}

// Add two aggregate values with defined overflow. A signed integer sum wraps in
// two's complement (matching Cypher's Java-long integer sum) rather than invoking
// C++ signed-overflow undefined behavior; unsigned integers already wrap by the
// language, and doubles use the built-in +.
template <typename Primitive>
Primitive numericAdd(Primitive accumulator, Primitive value) {
    if constexpr (std::is_integral_v<Primitive> && std::is_signed_v<Primitive>) {
        using Unsigned = std::make_unsigned_t<Primitive>;
        return static_cast<Primitive>(static_cast<Unsigned>(accumulator) + static_cast<Unsigned>(value));
    } else {
        return accumulator + value;
    }
}

// Fold a chunk's present values into a sum accumulator (same primitive as the
// input). The accumulator is always present, so read-modify-write its one row.
template <typename Primitive>
void aggregateUpdateSum(NLAggregateState* state, const Column* input) {
    auto* accumulator = static_cast<ColumnOptVector<Primitive>*>(state->getAccumulator());
    std::optional<Primitive>& current = accumulator->getRaw().front();
    const auto& inputRaw = static_cast<const ColumnOptVector<Primitive>*>(input)->getRaw();

    Primitive running = current.value();
    for (const std::optional<Primitive>& value : inputRaw) {
        if (value.has_value()) {
            running = numericAdd(running, *value);
        }
    }

    current = running;
}

// Fold a chunk's present values into a min (IsMax false) or max (IsMax true)
// accumulator. The first present value seeds the accumulator; later values
// replace it when more extreme. Nulls are skipped, so an all-null input leaves
// the accumulator null.
template <typename Primitive, bool IsMax>
void aggregateUpdateMinMax(NLAggregateState* state, const Column* input) {
    auto* accumulator = static_cast<ColumnOptVector<Primitive>*>(state->getAccumulator());
    std::optional<Primitive>& current = accumulator->getRaw().front();
    const auto& inputRaw = static_cast<const ColumnOptVector<Primitive>*>(input)->getRaw();

    for (const std::optional<Primitive>& value : inputRaw) {
        if (!value.has_value()) {
            continue;
        }

        if (!current.has_value()) {
            current = *value;
        } else if constexpr (IsMax) {
            if (*current < *value) {
                current = *value;
            }
        } else {
            if (*value < *current) {
                current = *value;
            }
        }
    }
}

// Fold a chunk's present values into an avg accumulator: a running f64 sum plus a
// count of the non-null rows (the input primitive is widened to f64). avg divides
// the two at the emit step, so both are accumulated here.
template <typename Primitive>
void aggregateUpdateAvg(NLAggregateState* state, const Column* input) {
    auto* accumulator = static_cast<ColumnOptVector<double>*>(state->getAccumulator());
    std::optional<double>& current = accumulator->getRaw().front();
    const auto& inputRaw = static_cast<const ColumnOptVector<Primitive>*>(input)->getRaw();

    double running = current.value();
    size_t seen = 0;
    for (const std::optional<Primitive>& value : inputRaw) {
        if (value.has_value()) {
            running += static_cast<double>(*value);
            seen++;
        }
    }

    current = running;
    state->addCount(seen);
}

// Emit a sum/min/max result: the accumulator already holds the reduced value in
// the result's own type, so copy its single row into the output (a present sum,
// or the extreme / null for min/max).
template <typename Primitive>
void aggregateResultCopy(const NLAggregateState* state, Column* output) {
    const auto* accumulator = static_cast<const ColumnOptVector<Primitive>*>(state->getAccumulator());
    auto* typedOutput = static_cast<ColumnOptVector<Primitive>*>(output);
    typedOutput->getRaw().assign(1, accumulator->getRaw().front());
}

// Emit an avg result: divide the running f64 sum by the non-null count. With no
// non-null row the average is null (division is undefined), matching Cypher.
void aggregateResultAvg(const NLAggregateState* state, Column* output) {
    const auto* accumulator = static_cast<const ColumnOptVector<double>*>(state->getAccumulator());
    auto* typedOutput = static_cast<ColumnOptVector<double>*>(output);
    std::vector<std::optional<double>>& outputRaw = typedOutput->getRaw();
    const size_t count = state->getCount();

    if (count == 0) {
        outputRaw.assign(1, std::nullopt);
    } else {
        const double average = accumulator->getRaw().front().value() / static_cast<double>(count);
        outputRaw.assign(1, std::optional<double>(average));
    }
}

// The fold handler for a sum over a column of this value type. sum adds the
// values, so only a numeric column is valid - a string or bool sum is rejected
// (which also keeps aggregateUpdateSum from being instantiated for a type whose
// operator+= would not compile).
NLAggregateUpdateFunction selectSumUpdate(ValueType inputType) {
    switch (inputType) {
        case ValueType::Int64:
            return &aggregateUpdateSum<types::Int64::Primitive>;
        break;

        case ValueType::UInt64:
            return &aggregateUpdateSum<types::UInt64::Primitive>;
        break;

        case ValueType::Double:
            return &aggregateUpdateSum<types::Double::Primitive>;
        break;

        default:
            throw IRException("sum requires a numeric column");
        break;
    }

    return nullptr;
}

// The fold handler for an avg over a column of this value type. avg widens each
// value to f64, so - like sum - only a numeric column is valid.
NLAggregateUpdateFunction selectAvgUpdate(ValueType inputType) {
    switch (inputType) {
        case ValueType::Int64:
            return &aggregateUpdateAvg<types::Int64::Primitive>;
        break;

        case ValueType::UInt64:
            return &aggregateUpdateAvg<types::UInt64::Primitive>;
        break;

        case ValueType::Double:
            return &aggregateUpdateAvg<types::Double::Primitive>;
        break;

        default:
            throw IRException("avg requires a numeric column");
        break;
    }

    return nullptr;
}

// The fold handler for a min (IsMax false) or max (IsMax true) over a column of
// this value type. min/max order the values, so any orderable type is valid -
// numbers, bools and strings - but an embedding has no order (its < would not
// compile), so it is rejected.
template <bool IsMax>
NLAggregateUpdateFunction selectMinMaxUpdate(ValueType inputType) {
    switch (inputType) {
        case ValueType::Int64:
            return &aggregateUpdateMinMax<types::Int64::Primitive, IsMax>;
        break;

        case ValueType::UInt64:
            return &aggregateUpdateMinMax<types::UInt64::Primitive, IsMax>;
        break;

        case ValueType::Double:
            return &aggregateUpdateMinMax<types::Double::Primitive, IsMax>;
        break;

        case ValueType::Bool:
            return &aggregateUpdateMinMax<types::Bool::Primitive, IsMax>;
        break;

        case ValueType::String:
            return &aggregateUpdateMinMax<types::String::Primitive, IsMax>;
        break;

        default:
            throw IRException("min/max requires an orderable column");
        break;
    }

    return nullptr;
}

// Append the values of the given rows of an input chunk onto the tail of a key
// buffer of the same element type. nl.group_aggregate_update passes the rows that
// created a new group this step, so the buffer grows one key value per group in
// creation order.
template <typename ElementType>
void groupGatherAppendColumn(const Column* input,
                             const std::vector<size_t>& rows,
                             Column* buffer) {
    const auto& inputRaw = static_cast<const ColumnVector<ElementType>*>(input)->getRaw();
    auto& bufferRaw = static_cast<ColumnVector<ElementType>*>(buffer)->getRaw();

    for (const size_t row : rows) {
        bufferRaw.push_back(inputRaw[row]);
    }
}

// Grow a sum accumulator to groupCount groups, initializing each new group to a
// present zero (the additive identity). Existing groups keep their running sum. sum
// keeps no per-group tally, so the counts vector is left untouched.
template <typename Primitive>
void groupGrowZero(Column* accumulator,
                   std::vector<uint64_t>& counts,
                   size_t groupCount) {
    auto& raw = static_cast<ColumnOptVector<Primitive>*>(accumulator)->getRaw();
    raw.resize(groupCount, std::optional<Primitive>(Primitive {}));
}

// Grow a min/max accumulator to groupCount groups, initializing each new group to
// null (no extreme seen yet). Existing groups keep their running extreme. min/max
// keeps no per-group tally, so the counts vector is left untouched.
template <typename Primitive>
void groupGrowNull(Column* accumulator,
                   std::vector<uint64_t>& counts,
                   size_t groupCount) {
    auto& raw = static_cast<ColumnOptVector<Primitive>*>(accumulator)->getRaw();
    raw.resize(groupCount);
}

// Grow an avg accumulator to groupCount groups: a running f64 sum initialized to a
// present zero plus the per-group non-null tally zeroed for new groups. avg is the
// only value reduction carried as (sum, count), so it is the only one that grows the
// counts vector besides count itself.
void groupGrowAvg(Column* accumulator,
                  std::vector<uint64_t>& counts,
                  size_t groupCount) {
    auto& raw = static_cast<ColumnOptVector<double>*>(accumulator)->getRaw();
    raw.resize(groupCount, std::optional<double>(0.0));
    counts.resize(groupCount, 0);
}

// Grow a count accumulator: only the per-group tally, zeroed for new groups. count
// keeps no reduced value, so there is no accumulator column to grow.
void groupGrowCount(Column* accumulator,
                    std::vector<uint64_t>& counts,
                    size_t groupCount) {
    counts.resize(groupCount, 0);
}

// Fold a chunk's present values into per-group sum accumulators. Each new group was
// grown to a present zero, so read-modify-write its running sum.
template <typename Primitive>
void groupFoldSum(Column* accumulator,
                  std::vector<uint64_t>& counts,
                  const Column* input,
                  const std::vector<size_t>& groups) {
    auto& raw = static_cast<ColumnOptVector<Primitive>*>(accumulator)->getRaw();
    const auto& inputRaw = static_cast<const ColumnOptVector<Primitive>*>(input)->getRaw();

    for (size_t row = 0; row < inputRaw.size(); row++) {
        const std::optional<Primitive>& value = inputRaw[row];
        if (value.has_value()) {
            std::optional<Primitive>& running = raw[groups[row]];
            running = numericAdd(running.value(), *value);
        }
    }
}

// Fold a chunk's present values into per-group min (IsMax false) or max (IsMax
// true) accumulators. The first present value of a group seeds it; a later value
// replaces it when more extreme. A group with no present value stays null.
template <typename Primitive, bool IsMax>
void groupFoldMinMax(Column* accumulator,
                     std::vector<uint64_t>& counts,
                     const Column* input,
                     const std::vector<size_t>& groups) {
    auto& raw = static_cast<ColumnOptVector<Primitive>*>(accumulator)->getRaw();
    const auto& inputRaw = static_cast<const ColumnOptVector<Primitive>*>(input)->getRaw();

    for (size_t row = 0; row < inputRaw.size(); row++) {
        const std::optional<Primitive>& value = inputRaw[row];
        if (!value.has_value()) {
            continue;
        }

        std::optional<Primitive>& current = raw[groups[row]];
        if (!current.has_value()) {
            current = *value;
        } else if constexpr (IsMax) {
            if (*current < *value) {
                current = *value;
            }
        } else {
            if (*value < *current) {
                current = *value;
            }
        }
    }
}

// Fold a chunk's present values into per-group avg accumulators: a running f64 sum
// (the accumulator, widened from the input) plus the per-group non-null count. avg
// divides the two at the emit step.
template <typename Primitive>
void groupFoldAvg(Column* accumulator,
                  std::vector<uint64_t>& counts,
                  const Column* input,
                  const std::vector<size_t>& groups) {
    auto& raw = static_cast<ColumnOptVector<double>*>(accumulator)->getRaw();
    const auto& inputRaw = static_cast<const ColumnOptVector<Primitive>*>(input)->getRaw();

    for (size_t row = 0; row < inputRaw.size(); row++) {
        const std::optional<Primitive>& value = inputRaw[row];
        if (value.has_value()) {
            const size_t group = groups[row];
            std::optional<double>& running = raw[group];
            running = running.value() + static_cast<double>(*value);
            counts[group]++;
        }
    }
}

// Tally every row into its group (count(*) over a never-null column): each row
// charges its group regardless of value, so the input values are never read.
void groupFoldCountAll(Column* accumulator,
                       std::vector<uint64_t>& counts,
                       const Column* input,
                       const std::vector<size_t>& groups) {
    for (const size_t group : groups) {
        counts[group]++;
    }
}

// Tally each group's present (non-null) rows (count(x)): a null row is not charged,
// matching Cypher's count(x).
template <typename Primitive>
void groupFoldCountPresent(Column* accumulator,
                           std::vector<uint64_t>& counts,
                           const Column* input,
                           const std::vector<size_t>& groups) {
    const auto& inputRaw = static_cast<const ColumnOptVector<Primitive>*>(input)->getRaw();

    for (size_t row = 0; row < inputRaw.size(); row++) {
        if (inputRaw[row].has_value()) {
            counts[groups[row]]++;
        }
    }
}

// Emit a sum/min/max slice: the accumulator holds each group's reduced value in the
// result's own type, so copy groups [begin, begin + count) into the output.
template <typename Primitive>
void groupEmitCopy(const Column* accumulator,
                   const std::vector<uint64_t>& counts,
                   size_t begin,
                   size_t count,
                   Column* output) {
    const auto& raw = static_cast<const ColumnOptVector<Primitive>*>(accumulator)->getRaw();
    auto& outputRaw = static_cast<ColumnOptVector<Primitive>*>(output)->getRaw();
    outputRaw.assign(raw.begin() + begin, raw.begin() + begin + count);
}

// Emit an avg slice: divide each group's running f64 sum by its non-null count. A
// group with no non-null row (count zero) averages to null, matching Cypher.
void groupEmitAvg(const Column* accumulator,
                  const std::vector<uint64_t>& counts,
                  size_t begin,
                  size_t count,
                  Column* output) {
    const auto& raw = static_cast<const ColumnOptVector<double>*>(accumulator)->getRaw();
    auto& outputRaw = static_cast<ColumnOptVector<double>*>(output)->getRaw();
    outputRaw.resize(count);

    for (size_t index = 0; index < count; index++) {
        const size_t group = begin + index;
        if (counts[group] == 0) {
            outputRaw[index] = std::nullopt;
        } else {
            outputRaw[index] = std::optional<double>(raw[group].value() / static_cast<double>(counts[group]));
        }
    }
}

// Emit a count slice: each group's tally is a present unsigned i64, so copy the
// tallies of groups [begin, begin + count) into the ui64 output.
void groupEmitCount(const Column* accumulator,
                    const std::vector<uint64_t>& counts,
                    size_t begin,
                    size_t count,
                    Column* output) {
    auto& outputRaw = static_cast<ColumnVector<uint64_t>*>(output)->getRaw();
    outputRaw.assign(counts.begin() + begin, counts.begin() + begin + count);
}

// The grouped sum fold for a column of this value type. sum adds the values, so
// only a numeric column is valid - the same restriction as the scalar selectSumUpdate.
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

        default:
            throw IRException("sum requires a numeric column");
        break;
    }

    return nullptr;
}

// The grouped avg fold for a column of this value type. avg widens each value to
// f64, so - like sum - only a numeric column is valid.
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

        default:
            throw IRException("avg requires a numeric column");
        break;
    }

    return nullptr;
}

// The grouped min (IsMax false) / max (IsMax true) fold for a column of this value
// type. min/max order the values, so any orderable type is valid - numbers, bools
// and strings - but an embedding has no order, so it is rejected.
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

        default:
            throw IRException("min/max requires an orderable column");
        break;
    }

    return nullptr;
}

// Execute a get_out_edges/get_in_edges loop
template <typename ChunkWriterType>
void runEdgeLoopSteps(NLExecutionContext* context,
                      NLEdgeLoopData* loopData,
                      ChunkWriterType* chunkWriter,
                      ColumnNodeIDs* gatheredNodeIDs) {
    const NLStmtContainer* loopBody = loopData->getStmts();

    // Same early-exit as the scan loop: a null limit is unbounded, otherwise the
    // loop stops once the budget is spent, and the break unwinds any enclosing
    // loop carrying the same handle. The limit is fixed for the whole loop, so
    // the null check is hoisted out of the per-iteration condition.
    const NLLimitState* limit = loopData->getLimit();

    const auto runIteration = [&]() {
        chunkWriter->fill(context->getChunkSize());

        const ColumnVector<size_t>* indices = loopData->getIndices();
        if (indices->empty()) {
            return;
        }

        // Gather either source or target if we are get_out or get_in (the source side)
        gatherColumn<NodeID>(loopData->getInput(), indices, gatheredNodeIDs);

        // Transform all the columns in the carried set according to indices
        for (const NLCarriedColumn& carriedColumn : loopData->carriedColumns()) {
            const auto gatherFunc = carriedColumn.getGatherFunc();
            gatherFunc(carriedColumn.getInput(), indices, carriedColumn.getOutput());
        }

        runBody(context, loopBody);
    };

    if (limit) {
        while (chunkWriter->isValid() && limit->getRemaining() > 0) {
            runIteration();
        }
    } else {
        while (chunkWriter->isValid()) {
            runIteration();
        }
    }
}

}

NLExecutor::NLExecutor(const GraphView* view,
                             const NLProgram* prog,
                             NLOutputSink* sink)
    : _ctxt(view, sink, prog->getChunkSize()),
    _prog(prog)
{
}

NLExecutor::~NLExecutor() {
}

void NLExecutor::run() {
    runBody(&_ctxt, _prog->getStmts());
}

void NLExecutor::runScanNodesLoop(NLExecutionContext* context, NLFunctionData* data) {
    NLScanLoopData* loopData = static_cast<NLScanLoopData*>(data);
    const NLStmtContainer* loopBody = loopData->getStmts();
    ColumnNodeIDs* nodeIDs = loopData->getNodeIDs();
    const size_t chunkSize = context->getChunkSize();

    // A null limit leaves the loop unbounded; otherwise it stops once the budget
    // is spent. nl.limit_update inside runBody mutates remaining, so the next
    // test breaks here, and an enclosing loop carrying the same handle breaks on
    // its next test too - unwinding the whole nest. LIMIT 0 fails the guard on
    // entry, so nothing is scanned. The limit is fixed for the whole loop, so
    // the null check is hoisted out of the per-iteration condition.
    const NLLimitState* limit = loopData->getLimit();

    ScanNodesChunkWriter chunkWriter(*context->getView());
    chunkWriter.setNodeIDs(nodeIDs);

    const auto runIteration = [&]() {
        chunkWriter.fill(chunkSize);

        if (nodeIDs->empty()) {
            return;
        }

        runBody(context, loopBody);
    };

    if (limit) {
        while (chunkWriter.isValid() && limit->getRemaining() > 0) {
            runIteration();
        }
    } else {
        while (chunkWriter.isValid()) {
            runIteration();
        }
    }
}

void NLExecutor::runScanNodesByLabelLoop(NLExecutionContext* context, NLFunctionData* data) {
    NLScanByLabelLoopData* loopData = static_cast<NLScanByLabelLoopData*>(data);

    // A requested label was absent from the schema, so no node carries the full
    // conjunction: the scan matches nothing and the loop body never runs.
    if (!loopData->isMatchable()) {
        return;
    }

    const NLStmtContainer* loopBody = loopData->getStmts();
    ColumnNodeIDs* nodeIDs = loopData->getNodeIDs();
    const size_t chunkSize = context->getChunkSize();

    // A null limit leaves the loop unbounded, exactly as in runScanNodesLoop.
    const NLLimitState* limit = loopData->getLimit();

    // The LabelSetHandle borrows the loop data's owned LabelSet, which lives for
    // the whole program, so the handle stays valid for every fill below.
    const GraphView& view = *context->getView();
    const LabelSetHandle labelset(loopData->getLabelSet());

    ScanNodesByLabelChunkWriter chunkWriter(view, labelset);
    chunkWriter.setNodeIDs(nodeIDs);

    const auto runIteration = [&]() {
        chunkWriter.fill(chunkSize);

        if (nodeIDs->empty()) {
            return;
        }

        runBody(context, loopBody);
    };

    if (limit) {
        while (chunkWriter.isValid() && limit->getRemaining() > 0) {
            runIteration();
        }
    } else {
        while (chunkWriter.isValid()) {
            runIteration();
        }
    }
}

void NLExecutor::runScanEdgesLoop(NLExecutionContext* context, NLFunctionData* data) {
    NLScanEdgesLoopData* loopData = static_cast<NLScanEdgesLoopData*>(data);
    const NLStmtContainer* loopBody = loopData->getStmts();
    ColumnNodeIDs* sources = loopData->getSources();
    const size_t chunkSize = context->getChunkSize();

    // A null limit leaves the loop unbounded; otherwise it stops once the budget
    // is spent, exactly as in runScanNodesLoop.
    const NLLimitState* limit = loopData->getLimit();

    ScanEdgesChunkWriter chunkWriter(*context->getView());
    chunkWriter.setSrcIDs(sources);
    chunkWriter.setEdgeIDs(loopData->getEdgeIDs());
    chunkWriter.setEdgeTypes(loopData->getEdgeTypes());
    chunkWriter.setTgtIDs(loopData->getTargets());

    const auto runIteration = [&]() {
        chunkWriter.fill(chunkSize);

        // The four columns are row-aligned, so the source column measures the
        // step; an empty fill means the scan is drained and there is nothing to emit.
        if (sources->empty()) {
            return;
        }

        runBody(context, loopBody);
    };

    if (limit) {
        while (chunkWriter.isValid() && limit->getRemaining() > 0) {
            runIteration();
        }
    } else {
        while (chunkWriter.isValid()) {
            runIteration();
        }
    }
}

void NLExecutor::runGetOutEdgesLoop(NLExecutionContext* context, NLFunctionData* data) {
    NLEdgeLoopData* loopData = static_cast<NLEdgeLoopData*>(data);
    const ColumnNodeIDs* inputNodeIDs = loopData->getInput();

    if (inputNodeIDs->empty()) {
        return;
    }

    GetOutEdgesChunkWriter chunkWriter(*context->getView(), inputNodeIDs);
    chunkWriter.setIndices(loopData->getIndices());
    chunkWriter.setEdgeIDs(loopData->getEdgeIDs());
    chunkWriter.setEdgeTypes(loopData->getEdgeTypes());
    chunkWriter.setTgtIDs(loopData->getTargets());

    runEdgeLoopSteps(context, loopData, &chunkWriter, loopData->getSources());
}

void NLExecutor::runGetInEdgesLoop(NLExecutionContext* context, NLFunctionData* data) {
    NLEdgeLoopData* loopData = static_cast<NLEdgeLoopData*>(data);
    const ColumnNodeIDs* inputNodeIDs = loopData->getInput();

    if (inputNodeIDs->empty()) {
        return;
    }

    GetInEdgesChunkWriter chunkWriter(*context->getView(), inputNodeIDs);
    chunkWriter.setIndices(loopData->getIndices());
    chunkWriter.setEdgeIDs(loopData->getEdgeIDs());
    chunkWriter.setEdgeTypes(loopData->getEdgeTypes());
    chunkWriter.setSrcIDs(loopData->getSources());

    runEdgeLoopSteps(context, loopData, &chunkWriter, loopData->getTargets());
}

void NLExecutor::runGetOutEdgesByTypeLoop(NLExecutionContext* context, NLFunctionData* data) {
    NLEdgeByTypeLoopData* loopData = static_cast<NLEdgeByTypeLoopData*>(data);
    const ColumnNodeIDs* inputNodeIDs = loopData->getInput();

    // An edge type absent from the schema matches no edge, and an empty input has
    // no edges to walk: either way the loop body never runs.
    if (!loopData->isMatchable() || inputNodeIDs->empty()) {
        return;
    }

    GetOutEdgesByTypeChunkWriter chunkWriter(*context->getView(), inputNodeIDs, loopData->getEdgeType());
    chunkWriter.setIndices(loopData->getIndices());
    chunkWriter.setEdgeIDs(loopData->getEdgeIDs());
    chunkWriter.setEdgeTypes(loopData->getEdgeTypes());
    chunkWriter.setTgtIDs(loopData->getTargets());

    runEdgeLoopSteps(context, loopData, &chunkWriter, loopData->getSources());
}

void NLExecutor::runGetInEdgesByTypeLoop(NLExecutionContext* context, NLFunctionData* data) {
    NLEdgeByTypeLoopData* loopData = static_cast<NLEdgeByTypeLoopData*>(data);
    const ColumnNodeIDs* inputNodeIDs = loopData->getInput();

    if (!loopData->isMatchable() || inputNodeIDs->empty()) {
        return;
    }

    GetInEdgesByTypeChunkWriter chunkWriter(*context->getView(), inputNodeIDs, loopData->getEdgeType());
    chunkWriter.setIndices(loopData->getIndices());
    chunkWriter.setEdgeIDs(loopData->getEdgeIDs());
    chunkWriter.setEdgeTypes(loopData->getEdgeTypes());
    chunkWriter.setSrcIDs(loopData->getSources());

    runEdgeLoopSteps(context, loopData, &chunkWriter, loopData->getTargets());
}

void NLExecutor::runCrossProduct(NLExecutionContext* context, NLFunctionData* data) {
    NLCrossProductData* cross = static_cast<NLCrossProductData*>(data);

    const NLCrossProductData::Columns& outerColumns = cross->outerColumns();
    const NLCrossProductData::Columns& innerColumns = cross->innerColumns();
    bioassert(!outerColumns.empty() && !innerColumns.empty(),
              "nl.cross_product needs a column on each side to size the product");

    // N outer rows crossed with M inner rows: each outer column is
    // block-repeated x M and each inner column tiled x N, so every outer row
    // pairs with every inner row. The counts come from the first column of each
    // side; all columns of a side are row-aligned, so any one measures it.
    const size_t outerRowCount = outerColumns.front().getInput()->size();
    const size_t innerRowCount = innerColumns.front().getInput()->size();

    const NLLimitState* limit = cross->getLimit();
    const size_t productRowCount = outerRowCount * innerRowCount;
    const size_t remaining = limit ? limit->getRemaining() : productRowCount;
    const size_t outputRowCount = std::min(productRowCount, remaining);

    for (const NLCrossColumn& column : outerColumns) {
        const NLBroadcastFunction broadcast = column.getBroadcast();
        broadcast(column.getInput(), innerRowCount, outputRowCount, column.getOutput());
    }

    for (const NLCrossColumn& column : innerColumns) {
        const NLBroadcastFunction broadcast = column.getBroadcast();
        broadcast(column.getInput(), outerRowCount, outputRowCount, column.getOutput());
    }
}

void NLExecutor::runLimitInit(NLExecutionContext* context, NLFunctionData* data) {
    const NLLimitInitData* init = static_cast<NLLimitInitData*>(data);
    init->getState()->reset(init->getCount());
}

void NLExecutor::runLimitUpdate(NLExecutionContext* context, NLFunctionData* data) {
    const NLLimitUpdateData* update = static_cast<NLLimitUpdateData*>(data);
    update->getState()->update(update->getRows()->size());
}

void NLExecutor::runLimitTruncate(NLExecutionContext* context, NLFunctionData* data) {
    const NLLimitTruncateData* truncate = static_cast<NLLimitTruncateData*>(data);
    const size_t emitThisStep = truncate->getState()->getEmitThisStep();

    // Block-repeat with factor 1 emits each input row once and stops at
    // emitThisStep, so it copies exactly the prefix [0, emitThisStep) into the
    // fresh output chunk. Reads emitThisStep (set by the preceding
    // nl.limit_update); never mutates the counter.
    for (const NLCrossColumn& column : truncate->columns()) {
        const NLBroadcastFunction copyPrefix = column.getBroadcast();
        copyPrefix(column.getInput(), 1, emitThisStep, column.getOutput());
    }
}

void NLExecutor::runSkipInit(NLExecutionContext* context, NLFunctionData* data) {
    const NLSkipInitData* init = static_cast<NLSkipInitData*>(data);
    init->getState()->reset(init->getCount());
}

void NLExecutor::runSkipUpdate(NLExecutionContext* context, NLFunctionData* data) {
    const NLSkipUpdateData* update = static_cast<NLSkipUpdateData*>(data);
    update->getState()->update(update->getRows()->size());
}

void NLExecutor::runSkipTruncate(NLExecutionContext* context, NLFunctionData* data) {
    const NLSkipTruncateData* truncate = static_cast<NLSkipTruncateData*>(data);
    const NLSkipState* state = truncate->getState();

    // Copy the surviving suffix [skipThisStep, skipThisStep + emitThisStep) of each
    // column into the fresh front-aligned output chunk. Reads the offset and count
    // (set by the preceding nl.skip_update); never mutates the counter.
    const size_t offset = state->getSkipThisStep();
    const size_t rowCount = state->getEmitThisStep();
    for (const NLSkipColumn& column : truncate->columns()) {
        const NLCopyFunction copySuffix = column.getCopy();
        copySuffix(column.getInput(), offset, rowCount, column.getOutput());
    }
}

void NLExecutor::runOutput(NLExecutionContext* context, NLFunctionData* data) {
    const NLOutputData* output = static_cast<NLOutputData*>(data);
    const auto& cols = output->outputs();
    bioassert(!cols.empty(), "nl.output requires at least one column");

    // Compute the [offset, offset + rowCount) window to emit, copy-free:
    //  - skip (the folded terminal-SKIP form): emit the surviving suffix at offset
    //    getSkipThisStep() for getEmitThisStep() rows, both sized by the preceding
    //    nl.skip_update. Reading them, not remaining, so the decrement already done
    //    does not affect the window.
    //  - limit (the folded terminal-LIMIT form): emit the getEmitThisStep() prefix
    //    nl.limit_update sized this step, from offset zero.
    //  - neither: emit the whole chunk (already cut by a truncate when one governs
    //    it).
    // A folded output carries at most one of limit/skip, so the two never combine.
    const NLLimitState* limit = output->getLimit();
    const NLSkipState* skip = output->getSkip();
    // Column that defines the cardinality of the result
    const Column* cardinality = output->getCardinality();

    size_t offset = 0;
    size_t rowCount = 0;
    if (skip) {
        offset = skip->getSkipThisStep();
        rowCount = skip->getEmitThisStep();
    } else if (limit) {
        rowCount = limit->getEmitThisStep();
    } else if (cardinality) {
        rowCount = cardinality->size();
    } else {
        // Logical row count, assumes all columns are either const or same dimension
        for (const Column* column : cols) {
            rowCount = std::max(rowCount, column->size());
        }
    }

    context->getSink()->appendChunks(cols, offset, rowCount);
}

void NLExecutor::runBinary(NLExecutionContext*, NLFunctionData* data) {
    const NLBinaryData* binary = static_cast<NLBinaryData*>(data);
    binary->getFn()(binary->getResult(), binary->getLhs(), binary->getRhs());
}

void NLExecutor::runUnary(NLExecutionContext*, NLFunctionData* data) {
    const NLUnaryData* unary = static_cast<NLUnaryData*>(data);
    unary->getFn()(unary->getResult(), unary->getOperand());
}

NLUnaryFn NLExecutor::selectNot(const Column* operand, LocalMemory* memory, Column*& result) {
    const ContainerKind::Code kind = operand->getContainerKind();

    if (kind == ContainerKind::code<ColumnConst<CustomBool>>()) {
        result = memory->alloc<ColumnConst<CustomBool>>();
        return &applyNotOnConst;
    }

    if (kind == ContainerKind::code<ColumnOptMask>()) {
        result = memory->alloc<ColumnOptMask>();
        return &applyNotOnOptMask;
    }

    result = memory->alloc<ColumnMask>();
    return &applyNotOnMask;
}

template <ColumnOperator Op>
NLBinaryFn NLExecutor::selectBinary(const Column* lhs,
                                    const Column* rhs,
                                    LocalMemory* memory,
                                    Column*& result) {
    using Pairs = PairRestrictions<Op>;

    BinaryOpSelector<Op> selector {._memory = memory};
    ColumnDoubleDispatcher<typename Pairs::Allowed,
                           typename Pairs::AllowedMixed,
                           BinaryOpSelector<Op>,
                           typename Pairs::Excluded>::dispatch(lhs, rhs, selector);

    result = selector._result;
    return selector._fn;
}

void NLExecutor::runSortReset(NLExecutionContext* context, NLFunctionData* data) {
    const NLSortResetData* reset = static_cast<NLSortResetData*>(data);
    reset->getState()->reset();
}

void NLExecutor::runSortCollect(NLExecutionContext* context, NLFunctionData* data) {
    const NLSortCollectData* collect = static_cast<NLSortCollectData*>(data);

    // Append this step's chunk of every column onto its buffer's tail; the
    // columns are taken together so the buffers stay row-aligned.
    for (const NLSortCollectData::Append& append : collect->appends()) {
        append._append(append._input, append._buffer);
    }

    // For a bounded (top-K) accumulator, drop all but the best k once the buffers
    // have grown past the bound, so memory stays O(k) rather than O(rows). A
    // no-op for an unbounded sort.
    collect->getState()->trimIfNeeded();
}

void NLExecutor::runSortLoop(NLExecutionContext* context, NLFunctionData* data) {
    NLSortLoopData* loopData = static_cast<NLSortLoopData*>(data);
    NLSortState* state = loopData->getState();

    // Sort the accumulated rows once: the permutation is the global row order the
    // emit chunks read in.
    state->sort();
    const std::vector<size_t>& permutation = state->permutation().getRaw();
    const size_t totalRows = permutation.size();

    const NLStmtContainer* loopBody = loopData->getStmts();
    const size_t chunkSize = context->getChunkSize();
    ColumnVector<size_t>* indices = loopData->getIndices();

    // Re-chunk the sorted rows: each step gathers the next chunkSize rows, in
    // permutation order, into the loop variables, then runs the body (nl.output).
    // The last chunk may be partial; an empty result runs the body zero times.
    for (size_t offset = 0; offset < totalRows; offset += chunkSize) {
        const size_t stepRows = std::min(chunkSize, totalRows - offset);

        std::vector<size_t>& indicesRaw = indices->getRaw();
        indicesRaw.assign(permutation.begin() + offset, permutation.begin() + offset + stepRows);

        for (const NLCarriedColumn& column : loopData->columns()) {
            const NLGatherFunction gather = column.getGatherFunc();
            gather(column.getInput(), indices, column.getOutput());
        }

        runBody(context, loopBody);
    }
}

void NLExecutor::runDistinctReset(NLExecutionContext* context, NLFunctionData* data) {
    const NLDistinctResetData* reset = static_cast<NLDistinctResetData*>(data);
    reset->getState()->reset();
}

void NLExecutor::runDistinctFilter(NLExecutionContext* context, NLFunctionData* data) {
    NLDistinctFilterData* filter = static_cast<NLDistinctFilterData*>(data);
    NLDistinctState* state = filter->getState();

    const std::vector<NLDistinctFilterData::FilterColumn>& columns = filter->columns();
    bioassert(!columns.empty(), "nl.distinct_filter needs at least one column");

    // Every column is row-aligned, so the first sizes this step's row set.
    const size_t rowCount = columns.front()._input->size();

    // Collect this step's surviving row indices: a row survives iff its key - the
    // concatenation of every column's serialized value at that row - is new to the
    // seen-set. insertIfNew both tests membership and records the new key, so a
    // duplicate later in the same chunk is caught by an earlier row of it too.
    ColumnVector<size_t>* indices = filter->getIndices();
    std::vector<size_t>& survivingRaw = indices->getRaw();
    survivingRaw.clear();

    std::string* key = filter->getKeyScratch();
    for (size_t row = 0; row < rowCount; row++) {
        key->clear();
        for (const NLDistinctFilterData::FilterColumn& column : columns) {
            column._keyAppend(column._input, row, *key);
        }

        if (state->insertIfNew(*key)) {
            survivingRaw.push_back(row);
        }
    }

    // Gather the survivors into the fresh output chunks, in first-seen order, so a
    // downstream consumer reads a genuinely deduped chunk.
    for (const NLDistinctFilterData::FilterColumn& column : columns) {
        column._gather(column._input, indices, column._output);
    }
}

void NLExecutor::runFilter(NLExecutionContext* context, NLFunctionData* data) {
    NLFilterData* filter = static_cast<NLFilterData*>(data);

    const std::vector<NLFilterData::FilterColumn>& columns = filter->columns();
    bioassert(!columns.empty(), "nl.filter needs at least one column");

    const Column* mask = filter->getMask();

    // Special case for a ColumnConst<Bool> mask where all rows are either kept or
    // filtered
    if (isMaskConstant(mask)) {
        const auto* constMask = static_cast<const ColumnConst<CustomBool>*>(mask);
        const bool passes = !constMask->empty() && constMask->getRaw()._boolean;

        for (const NLFilterData::FilterColumn& column : columns) {
            if (passes) {
                column._output->assign(column._input);
            } else {
                column._output->clear();
            }
        }
        return;
    }

    // General case: Otherwise apply the mask to each column
    ColumnVector<size_t>* indices = filter->getIndices();
    indices->getRaw().clear();
    filter->getSurvivors()(mask, indices);

    for (const NLFilterData::FilterColumn& column : columns) {
        column._gather(column._input, indices, column._output);
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

void NLExecutor::runAggregateReset(NLExecutionContext* context, NLFunctionData* data) {
    const NLAggregateResetData* reset = static_cast<NLAggregateResetData*>(data);
    reset->getReset()(reset->getState());
}

void NLExecutor::runAggregateUpdate(NLExecutionContext* context, NLFunctionData* data) {
    const NLAggregateUpdateData* update = static_cast<NLAggregateUpdateData*>(data);

    // Fold this step's non-null values into the accumulator the way its kind and
    // value type demand (add for sum/avg, keep the extreme for min/max).
    update->getUpdate()(update->getState(), update->getInput());
}

void NLExecutor::runAggregateResult(NLExecutionContext* context, NLFunctionData* data) {
    const NLAggregateResultData* result = static_cast<NLAggregateResultData*>(data);

    // The aggregate collapses every folded row to a single result row: write the
    // reduced value into the output chunk's one nullable row. Runs after the
    // producing loop, so the accumulator holds the whole dataflow's reduction;
    // nl.output emits this chunk at function scope.
    result->getResult()(result->getState(), result->getOutput());
}

void NLExecutor::runGroupAggregateReset(NLExecutionContext* context, NLFunctionData* data) {
    const NLGroupAggregateResetData* reset = static_cast<NLGroupAggregateResetData*>(data);
    reset->getState()->reset();
}

void NLExecutor::runGroupAggregateUpdate(NLExecutionContext* context, NLFunctionData* data) {
    NLGroupAggregateUpdateData* update = static_cast<NLGroupAggregateUpdateData*>(data);
    NLGroupAggregateState* state = update->getState();

    std::vector<NLGroupAggregateState::KeyColumn>& keyColumns = state->keyColumns();
    std::vector<NLGroupAggregateState::Aggregate>& aggregates = state->aggregates();
    bioassert(!keyColumns.empty(), "nl.group_aggregate_update needs at least one grouping key");

    // Every column is row-aligned, so the first grouping key sizes this step's rows.
    const size_t rowCount = keyColumns.front()._input->size();
    if (rowCount == 0) {
        return;
    }

    // Assign each row to its group: serialize the grouping-key tuple, look it up,
    // and on first sight create a new group (its index is the next group count) and
    // record the row that created it, so the key buffers can take that row's values.
    NLGroupTable& groupTable = state->groupTable();
    std::vector<size_t>& groupIndices = state->groupIndicesScratch();
    std::vector<size_t>& newGroupRows = state->newGroupRowsScratch();
    std::string& key = state->keyScratch();

    groupIndices.resize(rowCount);
    newGroupRows.clear();

    for (size_t row = 0; row < rowCount; row++) {
        key.clear();
        for (const NLGroupAggregateState::KeyColumn& keyColumn : keyColumns) {
            keyColumn._keyAppend(keyColumn._input, row, key);
        }

        const NLGroupTable::Assignment assignment = groupTable.assign(key);
        if (assignment._created) {
            newGroupRows.push_back(row);
        }

        groupIndices[row] = assignment._index;
    }

    const size_t groupCount = groupTable.getGroupCount();

    // Grow the key buffers with the new groups' key values, then grow every
    // accumulator to the new group count - initializing the new groups to their
    // reduction's identity - before folding this step's rows into their groups.
    for (NLGroupAggregateState::KeyColumn& keyColumn : keyColumns) {
        keyColumn._gatherAppend(keyColumn._input, newGroupRows, keyColumn._buffer);
    }

    for (NLGroupAggregateState::Aggregate& aggregate : aggregates) {
        aggregate._grow(aggregate._accumulator, aggregate._counts, groupCount);
    }

    for (NLGroupAggregateState::Aggregate& aggregate : aggregates) {
        aggregate._fold(aggregate._accumulator, aggregate._counts, aggregate._input, groupIndices);
    }
}

void NLExecutor::runGroupAggregateLoop(NLExecutionContext* context, NLFunctionData* data) {
    NLGroupAggregateLoopData* loopData = static_cast<NLGroupAggregateLoopData*>(data);
    NLGroupAggregateState* state = loopData->getState();

    const size_t totalGroups = state->groupTable().getGroupCount();
    const size_t chunkSize = context->getChunkSize();
    const NLStmtContainer* loopBody = loopData->getStmts();

    std::vector<NLGroupAggregateState::KeyColumn>& keyColumns = state->keyColumns();
    std::vector<NLGroupAggregateState::Aggregate>& aggregates = state->aggregates();

    // Re-chunk the accumulated groups: each step materializes the next chunk of
    // group rows - the key values sliced from the buffers and each aggregate
    // finalized from the per-group state - into the loop variables, then runs the
    // body (the nl.output) per chunk. An empty result (no group) runs the body zero
    // times, so a grouped aggregate over no row emits nothing.
    for (size_t offset = 0; offset < totalGroups; offset += chunkSize) {
        const size_t stepGroups = std::min(chunkSize, totalGroups - offset);

        for (const NLGroupAggregateState::KeyColumn& keyColumn : keyColumns) {
            keyColumn._emitCopy(keyColumn._buffer, offset, stepGroups, keyColumn._output);
        }

        for (const NLGroupAggregateState::Aggregate& aggregate : aggregates) {
            aggregate._emit(aggregate._accumulator, aggregate._counts, offset, stepGroups, aggregate._output);
        }

        runBody(context, loopBody);
    }
}

NLGatherFunction NLExecutor::selectGatherFunction(NLChunkKind kind) {
    switch (kind) {
        case NLChunkKind::NodeID:
            return &gatherColumn<NodeID>;
        break;

        case NLChunkKind::EdgeID:
            return &gatherColumn<EdgeID>;
        break;

        case NLChunkKind::EdgeTypeID:
            return &gatherColumn<EdgeTypeID>;
        break;
    }

    bioassert(false, "Unknown NLChunkKind");
    return nullptr;
}

NLMaskSurvivorFunction NLExecutor::selectMaskSurvivorFunction(bool nullable) {
    if (nullable) {
        return &collectOptMaskSurvivors;
    }

    return &collectMaskSurvivors;
}

NLBroadcastFunction NLExecutor::selectBlockRepeatFunction(NLChunkKind kind) {
    switch (kind) {
        case NLChunkKind::NodeID:
            return &blockRepeatColumn<NodeID>;
        break;

        case NLChunkKind::EdgeID:
            return &blockRepeatColumn<EdgeID>;
        break;

        case NLChunkKind::EdgeTypeID:
            return &blockRepeatColumn<EdgeTypeID>;
        break;
    }

    bioassert(false, "Unknown NLChunkKind");
    return nullptr;
}

NLBroadcastFunction NLExecutor::selectTileFunction(NLChunkKind kind) {
    switch (kind) {
        case NLChunkKind::NodeID:
            return &tileColumn<NodeID>;
        break;

        case NLChunkKind::EdgeID:
            return &tileColumn<EdgeID>;
        break;

        case NLChunkKind::EdgeTypeID:
            return &tileColumn<EdgeTypeID>;
        break;
    }

    bioassert(false, "Unknown NLChunkKind");
    return nullptr;
}

// A nullable value chunk is a ColumnOptVector<Primitive> - that is,
// ColumnVector<std::optional<Primitive>> - so the same broadcast templates,
// instantiated on std::optional<Primitive>, carry value and null together.
NLBroadcastFunction NLExecutor::selectOptBlockRepeatFunction(ValueType valueType) {
    NLBroadcastFunction broadcast = nullptr;
    const auto select = [&]<SupportedType T>() {
        broadcast = &blockRepeatColumn<std::optional<typename T::Primitive>>;
    };
    ValueTypeDispatcher(valueType).execute(select);

    return broadcast;
}

NLBroadcastFunction NLExecutor::selectOptTileFunction(ValueType valueType) {
    NLBroadcastFunction broadcast = nullptr;
    const auto select = [&]<SupportedType T>() {
        broadcast = &tileColumn<std::optional<typename T::Primitive>>;
    };
    ValueTypeDispatcher(valueType).execute(select);

    return broadcast;
}

NLCopyFunction NLExecutor::selectCopyFunction(NLChunkKind kind) {
    switch (kind) {
        case NLChunkKind::NodeID:
            return &copyRangeColumn<NodeID>;
        break;

        case NLChunkKind::EdgeID:
            return &copyRangeColumn<EdgeID>;
        break;

        case NLChunkKind::EdgeTypeID:
            return &copyRangeColumn<EdgeTypeID>;
        break;
    }

    bioassert(false, "Unknown NLChunkKind");
    return nullptr;
}

// A nullable value chunk gathers the same way an ID chunk does - copy the indexed
// rows - on the ColumnOptVector<Primitive> instantiation of the gather template.
NLGatherFunction NLExecutor::selectOptGatherFunction(ValueType valueType) {
    NLGatherFunction gather = nullptr;
    const auto select = [&]<SupportedType T>() {
        gather = &gatherColumn<std::optional<typename T::Primitive>>;
    };
    ValueTypeDispatcher(valueType).execute(select);

    return gather;
}

NLAppendFunction NLExecutor::selectAppendFunction(NLChunkKind kind) {
    switch (kind) {
        case NLChunkKind::NodeID:
            return &appendColumn<NodeID>;
        break;

        case NLChunkKind::EdgeID:
            return &appendColumn<EdgeID>;
        break;

        case NLChunkKind::EdgeTypeID:
            return &appendColumn<EdgeTypeID>;
        break;
    }

    bioassert(false, "Unknown NLChunkKind");
    return nullptr;
}

// A nullable value chunk is a ColumnOptVector<Primitive> - that is,
// ColumnVector<std::optional<Primitive>> - so the range copy template,
// instantiated on std::optional<Primitive>, carries value and null together.
NLCopyFunction NLExecutor::selectOptCopyFunction(ValueType valueType) {
    NLCopyFunction copy = nullptr;
    const auto select = [&]<SupportedType T>() {
        copy = &copyRangeColumn<std::optional<typename T::Primitive>>;
    };
    ValueTypeDispatcher(valueType).execute(select);

    return copy;
}

NLAppendFunction NLExecutor::selectOptAppendFunction(ValueType valueType) {
    NLAppendFunction append = nullptr;
    const auto select = [&]<SupportedType T>() {
        append = &appendColumn<std::optional<typename T::Primitive>>;
    };
    ValueTypeDispatcher(valueType).execute(select);

    return append;
}

NLKeyAppendFunction NLExecutor::selectKeyAppendFunction(NLChunkKind kind) {
    switch (kind) {
        case NLChunkKind::NodeID:
            return &distinctKeyAppendColumn<NodeID>;
        break;

        case NLChunkKind::EdgeID:
            return &distinctKeyAppendColumn<EdgeID>;
        break;

        case NLChunkKind::EdgeTypeID:
            return &distinctKeyAppendColumn<EdgeTypeID>;
        break;
    }

    bioassert(false, "Unknown NLChunkKind");
    return nullptr;
}

// Selected per column from its value type. A manual switch, not ValueTypeDispatcher,
// because an embedding has no byte identity to key on: dispatching would instantiate
// the serializer for std::span<const float> - a view, not owned bytes - which cannot
// be a DISTINCT key. The embedding case throws instead, so that instantiation is
// never named, the same shape as selectOptCompareFunction.
NLKeyAppendFunction NLExecutor::selectOptKeyAppendFunction(ValueType valueType) {
    switch (valueType) {
        case ValueType::Int64:
            return &distinctKeyAppendOptColumn<types::Int64::Primitive>;
        break;

        case ValueType::UInt64:
            return &distinctKeyAppendOptColumn<types::UInt64::Primitive>;
        break;

        case ValueType::Double:
            return &distinctKeyAppendOptColumn<types::Double::Primitive>;
        break;

        case ValueType::Bool:
            return &distinctKeyAppendOptColumn<types::Bool::Primitive>;
        break;

        case ValueType::String:
            return &distinctKeyAppendOptColumn<types::String::Primitive>;
        break;

        case ValueType::Embedding:
            throw IRException("cannot remove duplicates on an embedding column");
        break;

        case ValueType::Invalid:
        case ValueType::_SIZE:
            throw IRException("invalid distinct key value type");
        break;
    }

    bioassert(false, "Unhandled value type");
    return nullptr;
}

// An ID chunk (node/edge/edge-type IDs) has no null rows, so every row counts,
// regardless of kind. The count sibling of selectKeyAppendFunction, but kind is
// irrelevant here - the row count is just the column size.
size_t NLExecutor::countAllRows(const Column* column) {
    return column->size();
}

// Selected per column from its value type, so count(x) tallies only the rows in
// which x is not null. Every value type has a present/absent flag, so - unlike
// selectOptKeyAppendFunction - an embedding is fine: counting reads has_value(),
// never the value's bytes.
NLCountFunction NLExecutor::selectOptCountFunction(ValueType valueType) {
    NLCountFunction count = nullptr;
    const auto select = [&]<SupportedType T>() {
        count = &countPresentColumn<std::optional<typename T::Primitive>>;
    };
    ValueTypeDispatcher(valueType).execute(select);

    return count;
}

NLAggregateResetFunction NLExecutor::selectAggregateReset(AggregateKind kind, ValueType accumulatorType) {
    // sum/avg reset to a present zero (their identity); min/max reset to null. Both
    // resets compile for any value type and lowering has already validated the
    // kind / type pairing (and the update selector re-checks it), so a single
    // dispatch over the accumulator type suffices here.
    const bool resetsToZero = (kind == AggregateKind::Sum || kind == AggregateKind::Avg);

    NLAggregateResetFunction reset = nullptr;
    const auto select = [&]<SupportedType T>() {
        reset = resetsToZero ? &aggregateResetZero<typename T::Primitive>
                             : &aggregateResetNull<typename T::Primitive>;
    };
    ValueTypeDispatcher(accumulatorType).execute(select);

    return reset;
}

NLAggregateUpdateFunction NLExecutor::selectAggregateUpdate(AggregateKind kind, ValueType inputType) {
    switch (kind) {
        case AggregateKind::Sum:
            return selectSumUpdate(inputType);
        break;

        case AggregateKind::Avg:
            return selectAvgUpdate(inputType);
        break;

        case AggregateKind::Min:
            return selectMinMaxUpdate</*IsMax=*/false>(inputType);
        break;

        case AggregateKind::Max:
            return selectMinMaxUpdate</*IsMax=*/true>(inputType);
        break;
    }

    bioassert(false, "Unhandled aggregate kind");
    return nullptr;
}

NLAggregateResultFunction NLExecutor::selectAggregateResult(AggregateKind kind, ValueType resultType) {
    if (kind == AggregateKind::Avg) {
        // avg always emits an f64 (the running sum divided by the count), whatever
        // the input type was.
        return &aggregateResultAvg;
    }

    // sum/min/max hold the reduced value in the result's own type, so the emit is a
    // copy of the accumulator's single row - valid for any value type.
    NLAggregateResultFunction result = nullptr;
    const auto select = [&]<SupportedType T>() {
        result = &aggregateResultCopy<typename T::Primitive>;
    };
    ValueTypeDispatcher(resultType).execute(select);

    return result;
}

NLGroupAggregateGrowFunction NLExecutor::selectGroupAggregateGrow(GroupAggregateKind kind, ValueType accumulatorType) {
    // count keeps only a per-group tally, so its grow ignores the accumulator type;
    // avg carries a running f64 sum plus that tally, so its grow is always f64-typed
    // and is the only value reduction besides count that grows the counts vector. A
    // switch (not an if/else) over every kind so a new one is a compile error here
    // rather than silently taking the sum/min/max path.
    switch (kind) {
        case GroupAggregateKind::Count:
            return &groupGrowCount;
        break;

        case GroupAggregateKind::Avg:
            return &groupGrowAvg;
        break;

        case GroupAggregateKind::Sum:
        case GroupAggregateKind::Min:
        case GroupAggregateKind::Max: {
            // sum grows each new group to a present zero (its additive identity),
            // min/max to null (no extreme seen yet); neither carries a per-group
            // tally. Both compile for any value type, and lowering has already
            // validated the kind / type pairing (and the fold selector re-checks it),
            // so one dispatch over the accumulator type suffices - the grouped sibling
            // of selectAggregateReset.
            const bool growsToZero = (kind == GroupAggregateKind::Sum);

            NLGroupAggregateGrowFunction grow = nullptr;
            const auto select = [&]<SupportedType T>() {
                grow = growsToZero ? &groupGrowZero<typename T::Primitive>
                                   : &groupGrowNull<typename T::Primitive>;
            };
            ValueTypeDispatcher(accumulatorType).execute(select);

            return grow;
        }
        break;
    }

    bioassert(false, "Unhandled group aggregate kind");
    return nullptr;
}

NLGroupAggregateFoldFunction NLExecutor::selectGroupAggregateFold(GroupAggregateKind kind, ValueType inputType) {
    switch (kind) {
        case GroupAggregateKind::Count: {
            // count(x) over a nullable value chunk: tally the present values. count(*)
            // over an ID chunk goes through selectGroupCountAllFold instead. Every
            // value type has a present flag, so - like selectOptCountFunction - an
            // embedding is fine (the fold reads has_value(), never the bytes).
            NLGroupAggregateFoldFunction fold = nullptr;
            const auto select = [&]<SupportedType T>() {
                fold = &groupFoldCountPresent<typename T::Primitive>;
            };
            ValueTypeDispatcher(inputType).execute(select);
            return fold;
        }
        break;

        case GroupAggregateKind::Sum:
            return selectGroupSumFold(inputType);
        break;

        case GroupAggregateKind::Avg:
            return selectGroupAvgFold(inputType);
        break;

        case GroupAggregateKind::Min:
            return selectGroupMinMaxFold</*IsMax=*/false>(inputType);
        break;

        case GroupAggregateKind::Max:
            return selectGroupMinMaxFold</*IsMax=*/true>(inputType);
        break;
    }

    bioassert(false, "Unhandled group aggregate kind");
    return nullptr;
}

NLGroupAggregateFoldFunction NLExecutor::selectGroupCountAllFold() {
    // count(*) over an ID chunk (never null): every row charges its group.
    return &groupFoldCountAll;
}

NLGroupAggregateEmitFunction NLExecutor::selectGroupAggregateEmit(GroupAggregateKind kind, ValueType resultType) {
    // A switch (not an if/else) over every kind so a new one is a compile error here
    // rather than silently taking the sum/min/max path.
    switch (kind) {
        case GroupAggregateKind::Count:
            // count emits its per-group tally as an unsigned i64, whatever the input was.
            return &groupEmitCount;
        break;

        case GroupAggregateKind::Avg:
            // avg emits the running f64 sum divided by the count, per group.
            return &groupEmitAvg;
        break;

        case GroupAggregateKind::Sum:
        case GroupAggregateKind::Min:
        case GroupAggregateKind::Max: {
            // sum/min/max hold each group's reduced value in the result's own type, so
            // the emit copies the accumulator slice - valid for any value type, the
            // grouped sibling of selectAggregateResult.
            NLGroupAggregateEmitFunction emit = nullptr;
            const auto select = [&]<SupportedType T>() {
                emit = &groupEmitCopy<typename T::Primitive>;
            };
            ValueTypeDispatcher(resultType).execute(select);

            return emit;
        }
        break;
    }

    bioassert(false, "Unhandled group aggregate kind");
    return nullptr;
}

NLGroupKeyGatherFunction NLExecutor::selectGroupKeyGather(NLChunkKind kind) {
    switch (kind) {
        case NLChunkKind::NodeID:
            return &groupGatherAppendColumn<NodeID>;
        break;

        case NLChunkKind::EdgeID:
            return &groupGatherAppendColumn<EdgeID>;
        break;

        case NLChunkKind::EdgeTypeID:
            return &groupGatherAppendColumn<EdgeTypeID>;
        break;
    }

    bioassert(false, "Unknown NLChunkKind");
    return nullptr;
}

// A nullable key column appends the same way an ID key does - copy the chosen rows -
// on the ColumnOptVector<Primitive> instantiation of the gather-append template.
NLGroupKeyGatherFunction NLExecutor::selectOptGroupKeyGather(ValueType valueType) {
    NLGroupKeyGatherFunction gather = nullptr;
    const auto select = [&]<SupportedType T>() {
        gather = &groupGatherAppendColumn<std::optional<typename T::Primitive>>;
    };
    ValueTypeDispatcher(valueType).execute(select);

    return gather;
}

NLCompareFunction NLExecutor::selectCompareFunction(NLChunkKind kind) {
    switch (kind) {
        case NLChunkKind::NodeID:
            return &compareColumn<NodeID>;
        break;

        case NLChunkKind::EdgeID:
            return &compareColumn<EdgeID>;
        break;

        case NLChunkKind::EdgeTypeID:
            return &compareColumn<EdgeTypeID>;
        break;
    }

    bioassert(false, "Unknown NLChunkKind");
    return nullptr;
}

// Selected per key from its value type. A manual switch, not ValueTypeDispatcher,
// because an embedding has no order: dispatching would instantiate the comparator
// for std::span<const float>, which does not compile. The embedding case throws
// instead, so that instantiation is never named.
NLCompareFunction NLExecutor::selectOptCompareFunction(ValueType valueType) {
    switch (valueType) {
        case ValueType::Int64:
            return &compareOptColumn<types::Int64::Primitive>;
        break;

        case ValueType::UInt64:
            return &compareOptColumn<types::UInt64::Primitive>;
        break;

        case ValueType::Double:
            return &compareOptColumn<types::Double::Primitive>;
        break;

        case ValueType::Bool:
            return &compareOptColumn<types::Bool::Primitive>;
        break;

        case ValueType::String:
            return &compareOptColumn<types::String::Primitive>;
        break;

        case ValueType::Embedding:
            throw IRException("cannot sort by an embedding column");
        break;

        case ValueType::Invalid:
        case ValueType::_SIZE:
            throw IRException("invalid sort key value type");
        break;
    }

    bioassert(false, "Unhandled value type");
    return nullptr;
}

// Read one property of the current input chunk into a nullable value column.
// The with-null writer emits one value per input row (null where the row lacks
// it), so no row is dropped and the value column lines up with the input chunk.
// The PropertyTypeID was resolved from the name during translation.
template <typename ID, typename T>
void NLExecutor::runPropertyFetch(NLExecutionContext* context, NLFunctionData* data) {
    NLPropertyFetchData* fetchData = static_cast<NLPropertyFetchData*>(data);

    const GraphView& view = *context->getView();
    const PropertyTypeID propertyTypeID = fetchData->getPropertyTypeID();
    const auto* inputIDs = static_cast<const ColumnVector<ID>*>(fetchData->getInput());
    auto* output = static_cast<ColumnOptVector<typename T::Primitive>*>(fetchData->getOutput());

    GetPropertiesWithNullChunkWriter<ID, T> writer(view, propertyTypeID, inputIDs);
    writer.setOutput(output);
    writer.fill(inputIDs->size());
}

// The translator selects among these by the value type the property resolves
// to, on the node or edge side; only these (ID, T) pairs are available as
// handlers.
template void NLExecutor::runPropertyFetch<NodeID, types::Int64>(NLExecutionContext*, NLFunctionData*);
template void NLExecutor::runPropertyFetch<NodeID, types::UInt64>(NLExecutionContext*, NLFunctionData*);
template void NLExecutor::runPropertyFetch<NodeID, types::Double>(NLExecutionContext*, NLFunctionData*);
template void NLExecutor::runPropertyFetch<NodeID, types::Bool>(NLExecutionContext*, NLFunctionData*);
template void NLExecutor::runPropertyFetch<NodeID, types::String>(NLExecutionContext*, NLFunctionData*);
template void NLExecutor::runPropertyFetch<NodeID, types::Embedding>(NLExecutionContext*, NLFunctionData*);
template void NLExecutor::runPropertyFetch<EdgeID, types::Int64>(NLExecutionContext*, NLFunctionData*);
template void NLExecutor::runPropertyFetch<EdgeID, types::UInt64>(NLExecutionContext*, NLFunctionData*);
template void NLExecutor::runPropertyFetch<EdgeID, types::Double>(NLExecutionContext*, NLFunctionData*);
template void NLExecutor::runPropertyFetch<EdgeID, types::Bool>(NLExecutionContext*, NLFunctionData*);
template void NLExecutor::runPropertyFetch<EdgeID, types::String>(NLExecutionContext*, NLFunctionData*);
template void NLExecutor::runPropertyFetch<EdgeID, types::Embedding>(NLExecutionContext*, NLFunctionData*);

template NLBinaryFn NLExecutor::selectBinary<OP_ADD>(const Column* lhs, const Column* rhs, LocalMemory* memory, Column*& result);
template NLBinaryFn NLExecutor::selectBinary<OP_SUB>(const Column* lhs, const Column* rhs, LocalMemory* memory, Column*& result);
template NLBinaryFn NLExecutor::selectBinary<OP_MUL>(const Column* lhs, const Column* rhs, LocalMemory* memory, Column*& result);
template NLBinaryFn NLExecutor::selectBinary<OP_DIV>(const Column* lhs, const Column* rhs, LocalMemory* memory, Column*& result);
template NLBinaryFn NLExecutor::selectBinary<OP_EQUAL>(const Column* lhs, const Column* rhs, LocalMemory* memory, Column*& result);
template NLBinaryFn NLExecutor::selectBinary<OP_GREATER_THAN>(const Column* lhs, const Column* rhs, LocalMemory* memory, Column*& result);
template NLBinaryFn NLExecutor::selectBinary<OP_LESS_THAN>(const Column* lhs, const Column* rhs, LocalMemory* memory, Column*& result);
template NLBinaryFn NLExecutor::selectBinary<OP_GREATER_THAN_OR_EQUAL>(const Column* lhs, const Column* rhs, LocalMemory* memory, Column*& result);
template NLBinaryFn NLExecutor::selectBinary<OP_LESS_THAN_OR_EQUAL>(const Column* lhs, const Column* rhs, LocalMemory* memory, Column*& result);
template NLBinaryFn NLExecutor::selectBinary<OP_AND>(const Column* lhs, const Column* rhs, LocalMemory* memory, Column*& result);
template NLBinaryFn NLExecutor::selectBinary<OP_OR>(const Column* lhs, const Column* rhs, LocalMemory* memory, Column*& result);

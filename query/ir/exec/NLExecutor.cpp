#include "NLExecutor.h"

#include <stdint.h>
#include <algorithm>
#include <bit>
#include <cmath>
#include <concepts>
#include <limits>
#include <numeric>
#include <span>
#include <string>
#include <string_view>
#include <type_traits>

#include <spdlog/fmt/bundled/format.h>

#include "columns/ContainerKind.h"
#include "ID.h"
#include "iterators/GetInEdgesIterator.h"
#include "iterators/GetOutEdgesIterator.h"
#include "columns/ColumnOptVector.h"
#include "columns/ColumnVector.h"
#include "columns/ColumnConst.h"
#include "columns/ColumnCombinations.h"
#include "columns/ColumnOperator.h"
#include "columns/ColumnOperators.h"
#include "columns/ColumnOperatorDispatcher.h"
#include "columns/AllowedKinds.h"
#include "columns/Functions.h"
#include "CellOrder.h"
#include "CellUtils.h"
#include "QueryTypeName.h"
#include "metadata/PropertyType.h"

#include "versioning/CommitWriteBuffer.h"
#include "views/GraphView.h"

#include "NLSystemContext.h"

#include "NLSerializedKey.h"
#include "NLProgram.h"
#include "NLRuntimeProperty.h"
#include "NLMergeExecutor.h"
#include "NLEntityProperties.h"
#include "NLWriteProperties.h"
#include "NLWrittenValues.h"
#include "NLOutputSink.h"
#include "NLColumnKernels.h"
#include "NLLoopSteps.h"
#include "NLPendingRows.h"
#include "NLSortOrder.h"
#include "NLTaggedCells.h"

#include "LocalMemory.h"
#include "IRException.h"
#include "BioAssert.h"
#include "Overloaded.h"

using namespace db;

namespace {

// The step row an untagged mark, tally or collect answers for: the position its row chunk
// holds, or the single row of the step when it has none
uint64_t untaggedRow(const ColumnVector<uint64_t>* row) {
    return row ? row->getRaw().front() : 0;
}

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
struct BinaryOpTraits<OP_CONCAT> {
    using Functor = Concat;
};

template <>
struct BinaryOpTraits<OP_INDEX> {
    using Functor = ListIndex;

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
struct BinaryOpTraits<OP_NOT_EQUAL> {
    using Functor = Ne;

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
struct BinaryOpTraits<OP_MOD> {
    using Functor = Mod;

    template <typename ResCol, typename LhsCol, typename RhsCol>
    static void exec(ResCol* result, const LhsCol* lhs, const RhsCol* rhs) {
        BinaryOperators::exec<Functor>(result, lhs, rhs);
    }
};

template <>
struct BinaryOpTraits<OP_POW> {
    using Functor = Pow;

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

template <>
struct BinaryOpTraits<OP_XOR> {
    using Functor = Xor;

    template <typename ResCol, typename LhsCol, typename RhsCol>
    static void exec(ResCol* result, const LhsCol* lhs, const RhsCol* rhs) {
        BinaryPredicates::exec<Functor>(result, lhs, rhs);
    }
};

template <>
struct BinaryOpTraits<OP_STARTS_WITH> {
    using Functor = StartsWith;

    template <typename ResCol, typename LhsCol, typename RhsCol>
    static void exec(ResCol* result, const LhsCol* lhs, const RhsCol* rhs) {
        BinaryPredicates::exec<Functor>(result, lhs, rhs);
    }
};

template <>
struct BinaryOpTraits<OP_ENDS_WITH> {
    using Functor = EndsWith;

    template <typename ResCol, typename LhsCol, typename RhsCol>
    static void exec(ResCol* result, const LhsCol* lhs, const RhsCol* rhs) {
        BinaryPredicates::exec<Functor>(result, lhs, rhs);
    }
};

template <>
struct BinaryOpTraits<OP_CONTAINS> {
    using Functor = Contains;

    template <typename ResCol, typename LhsCol, typename RhsCol>
    static void exec(ResCol* result, const LhsCol* lhs, const RhsCol* rhs) {
        BinaryPredicates::exec<Functor>(result, lhs, rhs);
    }
};

template <>
struct BinaryOpTraits<OP_IN> {
    using Functor = In;

    template <typename ResCol, typename LhsCol, typename RhsCol>
    static void exec(ResCol* result, const LhsCol* lhs, const RhsCol* rhs) {
        BinaryPredicates::exec<Functor>(result, lhs, rhs);
    }
};

template <>
struct BinaryOpTraits<OP_INDEX_OF> {
    using Functor = IndexOf;

    template <typename ResCol, typename LhsCol, typename RhsCol>
    static void exec(ResCol* result, const LhsCol* lhs, const RhsCol* rhs) {
        BinaryOperators::exec<Functor>(result, lhs, rhs);
    }
};

template <>
struct BinaryOpTraits<OP_FUNC_COSINE_SIMILARITY> {
    using Functor = CosineSimilarity;

    template <typename ResCol, typename LhsCol, typename RhsCol>
    static void exec(ResCol* result, const LhsCol* lhs, const RhsCol* rhs) {
        BinaryOperators::exec<Functor>(result, lhs, rhs);
    }
};

template <>
struct BinaryOpTraits<OP_FUNC_EUCLIDEAN_DISTANCE> {
    using Functor = EuclideanDistance;

    template <typename ResCol, typename LhsCol, typename RhsCol>
    static void exec(ResCol* result, const LhsCol* lhs, const RhsCol* rhs) {
        BinaryOperators::exec<Functor>(result, lhs, rhs);
    }
};

template <ColumnOperator Op, typename ResCol, typename LhsCol, typename RhsCol>
struct BinaryOpKernel {
    static void run(Column* result, const Column* lhs, const Column* rhs, NLStepBuffers*) {
        BinaryOpTraits<Op>::exec(static_cast<ResCol*>(result),
                                 static_cast<const LhsCol*>(lhs),
                                 static_cast<const RhsCol*>(rhs));
    }
};

template <typename ResCol, typename LhsCol, typename RhsCol>
struct BinaryOpKernel<OP_CONCAT, ResCol, LhsCol, RhsCol> {
    static void run(Column* result, const Column* lhs, const Column* rhs, NLStepBuffers* mem) {
        BinaryOperators::exec<Concat>(static_cast<ResCol*>(result),
                                      static_cast<const LhsCol*>(lhs),
                                      static_cast<const RhsCol*>(rhs),
                                      Concat {&mem->stringBuffer(), &mem->listBuffer()});
    }
};

template <typename Op, typename ResCol, typename LhsCol, typename RhsCol>
void runArithmeticOp(Column* result, const Column* lhs, const Column* rhs, NLStepBuffers* mem) {
    BinaryOperators::exec<Op>(static_cast<ResCol*>(result),
                              static_cast<const LhsCol*>(lhs),
                              static_cast<const RhsCol*>(rhs),
                              Op {&mem->listBuffer(), &mem->stringBuffer()});
}

template <typename ResCol, typename LhsCol, typename RhsCol>
struct BinaryOpKernel<OP_ADD, ResCol, LhsCol, RhsCol> {
    static void run(Column* result, const Column* lhs, const Column* rhs, NLStepBuffers* mem) {
        runArithmeticOp<Add, ResCol, LhsCol, RhsCol>(result, lhs, rhs, mem);
    }
};

template <typename ResCol, typename LhsCol, typename RhsCol>
struct BinaryOpKernel<OP_SUB, ResCol, LhsCol, RhsCol> {
    static void run(Column* result, const Column* lhs, const Column* rhs, NLStepBuffers* mem) {
        runArithmeticOp<Sub, ResCol, LhsCol, RhsCol>(result, lhs, rhs, mem);
    }
};

template <typename ResCol, typename LhsCol, typename RhsCol>
struct BinaryOpKernel<OP_MUL, ResCol, LhsCol, RhsCol> {
    static void run(Column* result, const Column* lhs, const Column* rhs, NLStepBuffers* mem) {
        runArithmeticOp<Mul, ResCol, LhsCol, RhsCol>(result, lhs, rhs, mem);
    }
};

template <typename ResCol, typename LhsCol, typename RhsCol>
struct BinaryOpKernel<OP_DIV, ResCol, LhsCol, RhsCol> {
    static void run(Column* result, const Column* lhs, const Column* rhs, NLStepBuffers* mem) {
        runArithmeticOp<Div, ResCol, LhsCol, RhsCol>(result, lhs, rhs, mem);
    }
};

template <typename ResCol, typename LhsCol, typename RhsCol>
struct BinaryOpKernel<OP_MOD, ResCol, LhsCol, RhsCol> {
    static void run(Column* result, const Column* lhs, const Column* rhs, NLStepBuffers* mem) {
        runArithmeticOp<Mod, ResCol, LhsCol, RhsCol>(result, lhs, rhs, mem);
    }
};

template <ColumnOperator Op, typename ResCol, typename LhsCol, typename RhsCol>
void applyBinaryOp(Column* result, const Column* lhs, const Column* rhs, NLStepBuffers* mem) {
    BinaryOpKernel<Op, ResCol, LhsCol, RhsCol>::run(result, lhs, rhs, mem);
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

template <typename Element>
void gatherOptColumn(const Column* input,
                     const ColumnVector<size_t>* indices,
                     Column* output) {
    const ColumnOptVector<Element>* typedInput = static_cast<const ColumnOptVector<Element>*>(input);
    ColumnOptVector<Element>* typedOutput = static_cast<ColumnOptVector<Element>*>(output);

    const std::vector<size_t>& indicesRaw = indices->getRaw();
    const size_t rowCount = indicesRaw.size();
    const std::vector<Element>& inputValues = typedInput->getValues();

    resetForRowWrites(typedOutput, rowCount);
    std::vector<Element>& outputValues = typedOutput->getValues();

    for (size_t row = 0; row < rowCount; row++) {
        outputValues[row] = inputValues[indicesRaw[row]];
    }

    if (typedInput->getNullCount() == 0) {
        return;
    }

    for (size_t row = 0; row < rowCount; row++) {
        if (!typedInput->isValid(indicesRaw[row])) {
            typedOutput->setNull(row);
        }
    }
}

// Fill a chunk with a run of null rows. Every element type reads its own
// default-constructed value as null: an ID defaults to the invalid ID that ID::isValid
// rejects, which is how an entity an OPTIONAL MATCH did not match is spelled.
template <typename ElementType>
void fillNullColumn(Column* output, size_t rowCount) {
    ColumnVector<ElementType>* typedOutput = static_cast<ColumnVector<ElementType>*>(output);

    typedOutput->getRaw().assign(rowCount, ElementType {});
}

template <typename Element>
void fillNullOptColumn(Column* output, size_t rowCount) {
    ColumnOptVector<Element>* typedOutput = static_cast<ColumnOptVector<Element>*>(output);

    typedOutput->assignNulls(rowCount);
}

template <typename Element>
void dropAllValidOptMask(Column* output) {
    static_cast<ColumnOptVector<Element>*>(output)->dropAllValidMask();
}

// Block-repeat: the outer column of a cross product, where each outer row pairs
// with the whole inner chunk, so pair p takes input row p/factor. Fills the
// `rowCount` pairs from `position` on, which may start partway into one input
// row's block and end partway into another's.
template <typename ElementType>
void blockRepeatColumn(const Column* input, size_t factor, size_t position, size_t rowCount, Column* output) {
    const ColumnVector<ElementType>* typedInput = static_cast<const ColumnVector<ElementType>*>(input);
    ColumnVector<ElementType>* typedOutput = static_cast<ColumnVector<ElementType>*>(output);

    const auto& inputRaw = typedInput->getRaw();
    auto& outputRaw = typedOutput->getRaw();
    outputRaw.resize(rowCount);

    auto outputIt = outputRaw.begin();
    size_t rowsLeft = rowCount;
    size_t inputIndex = position / factor;
    size_t doneInBlock = position % factor;

    while (rowsLeft > 0 && inputIndex < inputRaw.size()) {
        const size_t count = std::min(factor - doneInBlock, rowsLeft);
        std::fill_n(outputIt, count, inputRaw[inputIndex]);

        outputIt += count;
        rowsLeft -= count;
        inputIndex++;
        doneInBlock = 0;
    }
}

template <typename Element>
void blockRepeatOptColumn(const Column* input, size_t factor, size_t position, size_t rowCount, Column* output) {
    const ColumnOptVector<Element>* typedInput = static_cast<const ColumnOptVector<Element>*>(input);
    ColumnOptVector<Element>* typedOutput = static_cast<ColumnOptVector<Element>*>(output);

    const size_t inputSize = typedInput->size();
    resetForRowWrites(typedOutput, rowCount);
    std::vector<Element>& outputValues = typedOutput->getValues();

    size_t outputRow = 0;
    size_t inputIndex = position / factor;
    size_t doneInBlock = position % factor;

    while (outputRow < rowCount && inputIndex < inputSize) {
        const size_t count = std::min(factor - doneInBlock, rowCount - outputRow);

        if (typedInput->isValid(inputIndex)) {
            std::fill_n(outputValues.begin() + outputRow, count, typedInput->getValue(inputIndex));
        } else {
            typedOutput->fillNull(outputRow, count);
        }

        outputRow += count;
        inputIndex++;
        doneInBlock = 0;
    }

    if (outputRow < rowCount) {
        typedOutput->fillNull(outputRow, rowCount - outputRow);
    }
}

void blockRepeatConstColumn(const Column* input, size_t factor, size_t position, size_t rowCount, Column* output) {
    output->assignFromLine(input, 0, rowCount);
}

// Tile: the inner column of a cross product, where the inner chunk repeats once
// per outer row, so pair p takes input row p % M. Fills the `rowCount` pairs from
// `position` on, which may start partway into one tile and end partway into
// another. M is the tile's own length, which is what the enclosing product passes
// as the factor.
template <typename ElementType>
void tileColumn(const Column* input, size_t factor, size_t position, size_t rowCount, Column* output) {
    const ColumnVector<ElementType>* typedInput = static_cast<const ColumnVector<ElementType>*>(input);
    ColumnVector<ElementType>* typedOutput = static_cast<ColumnVector<ElementType>*>(output);

    const auto& inputRaw = typedInput->getRaw();
    auto& outputRaw = typedOutput->getRaw();
    outputRaw.resize(rowCount);

    const size_t tileLength = inputRaw.size();
    if (tileLength == 0) {
        return;
    }

    auto outputIt = outputRaw.begin();
    size_t rowsLeft = rowCount;
    size_t inputIndex = position % tileLength;

    while (rowsLeft > 0) {
        const size_t count = std::min(tileLength - inputIndex, rowsLeft);
        outputIt = std::copy(inputRaw.begin() + inputIndex, inputRaw.begin() + inputIndex + count, outputIt);

        rowsLeft -= count;
        inputIndex = 0;
    }
}

template <typename Element>
void tileOptColumn(const Column* input, size_t factor, size_t position, size_t rowCount, Column* output) {
    const ColumnOptVector<Element>* typedInput = static_cast<const ColumnOptVector<Element>*>(input);
    ColumnOptVector<Element>* typedOutput = static_cast<ColumnOptVector<Element>*>(output);

    typedOutput->clear();

    const size_t tileLength = typedInput->size();
    if (tileLength == 0) {
        typedOutput->resize(rowCount);
        return;
    }

    typedOutput->reserve(rowCount);

    size_t rowsLeft = rowCount;
    size_t inputIndex = position % tileLength;

    while (rowsLeft > 0) {
        const size_t count = std::min(tileLength - inputIndex, rowsLeft);
        typedOutput->appendRows(*typedInput, inputIndex, count);

        rowsLeft -= count;
        inputIndex = 0;
    }
}

template <typename Primitive>
void layOutValue(ColumnOptVector<Primitive>* output, size_t rowCount, const Primitive& value) {
    resetForRowWrites(output, rowCount);
    std::fill_n(output->getValues().begin(), rowCount, value);
}

template <typename Primitive>
void layOutOptional(ColumnOptVector<Primitive>* output, size_t rowCount, const std::optional<Primitive>& value) {
    if (value.has_value()) {
        layOutValue(output, rowCount, *value);
    } else {
        output->assignNulls(rowCount);
    }
}

// Constant broadcast: the one value a ColumnConst holds is laid out over every row
// of the step, as a present value. The two broadcasts above repeat the several
// values of a chunk; a constant column has no rows of its own to repeat, so its row
// count comes from the driving relation and its value from the column itself.
template <typename Primitive>
void broadcastConstantColumn(const Column* value, size_t rowCount, Column* output) {
    const ColumnConst<Primitive>* typedValue = static_cast<const ColumnConst<Primitive>*>(value);
    ColumnOptVector<Primitive>* typedOutput = static_cast<ColumnOptVector<Primitive>*>(output);

    layOutValue(typedOutput, rowCount, typedValue->getRaw());
}

template <typename Primitive>
void broadcastNullableConstantColumn(const Column* value, size_t rowCount, Column* output) {
    const auto* typedValue = static_cast<const ColumnConst<std::optional<Primitive>>*>(value);
    ColumnOptVector<Primitive>* typedOutput = static_cast<ColumnOptVector<Primitive>*>(output);

    layOutOptional(typedOutput, rowCount, typedValue->get(0));
}

// A constant chunk is not always a ColumnConst: a kernel computing over literals writes
// its one row into an ordinary column. That row is the one every row of the step reads.
template <typename Primitive>
void broadcastSingleRowColumn(const Column* value, size_t rowCount, Column* output) {
    const ColumnOptVector<Primitive>* typedValue = static_cast<const ColumnOptVector<Primitive>*>(value);
    ColumnOptVector<Primitive>* typedOutput = static_cast<ColumnOptVector<Primitive>*>(output);

    const std::optional<Primitive> constant = typedValue->empty() ? std::nullopt : typedValue->get(0);
    layOutOptional(typedOutput, rowCount, constant);
}

// The null literal laid out over every row of the step: it holds no value to repeat, so
// each row is the absent value. An untyped null is carried as a null integer, which is
// the column the layout fills.
void broadcastNullColumn(const Column* value, size_t rowCount, Column* output) {
    ColumnOptVector<int64_t>* typedOutput = static_cast<ColumnOptVector<int64_t>*>(output);

    typedOutput->assignNulls(rowCount);
}

// The plain sibling of broadcastConstantColumn: a list, a map, a map entry, a node, an edge
// or a path carries no absence of its own as a value does, so the rows are a plain column
// of the one element the constant holds.
template <typename Element>
void broadcastConstantPlainColumn(const Column* value, size_t rowCount, Column* output) {
    const ColumnConst<Element>* typedValue = static_cast<const ColumnConst<Element>*>(value);
    ColumnVector<Element>* typedOutput = static_cast<ColumnVector<Element>*>(output);

    auto& outputRaw = typedOutput->getRaw();
    outputRaw.resize(rowCount);

    std::fill_n(outputRaw.begin(), rowCount, typedValue->getRaw());
}

// The one row a computation over constants wrote, held as the constant it stands for
template <typename Element>
void readSingleRowAsConstant(Column* result, const Column* row) {
    const std::vector<Element>& rowRaw = static_cast<const ColumnVector<Element>*>(row)->getRaw();
    bioassert(rowRaw.size() == 1, "A computation over constants did not write a single row.");

    static_cast<ColumnConst<Element>*>(result)->set(rowRaw.front());
}

template <typename Element>
NLUnaryFn selectRowConstantOf(const Column* row, LocalMemory* memory, Column*& result) {
    bioassert(row->getKind() == ColumnVector<Element>::staticKind(),
              "nl.row_constant reads a {} under a chunk of another element type",
              row->getTypeName());

    result = memory->alloc<ColumnConst<Element>>();
    return &readSingleRowAsConstant<Element>;
}

template <typename Element>
void readOptSingleRowAsConstant(Column* result, const Column* row) {
    const ColumnOptVector<Element>* typedRow = static_cast<const ColumnOptVector<Element>*>(row);
    bioassert(typedRow->size() == 1, "A computation over constants did not write a single row.");

    static_cast<ColumnConst<std::optional<Element>>*>(result)->setOptional(typedRow->get(0));
}

template <typename Element>
NLUnaryFn selectOptRowConstantOf(const Column* row, LocalMemory* memory, Column*& result) {
    bioassert(row->getKind() == ColumnOptVector<Element>::staticKind(),
              "nl.row_constant reads a {} under a chunk of another element type",
              row->getTypeName());

    result = memory->alloc<ColumnConst<std::optional<Element>>>();
    return &readOptSingleRowAsConstant<Element>;
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

template <typename Element>
void copyRangeOptColumn(const Column* input, size_t inputOffset, size_t rowCount, Column* output) {
    const ColumnOptVector<Element>* typedInput = static_cast<const ColumnOptVector<Element>*>(input);
    ColumnOptVector<Element>* typedOutput = static_cast<ColumnOptVector<Element>*>(output);

    typedOutput->clear();
    typedOutput->appendRows(*typedInput, inputOffset, rowCount);
}

void copyRangeConstColumn(const Column* input, size_t inputOffset, size_t rowCount, Column* output) {
    output->assignFromLine(input, inputOffset, rowCount);
}

void gatherConstColumn(const Column* input, const ColumnVector<size_t>* indices, Column* output) {
    output->assignFromLine(input, 0, indices->size());
}

// Append every row of an input chunk onto the tail of a growing buffer of the
// same element type. nl.sort_collect calls this once per producing-loop step, so
// the buffer accumulates every row across all chunks, row-aligned with the other
// buffers of the same accumulator.
template <typename ElementType>
void appendColumn(const Column* input, Column* buffer) {
    const ColumnVector<ElementType>* typedInput = static_cast<const ColumnVector<ElementType>*>(input);
    ColumnVector<ElementType>* typedBuffer = static_cast<ColumnVector<ElementType>*>(buffer);

    typedBuffer->appendRows(*typedInput, 0, typedInput->size());
}

template <typename Element>
void appendOptColumn(const Column* input, Column* buffer) {
    const ColumnOptVector<Element>* typedInput = static_cast<const ColumnOptVector<Element>*>(input);
    ColumnOptVector<Element>* typedBuffer = static_cast<ColumnOptVector<Element>*>(buffer);

    typedBuffer->appendRows(*typedInput, 0, typedInput->size());
}

// The append of a column of views, for a buffer that outlives the loop filling it: the
// views a chunk carries point into the buffers that built them, which the producing step
// empties when it comes round again, so the values themselves go into the accumulator's own.
template <typename ElementType>
void appendOwnedColumn(const Column* input, Column* buffer, NLValueStore& values) {
    const ColumnVector<ElementType>* typedInput = static_cast<const ColumnVector<ElementType>*>(input);
    ColumnVector<ElementType>* typedBuffer = static_cast<ColumnVector<ElementType>*>(buffer);

    const auto& inputRaw = typedInput->getRaw();
    auto& bufferRaw = typedBuffer->getRaw();

    const size_t firstRow = bufferRaw.size();
    bufferRaw.resize(firstRow + inputRaw.size());

    for (size_t row = 0; row < inputRaw.size(); row++) {
        bufferRaw[firstRow + row] = values.ownElement(inputRaw[row]);
    }
}

template <typename Element>
void appendOwnedOptColumn(const Column* input, Column* buffer, NLValueStore& values) {
    const ColumnOptVector<Element>* typedInput = static_cast<const ColumnOptVector<Element>*>(input);
    ColumnOptVector<Element>* typedBuffer = static_cast<ColumnOptVector<Element>*>(buffer);

    const size_t firstRow = typedBuffer->size();
    typedBuffer->appendRows(*typedInput, 0, typedInput->size());
    values.ownRows(typedBuffer, firstRow);
}

// 3-way compare two rows of a non-null orderable column (an ID column, or a plain scalar
// a procedure yielded): negative if row a sorts before row b, positive if after, zero if
// they are equal.
template <typename ElementType>
int compareColumn(const Column* column, size_t a, size_t b) {
    const auto& raw = static_cast<const ColumnVector<ElementType>*>(column)->getRaw();
    const ElementType& valueA = raw[a];
    const ElementType& valueB = raw[b];

    if (sortsBefore(valueA, valueB)) {
        return -1;
    } else if (sortsBefore(valueB, valueA)) {
        return 1;
    }

    return 0;
}

// 3-way compare two rows of a nullable value column. A null sorts after every
// value, so an ascending order places nulls last (matching Cypher's ORDER BY),
// and two nulls tie; non-null values compare by their natural order.
template <typename Primitive>
int compareOptColumn(const Column* column, size_t a, size_t b) {
    const ColumnOptVector<Primitive>* typedColumn = static_cast<const ColumnOptVector<Primitive>*>(column);

    const bool aNull = !typedColumn->isValid(a);
    const bool bNull = !typedColumn->isValid(b);
    if (aNull || bNull) {
        if (aNull && bNull) {
            return 0;
        }

        return aNull ? 1 : -1;
    }

    const Primitive& valueA = typedColumn->getValue(a);
    const Primitive& valueB = typedColumn->getValue(b);

    if (sortsBefore(valueA, valueB)) {
        return -1;
    } else if (sortsBefore(valueB, valueA)) {
        return 1;
    }

    return 0;
}

// Copy a plain value column into a nullable one with every row present (nl.to_nullable),
// so a kernel reading a nullable value column takes a column a procedure yielded.
template <typename Primitive>
void toNullableColumn(Column* result, const Column* operand) {
    const std::vector<Primitive>& values = static_cast<const ColumnVector<Primitive>*>(operand)->getRaw();
    ColumnOptVector<Primitive>* nullables = static_cast<ColumnOptVector<Primitive>*>(result);

    resetForRowWrites(nullables, values.size());
    std::copy(values.begin(), values.end(), nullables->getValues().begin());
}

// Read an entity column as a nullable column of its IDs' integers: a node or an edge an
// OPTIONAL MATCH did not match carries an invalid ID, which is the null. The entity
// sibling of toNullableColumn, for the column family whose null is not an absent optional.
template <typename ID>
void entityToNullableColumn(Column* result, const Column* operand) {
    const std::vector<ID>& ids = static_cast<const ColumnVector<ID>*>(operand)->getRaw();
    ColumnOptVector<types::UInt64::Primitive>* nullables = static_cast<ColumnOptVector<types::UInt64::Primitive>*>(result);

    resetForRowWrites(nullables, ids.size());
    std::vector<types::UInt64::Primitive>& values = nullables->getValues();

    for (size_t row = 0; row < ids.size(); row++) {
        if (ids[row].isValid()) {
            values[row] = ids[row].getValue();
        } else {
            nullables->setNull(row);
        }
    }
}

template <typename ID>
ID checkedEntity(const CellView cell) {
    const CellTag tag = cell.getTag();

    if (tag == TypeToCellTag<ID>::Tag) {
        return cell.getAs<ID>();
    } else if (tag == CellTag::Null) {
        return ID {};
    }

    constexpr std::string_view ENTITY = std::is_same_v<ID, NodeID> ? "a node" : "a relationship";
    throw IRException(fmt::format("A pattern reads {}, not '{}'", ENTITY, queryTypeName(tag)));
}

template <typename ID>
void checkedCastColumn(Column* result, const Column* operand) {
    const std::vector<CellView>& cells = static_cast<const ColumnVector<CellView>*>(operand)->getRaw();
    std::vector<ID>& ids = static_cast<ColumnVector<ID>*>(result)->getRaw();

    ids.resize(cells.size());
    std::transform(cells.begin(), cells.end(), ids.begin(), [](const CellView cell) {
        return checkedEntity<ID>(cell);
    });
}

template <typename ID>
NLUnaryFn selectCheckedCastOf(const Column* operand, LocalMemory* memory, Column*& result) {
    const ColumnKind::Code kind = operand->getKind();

    if (kind == ColumnVector<CellView>::staticKind()) {
        result = memory->alloc<ColumnVector<ID>>();
        return &checkedCastColumn<ID>;
    }

    throw IRException("A pattern reads its node or relationship out of a column of tagged cells laid out over its rows");
}

template <typename Primitive>
void toNullableConst(Column* result, const Column* operand) {
    const auto* constant = static_cast<const ColumnConst<Primitive>*>(operand);
    auto* nullable = static_cast<ColumnConst<std::optional<Primitive>>*>(result);

    if (constant->empty()) {
        nullable->clear();
        return;
    }

    nullable->setOptional((*constant)[0]);
}

template <typename Primitive>
NLUnaryFn selectToNullableOf(const Column* operand, LocalMemory* memory, Column*& result) {
    if (operand->getContainerKind() == ContainerKind::code<ColumnConst>()) {
        result = memory->alloc<ColumnConst<std::optional<Primitive>>>();
        return &toNullableConst<Primitive>;
    }

    result = memory->alloc<ColumnOptVector<Primitive>>();
    return &toNullableColumn<Primitive>;
}

template <typename ElementType, bool TESTS_NULL>
void applyNullTestOnVector(Column* result, const Column* operand) {
    const std::vector<ElementType>& operandRaw = static_cast<const ColumnVector<ElementType>*>(operand)->getRaw();
    std::vector<CustomBool>& resultRaw = static_cast<ColumnVector<CustomBool>*>(result)->getRaw();

    resultRaw.resize(operandRaw.size());
    std::transform(operandRaw.begin(), operandRaw.end(), resultRaw.begin(), [](const ElementType& value) {
        return CustomBool {holdsAValue(value) != TESTS_NULL};
    });
}

template <typename Element, bool TESTS_NULL>
void applyNullTestOnOptVector(Column* result, const Column* operand) {
    NullablePredicateExecutor::testNull<TESTS_NULL>(static_cast<ColumnVector<CustomBool>*>(result),
                                                    static_cast<const ColumnOptVector<Element>*>(operand));
}

template <typename ElementType, bool TESTS_NULL>
void applyNullTestOnConst(Column* result, const Column* operand) {
    const ElementType& value = static_cast<const ColumnConst<ElementType>*>(operand)->getRaw();
    static_cast<ColumnConst<CustomBool>*>(result)->set(CustomBool {holdsAValue(value) != TESTS_NULL});
}

template <typename Element, bool TESTS_NULL>
void applyNullTestOnOptConst(Column* result, const Column* operand) {
    const ColumnConst<std::optional<Element>>* typedOperand = static_cast<const ColumnConst<std::optional<Element>>*>(operand);

    const bool holds = typedOperand->isValid(0) && holdsAValue(typedOperand->getValue(0));
    static_cast<ColumnConst<CustomBool>*>(result)->set(CustomBool {holds != TESTS_NULL});
}

template <bool TESTS_NULL>
struct NullTestSelector {
    LocalMemory* _memory {nullptr};
    NLUnaryFn _fn {nullptr};
    Column* _result {nullptr};

    template <typename ElementType>
    void operator()(const ColumnVector<ElementType>* operand) {
        _result = _memory->alloc<ColumnVector<CustomBool>>();
        _fn = &applyNullTestOnVector<ElementType, TESTS_NULL>;
    }

    template <typename Element>
    void operator()(const ColumnOptVector<Element>* operand) {
        _result = _memory->alloc<ColumnVector<CustomBool>>();
        _fn = &applyNullTestOnOptVector<Element, TESTS_NULL>;
    }

    template <typename ElementType>
    void operator()(const ColumnConst<ElementType>* operand) {
        _result = _memory->alloc<ColumnConst<CustomBool>>();
        _fn = &applyNullTestOnConst<ElementType, TESTS_NULL>;
    }

    template <typename Element>
    void operator()(const ColumnConst<std::optional<Element>>* operand) {
        _result = _memory->alloc<ColumnConst<CustomBool>>();
        _fn = &applyNullTestOnOptConst<Element, TESTS_NULL>;
    }

    template <typename ElementType>
    void operator()(const ColumnSet<ElementType>* operand) {
        throw IRException("A set column holds no rows to test for null");
    }
};

template <typename Primitive>
void caseWriteOptCell(Column* result, size_t resultRow, const Column* value, size_t valueRow) {
    const ColumnOptVector<Primitive>* values = static_cast<const ColumnOptVector<Primitive>*>(value);
    static_cast<ColumnOptVector<Primitive>*>(result)->setOptional(resultRow, values->get(valueRow));
}

template <typename Primitive>
void caseWritePlainCell(Column* result, size_t resultRow, const Column* value, size_t valueRow) {
    const ColumnVector<Primitive>* values = static_cast<const ColumnVector<Primitive>*>(value);
    static_cast<ColumnOptVector<Primitive>*>(result)->set(resultRow, (*values)[valueRow]);
}

template <typename Primitive>
void caseWriteNull(Column* result, size_t resultRow, const Column* value, size_t valueRow) {
    static_cast<ColumnOptVector<Primitive>*>(result)->setNull(resultRow);
}

template <typename Element>
void caseWriteOwnNull(Column* result, size_t resultRow, const Column* value, size_t valueRow) {
    static_cast<ColumnVector<Element>*>(result)->getRaw()[resultRow] = Element {};
}

void caseWriteNullTaggedCell(Column* result, size_t resultRow, const Column* value, size_t valueRow) {
    static_cast<ColumnVector<CellView>*>(result)->getRaw()[resultRow] = CellView::nullElement();
}

template <typename Element>
void caseWriteOwnNullCell(Column* result, size_t resultRow, const Column* value, size_t valueRow) {
    std::vector<Element>& results = static_cast<ColumnVector<Element>*>(result)->getRaw();
    results[resultRow] = static_cast<const ColumnVector<Element>*>(value)->getRaw()[valueRow];
}

template <typename Element>
void fillNullRows(Column* result, const Column* value, const ColumnVector<CustomBool>* nullRows, const Column* fill) {
    const std::vector<Element>& values = static_cast<const ColumnVector<Element>*>(value)->getRaw();
    const std::vector<CustomBool>& nulls = nullRows->getRaw();
    const std::vector<Element>& fills = static_cast<const ColumnVector<Element>*>(fill)->getRaw();
    std::vector<Element>& results = static_cast<ColumnVector<Element>*>(result)->getRaw();

    bioassert(nulls.size() == values.size(), "A value's null rows are marked over other rows than its own.");

    results.resize(values.size());

    const auto isNull = [](CustomBool row) {
        return static_cast<bool>(row);
    };

    size_t row = 0;
    auto fillIt = fills.begin();
    auto resultIt = results.begin();

    while (row < values.size()) {
        const size_t nullStart = static_cast<size_t>(std::find_if(nulls.begin() + row, nulls.end(), isNull) - nulls.begin());
        resultIt = std::copy(values.begin() + row, values.begin() + nullStart, resultIt);

        const size_t nullEnd = static_cast<size_t>(std::find_if_not(nulls.begin() + nullStart, nulls.end(), isNull) - nulls.begin());
        const size_t nullCount = nullEnd - nullStart;
        const size_t fillsLeft = static_cast<size_t>(fills.end() - fillIt);
        bioassert(nullCount <= fillsLeft, "A value has more null rows than the rows filling them.");

        resultIt = std::copy_n(fillIt, nullCount, resultIt);
        fillIt += nullCount;
        row = nullEnd;
    }

    bioassert(fillIt == fills.end(), "A value has fewer null rows than the rows filling them.");
}

template <typename Element>
void fillNullOptRows(Column* result, const Column* value, const ColumnVector<CustomBool>* nullRows, const Column* fill) {
    const ColumnOptVector<Element>* values = static_cast<const ColumnOptVector<Element>*>(value);
    const std::vector<CustomBool>& nulls = nullRows->getRaw();
    const ColumnOptVector<Element>* fills = static_cast<const ColumnOptVector<Element>*>(fill);
    ColumnOptVector<Element>* results = static_cast<ColumnOptVector<Element>*>(result);

    const size_t rowCount = values->size();
    const size_t fillCount = fills->size();
    bioassert(nulls.size() == rowCount, "A value's null rows are marked over other rows than its own.");

    results->clear();
    results->reserve(rowCount);

    const auto isNull = [](CustomBool row) {
        return static_cast<bool>(row);
    };

    size_t row = 0;
    size_t fillRow = 0;

    while (row < rowCount) {
        const size_t nullStart = static_cast<size_t>(std::find_if(nulls.begin() + row, nulls.end(), isNull) - nulls.begin());
        results->appendRows(*values, row, nullStart - row);

        const size_t nullEnd = static_cast<size_t>(std::find_if_not(nulls.begin() + nullStart, nulls.end(), isNull) - nulls.begin());
        const size_t nullCount = nullEnd - nullStart;
        bioassert(nullCount <= fillCount - fillRow, "A value has more null rows than the rows filling them.");

        results->appendRows(*fills, fillRow, nullCount);
        fillRow += nullCount;
        row = nullEnd;
    }

    bioassert(fillRow == fillCount, "A value has fewer null rows than the rows filling them.");
}

void resetTaggedCells(Column* result, size_t rowCount) {
    std::vector<CellView>& results = static_cast<ColumnVector<CellView>*>(result)->getRaw();
    results.assign(rowCount, CellView::nullElement());
}

// 3-way compare two rows of a type-erased column of tagged scalars. Cells need not share
// a type, so a pair of different types compares by the order those types sort in; nulls
// tie and sort after every value, as they do in a nullable value column.
int compareCellColumn(const Column* column, size_t a, size_t b) {
    const auto& raw = static_cast<const ColumnVector<CellView>*>(column)->getRaw();
    const std::strong_ordering order = raw[a] <=> raw[b];

    if (order == std::strong_ordering::less) {
        return -1;
    } else if (order == std::strong_ordering::greater) {
        return 1;
    }

    return 0;
}

// 3-way compare two rows of a list or a map column. Two lists order lexicographically
// on the element order above, and two maps by size, then keys, then values, so either can
// be the key the rows are sorted on.
template <typename View>
int compareContainerColumn(const Column* column, size_t a, size_t b) {
    const auto& raw = static_cast<const ColumnVector<View>*>(column)->getRaw();
    const std::strong_ordering order = raw[a] <=> raw[b];

    if (order == std::strong_ordering::less) {
        return -1;
    } else if (order == std::strong_ordering::greater) {
        return 1;
    }

    return 0;
}

// 3-way compare two rows of a path column. The empty path a null is spelled as sorts after
// every path, as compareOptColumn places a null after every value.
int comparePathColumn(const Column* column, size_t a, size_t b) {
    const std::vector<GraphPath>& raw = static_cast<const ColumnVector<GraphPath>*>(column)->getRaw();
    const GraphPath& pathA = raw[a];
    const GraphPath& pathB = raw[b];

    const bool aNull = pathA.isNull();
    const bool bNull = pathB.isNull();
    if (aNull || bNull) {
        if (aNull && bNull) {
            return 0;
        }

        return aNull ? 1 : -1;
    }

    const std::strong_ordering order = PathView {pathA.getIDs()} <=> PathView {pathB.getIDs()};

    if (order == std::strong_ordering::less) {
        return -1;
    } else if (order == std::strong_ordering::greater) {
        return 1;
    }

    return 0;
}

// The nullable sibling, for a list or a map read out of a property: a row holding none
// sorts after every value, as compareOptColumn places a null after every value.
template <typename View>
int compareOptContainerColumn(const Column* column, size_t a, size_t b) {
    const ColumnOptVector<View>* typedColumn = static_cast<const ColumnOptVector<View>*>(column);

    const bool aNull = !typedColumn->isValid(a);
    const bool bNull = !typedColumn->isValid(b);
    if (aNull || bNull) {
        if (aNull && bNull) {
            return 0;
        }

        return aNull ? 1 : -1;
    }

    const std::strong_ordering order = typedColumn->getValue(a) <=> typedColumn->getValue(b);

    if (order == std::strong_ordering::less) {
        return -1;
    } else if (order == std::strong_ordering::greater) {
        return 1;
    }

    return 0;
}

// Append the raw bytes of a present property value to a distinct row key. The value
// is appended verbatim: a trivially-copyable primitive copies its object bytes; a
// string copies a length prefix then its characters, so two rows never collide by
// concatenation (so "a"+"b" and "ab"+"" get distinct keys). A view - a list, a map - would
// key on its own bytes, a pointer into its buffer, so it keys through its own serializer.
template <typename Primitive>
    requires (!NLViewElement<Primitive>)
void distinctAppendValueBytes(NLSerializedKey& key, const Primitive& value) {
    key.append(reinterpret_cast<const char*>(&value), sizeof(Primitive));
}

// A float keys by its Cypher value, not its bit pattern: -0.0 is +0.0 (0.0 == -0.0 in
// Cypher) and every NaN payload maps to one canonical NaN, so two distinct bit patterns of
// the same value never split into separate groups (or survive DISTINCT as two rows).
template <std::floating_point Float>
Float normalizedKeyFloat(const Float value) {
    if (value == 0) {
        return 0;
    } else if (std::isnan(value)) {
        return std::numeric_limits<Float>::quiet_NaN();
    }

    return value;
}

template <std::floating_point Float>
void distinctAppendFloatBytes(NLSerializedKey& key, const Float value) {
    const Float normalized = normalizedKeyFloat(value);
    key.append(reinterpret_cast<const char*>(&normalized), sizeof(normalized));
}

void distinctAppendValueBytes(NLSerializedKey& key, double value) {
    distinctAppendFloatBytes(key, value);
}

void distinctAppendValueBytes(NLSerializedKey& key, std::string_view value) {
    const size_t length = value.size();
    key.append(reinterpret_cast<const char*>(&length), sizeof(length));
    key.append(value.data(), value.size());
}

void distinctAppendValueBytes(NLSerializedKey& key, const types::Embedding::Primitive value) {
    const size_t length = value.size();
    key.append(reinterpret_cast<const char*>(&length), sizeof(length));

    for (const float element : value) {
        distinctAppendFloatBytes(key, element);
    }
}

// Serialize one row of an ID column (node/edge/edge-type IDs) into the row key as
// the ID's underlying integer value, byte for byte.
template <typename ElementType>
void distinctKeyAppendColumn(const Column* column, size_t row, NLSerializedKey& key) {
    const auto& raw = static_cast<const ColumnVector<ElementType>*>(column)->getRaw();
    const auto value = raw[row].getValue();
    distinctAppendValueBytes(key, value);
}

// Serialize one row of a chunk that holds its values plainly - a column a procedure
// yielded, a tally, an expression over one - into the row key. The ID sibling reads
// through the ID's integer; here the element is the value, and no row of such a chunk is
// null, so the key carries no tag byte to tell a null from a value.
template <typename ElementType>
void distinctKeyAppendPlainColumn(const Column* column, size_t row, NLSerializedKey& key) {
    const auto& raw = static_cast<const ColumnVector<ElementType>*>(column)->getRaw();
    distinctAppendValueBytes(key, raw[row]);
}

void distinctAppendListBytes(NLSerializedKey& key, const ListView list);
void distinctAppendMapBytes(NLSerializedKey& key, const MapView map);

template <typename Primitive>
void distinctAppendPresentValue(NLSerializedKey& key, const Primitive& value) {
    key.push_back('\1');
    distinctAppendValueBytes(key, value);
}

void distinctAppendPresentValue(NLSerializedKey& key, const ListView list) {
    distinctAppendListBytes(key, list);
}

void distinctAppendPresentValue(NLSerializedKey& key, const MapView map) {
    distinctAppendMapBytes(key, map);
}

// Serialize one row of a nullable value column into the row key as a tag byte telling
// null from present, then, when present, the value's bytes. A null serializes to the
// tag alone, so all nulls share a key and DISTINCT dedups them together, matching Cypher.
template <typename Primitive>
void distinctKeyAppendOptColumn(const Column* column, size_t row, NLSerializedKey& key) {
    const ColumnOptVector<Primitive>* typedColumn = static_cast<const ColumnOptVector<Primitive>*>(column);

    if (!typedColumn->isValid(row)) {
        key.push_back('\0');
        return;
    }

    distinctAppendPresentValue(key, typedColumn->getValues()[row]);
}

// What a key value packs as, holding the value as its serialized key does: an ID's integer, a
// number's own bits, a double's once normalized, a boolean's byte, the microseconds of a
// datetime or a duration, and a point's own bytes
template <IntegralType T, int I>
T packedKeyValue(ID<T, I> id) {
    return id.getValue();
}

types::Int64::Primitive packedKeyValue(types::Int64::Primitive value) {
    return value;
}

types::UInt64::Primitive packedKeyValue(types::UInt64::Primitive value) {
    return value;
}

uint64_t packedKeyValue(types::Double::Primitive value) {
    return std::bit_cast<uint64_t>(normalizedKeyFloat(value));
}

uint8_t packedKeyValue(types::Bool::Primitive value) {
    return value._boolean;
}

uint8_t packedKeyValue(ValueType value) {
    return static_cast<uint8_t>(value);
}

int64_t packedKeyValue(types::DateTime::Primitive value) {
    return value.getMicroseconds();
}

int64_t packedKeyValue(types::Duration::Primitive value) {
    return value.getMicroseconds();
}

types::Point::Primitive packedKeyValue(types::Point::Primitive value) {
    return value;
}

template <typename ElementType>
void packKeyColumn(const Column* column, size_t beginRow, size_t endRow, const NLKeyPackTarget& target) {
    const auto& raw = static_cast<const ColumnVector<ElementType>*>(column)->getRaw();
    std::byte* value = target._values;

    for (size_t row = beginRow; row < endRow; row++) {
        const auto packed = packedKeyValue(raw[row]);
        std::memcpy(value, &packed, sizeof(packed));

        value += target._valueStride;
    }
}

template <typename Primitive>
void packOptKeyColumn(const Column* column, size_t beginRow, size_t endRow, const NLKeyPackTarget& target) {
    const ColumnOptVector<Primitive>* typedColumn = static_cast<const ColumnOptVector<Primitive>*>(column);
    const std::vector<Primitive>& values = typedColumn->getValues();
    const std::byte nullMask {static_cast<uint8_t>(1u << (target._nullBit % 8))};

    const auto markNulls = [&](size_t first, size_t last) {
        std::byte* nulls = target._nulls + target._nullBit / 8 + (first - beginRow) * target._nullStride;

        for (size_t row = first; row < last; row++) {
            *nulls |= nullMask;
            nulls += target._nullStride;
        }
    };

    size_t nullFirst = beginRow;
    typedColumn->forEachPresentRun(beginRow, endRow, [&](size_t first, size_t last) {
        markNulls(nullFirst, first);

        std::byte* value = target._values + (first - beginRow) * target._valueStride;
        for (size_t row = first; row < last; row++) {
            const auto packed = packedKeyValue(values[row]);
            std::memcpy(value, &packed, sizeof(packed));

            value += target._valueStride;
        }

        nullFirst = last;
    });

    markNulls(nullFirst, endRow);
}

template <typename ElementType>
NLKeyPacking keyPacking() {
    using Packed = decltype(packedKeyValue(std::declval<ElementType>()));

    return {&packKeyColumn<ElementType>, sizeof(Packed)};
}

template <typename Primitive>
NLKeyPacking optKeyPacking() {
    using Packed = decltype(packedKeyValue(std::declval<Primitive>()));

    return {&packOptKeyColumn<Primitive>, sizeof(Packed)};
}

void distinctAppendElementBytes(NLSerializedKey& key, CellView element);

// The count fixes how many IDs follow it, so they need no separator.
void distinctAppendPathBytes(NLSerializedKey& key, std::span<const uint64_t> ids) {
    const size_t idCount = ids.size();
    key.append(reinterpret_cast<const char*>(&idCount), sizeof(idCount));
    key.append(reinterpret_cast<const char*>(ids.data()), ids.size_bytes());
}

// Serialize a nested list into the row key as its length then its elements, so a list
// never keys the same as the concatenation of its neighbours.
void distinctAppendListBytes(NLSerializedKey& key, const ListView list) {
    const size_t size = list.size();
    key.append(reinterpret_cast<const char*>(&size), sizeof(size));

    for (const CellView element : list) {
        distinctAppendElementBytes(key, element);
    }
}

// Serialize a number by its value rather than by the type it is tagged with: an integer
// and a float holding the same value are one Cypher value, so a float with no fractional
// part keys as that integer and only a fractional one keys as a double.
// A number keys under the Int tag whenever it holds one, so 1, 1.0 and a small unsigned 1
// all key alike - which is what makes the key agree with numeric equality.
void distinctAppendIntegerKey(NLSerializedKey& key, const types::Int64::Primitive value) {
    key.push_back(static_cast<char>(CellTag::Int));
    distinctAppendValueBytes(key, value);
}

void distinctAppendNumberKey(NLSerializedKey& key, const types::Int64::Primitive value) {
    distinctAppendIntegerKey(key, value);
}

void distinctAppendNumberKey(NLSerializedKey& key, const types::UInt64::Primitive value) {
    if (value <= static_cast<types::UInt64::Primitive>(std::numeric_limits<types::Int64::Primitive>::max())) {
        distinctAppendIntegerKey(key, static_cast<types::Int64::Primitive>(value));
        return;
    }

    key.push_back(static_cast<char>(CellTag::UInt));
    distinctAppendValueBytes(key, value);
}

void distinctAppendNumberKey(NLSerializedKey& key, const types::Double::Primitive value) {
    constexpr types::Double::Primitive integerBound = 9223372036854775808.0;
    constexpr types::Double::Primitive unsignedBound = 18446744073709551616.0;

    const bool holdsAWholeNumber = std::isfinite(value) && std::trunc(value) == value;
    const bool holdsAnInteger = holdsAWholeNumber && value >= -integerBound && value < integerBound;
    const bool holdsAnUnsigned = holdsAWholeNumber && value >= integerBound && value < unsignedBound;

    if (holdsAnInteger) {
        distinctAppendIntegerKey(key, static_cast<types::Int64::Primitive>(value));
    } else if (holdsAnUnsigned) {
        distinctAppendNumberKey(key, static_cast<types::UInt64::Primitive>(value));
    } else {
        key.push_back(static_cast<char>(CellTag::Double));
        distinctAppendValueBytes(key, value);
    }
}

void distinctAppendNumberBytes(NLSerializedKey& key, const CellView element) {
    switch (element.getTag()) {
        case CellTag::Int:
            return distinctAppendNumberKey(key, element.getAs<types::Int64::Primitive>());
        break;

        case CellTag::UInt:
            return distinctAppendNumberKey(key, element.getAs<types::UInt64::Primitive>());
        break;

        case CellTag::Double:
            return distinctAppendNumberKey(key, element.getAs<types::Double::Primitive>());
        break;

        default:
            bioassert(false, "Keying a non-numeric element as a number");
        break;
    }
}

// Serialize a map into the row key as its size, then each entry's key length, key bytes and
// value. Both producers store entries in key order, so equal maps serialize alike.
void distinctAppendMapBytes(NLSerializedKey& key, const MapView map) {
    const size_t size = map.size();
    key.append(reinterpret_cast<const char*>(&size), sizeof(size));

    for (const MapEntryView entry : map) {
        const std::string_view entryKey = entry.getKey();
        const size_t keySize = entryKey.size();
        key.append(reinterpret_cast<const char*>(&keySize), sizeof(keySize));
        key.append(entryKey);

        distinctAppendElementBytes(key, entry.getValue());
    }
}

// Serialize one tagged scalar into the row key: its tag, then its value's bytes. Cells of
// two types never collide because the tag leads, and a null is the tag alone, so all
// nulls dedup together as they do in a nullable value column.
void distinctAppendElementBytes(NLSerializedKey& key, const CellView element) {
    const CellTag tag = element.getTag();

    switch (tag) {
        case CellTag::Int:
        case CellTag::UInt:
        case CellTag::Double:
            return distinctAppendNumberBytes(key, element);
        break;

        case CellTag::Bool:
            key.push_back(static_cast<char>(tag));
            distinctAppendValueBytes(key, static_cast<bool>(element.getAs<types::Bool::Primitive>()));
            return;
        break;

        case CellTag::String:
            key.push_back(static_cast<char>(tag));
            distinctAppendValueBytes(key, element.getAs<types::String::Primitive>());
            return;
        break;

        case CellTag::ListView:
            key.push_back(static_cast<char>(tag));
            distinctAppendListBytes(key, element.getAs<ListView>());
            return;
        break;

        case CellTag::Null:
            key.push_back(static_cast<char>(tag));
            return;
        break;

        case CellTag::NodeID:
            key.push_back(static_cast<char>(tag));
            distinctAppendValueBytes(key, element.getAs<NodeID>().getValue());
            return;
        break;

        case CellTag::EdgeID:
            key.push_back(static_cast<char>(tag));
            distinctAppendValueBytes(key, element.getAs<EdgeID>().getValue());
            return;
        break;

        case CellTag::DateTime:
            key.push_back(static_cast<char>(tag));
            distinctAppendValueBytes(key, element.getAs<types::DateTime::Primitive>().getMicroseconds());
            return;
        break;

        case CellTag::Duration:
            key.push_back(static_cast<char>(tag));
            distinctAppendValueBytes(key, element.getAs<types::Duration::Primitive>().getMicroseconds());
            return;
        break;

        case CellTag::Point:
            key.push_back(static_cast<char>(tag));
            distinctAppendValueBytes(key, element.getAs<types::Point::Primitive>());
            return;
        break;

        case CellTag::LabelID:
            key.push_back(static_cast<char>(tag));
            distinctAppendValueBytes(key, element.getAs<LabelID>().getValue());
            return;
        break;

        case CellTag::EdgeTypeID:
            key.push_back(static_cast<char>(tag));
            distinctAppendValueBytes(key, element.getAs<EdgeTypeID>().getValue());
            return;
        break;

        case CellTag::PropertyTypeID:
            key.push_back(static_cast<char>(tag));
            distinctAppendValueBytes(key, element.getAs<PropertyTypeID>().getValue());
            return;
        break;

        case CellTag::ValueType:
            key.push_back(static_cast<char>(tag));
            distinctAppendValueBytes(key, element.getAs<ValueType>());
            return;
        break;

        case CellTag::Embedding:
            key.push_back(static_cast<char>(tag));
            distinctAppendValueBytes(key, element.getAs<types::Embedding::Primitive>());
            return;
        break;

        case CellTag::MapView:
            key.push_back(static_cast<char>(tag));
            distinctAppendMapBytes(key, element.getAs<MapView>());
            return;
        break;

        case CellTag::Path:
            key.push_back(static_cast<char>(tag));
            distinctAppendPathBytes(key, element.getAs<PathView>().getIDs());
            return;
        break;

        case CellTag::INVALID:
            throw IRException("cannot dedup by an untagged element");
        break;
    }

    bioassert(false, "Unknown CellTag");
}

// Serialize one row of a type-erased column of tagged scalars into the row key.
void distinctKeyAppendCellColumn(const Column* column, size_t row, NLSerializedKey& key) {
    const auto& raw = static_cast<const ColumnVector<CellView>*>(column)->getRaw();
    distinctAppendElementBytes(key, raw[row]);
}

// Serialize one row of a list column into the row key, so two rows key alike when their
// lists hold equal elements - the equality Cypher gives two lists.
void distinctKeyAppendListColumn(const Column* column, size_t row, NLSerializedKey& key) {
    const auto& raw = static_cast<const ColumnVector<ListView>*>(column)->getRaw();
    distinctAppendListBytes(key, raw[row]);
}

void distinctKeyAppendMapColumn(const Column* column, size_t row, NLSerializedKey& key) {
    const auto& raw = static_cast<const ColumnVector<MapView>*>(column)->getRaw();
    distinctAppendMapBytes(key, raw[row]);
}

void distinctKeyAppendPathColumn(const Column* column, size_t row, NLSerializedKey& key) {
    const std::vector<GraphPath>& raw = static_cast<const ColumnVector<GraphPath>*>(column)->getRaw();
    const GraphPath& path = raw[row];

    if (path.isNull()) {
        key.push_back(static_cast<char>(CellTag::Null));
        return;
    }

    key.push_back(static_cast<char>(CellTag::Path));
    distinctAppendPathBytes(key, path.getIDs());
}

void distinctKeyAppendOptListColumn(const Column* column, size_t row, NLSerializedKey& key) {
    const ColumnOptVector<ListView>* typedColumn = static_cast<const ColumnOptVector<ListView>*>(column);

    if (!typedColumn->isValid(row)) {
        key.push_back(static_cast<char>(CellTag::Null));
        return;
    }

    distinctAppendPresentValue(key, typedColumn->getValue(row));
}

void distinctKeyAppendOptMapColumn(const Column* column, size_t row, NLSerializedKey& key) {
    const ColumnOptVector<MapView>* typedColumn = static_cast<const ColumnOptVector<MapView>*>(column);

    if (!typedColumn->isValid(row)) {
        key.push_back(static_cast<char>(CellTag::Null));
        return;
    }

    distinctAppendPresentValue(key, typedColumn->getValue(row));
}

// Append the values of the given rows of an input chunk onto the tail of a key
// buffer of the same element type. nl.group_aggregate_update passes the rows that
// created a new group this step, so the buffer grows one key value per group in
// creation order.
template <typename ElementType>
void groupGatherAppendColumn(const Column* input,
                             const std::vector<size_t>& rows,
                             Column* buffer,
                             NLValueStore& values) {
    const auto& inputRaw = static_cast<const ColumnVector<ElementType>*>(input)->getRaw();
    auto& bufferRaw = static_cast<ColumnVector<ElementType>*>(buffer)->getRaw();

    for (const size_t row : rows) {
        bufferRaw.push_back(values.ownElement(inputRaw[row]));
    }
}

template <typename Element>
void groupGatherAppendOptColumn(const Column* input,
                                const std::vector<size_t>& rows,
                                Column* buffer,
                                NLValueStore& values) {
    const ColumnOptVector<Element>* typedInput = static_cast<const ColumnOptVector<Element>*>(input);
    ColumnOptVector<Element>* typedBuffer = static_cast<ColumnOptVector<Element>*>(buffer);
    const std::vector<Element>& inputValues = typedInput->getValues();

    if (typedInput->getNullCount() == 0) {
        for (const size_t row : rows) {
            typedBuffer->push_back(values.ownElement(inputValues[row]));
        }

        return;
    }

    for (const size_t row : rows) {
        if (typedInput->isValid(row)) {
            typedBuffer->push_back(values.ownElement(inputValues[row]));
        } else {
            typedBuffer->pushNull();
        }
    }
}

template <typename ColumnType>
void reownColumn(Column* column, NLValueStore& values) {
    values.ownRows(static_cast<ColumnType*>(column), 0);
}

template <typename ColumnType>
void reownColumnRows(Column* column, const std::vector<size_t>& rows, NLValueStore& values) {
    ColumnType* typedColumn = static_cast<ColumnType*>(column);

    for (const size_t row : rows) {
        values.ownRow(typedColumn, row);
    }
}

// Append a chunk's present values to the flat value buffer, recording each element's
// position in its group's list. A null value is skipped (Cypher collect ignores
// nulls). The flat buffer holds the collected type's primitive; the input is its
// nullable value chunk.
template <typename Primitive>
void collectFold(Column* values,
                 const Column* input,
                 const std::vector<size_t>& groups,
                 std::vector<std::vector<size_t>>& groupPositions,
                 const NLGroupDistinctTally* distinct,
                 NLValueStore& store) {
    auto& valuesRaw = static_cast<ColumnVector<Primitive>*>(values)->getRaw();
    const ColumnOptVector<Primitive>* typedInput = static_cast<const ColumnOptVector<Primitive>*>(input);
    const std::vector<Primitive>& inputValues = typedInput->getValues();

    typedInput->forEachPresentRun(0, inputValues.size(), [&](size_t first, size_t last) {
        for (size_t row = first; row < last; row++) {
            const size_t position = valuesRaw.size();
            valuesRaw.push_back(store.ownElement(inputValues[row]));
            groupPositions[groups[row]].push_back(position);
        }
    });
}

// Append this step's values a group has not collected yet - collect(DISTINCT x). A value
// repeated within its group is dropped rather than buffered a second time, so the list is
// the group's distinct values in first-seen order.
template <typename Primitive>
void collectFoldDistinct(Column* values,
                         const Column* input,
                         const std::vector<size_t>& groups,
                         std::vector<std::vector<size_t>>& groupPositions,
                         const NLGroupDistinctTally* distinct,
                         NLValueStore& store) {
    auto& valuesRaw = static_cast<ColumnVector<Primitive>*>(values)->getRaw();
    const ColumnOptVector<Primitive>* typedInput = static_cast<const ColumnOptVector<Primitive>*>(input);
    const std::vector<Primitive>& inputValues = typedInput->getValues();

    typedInput->forEachPresentRun(0, inputValues.size(), [&](size_t first, size_t last) {
        for (size_t row = first; row < last; row++) {
            if (!distinct->isNew(row)) {
                continue;
            }

            const size_t position = valuesRaw.size();
            valuesRaw.push_back(store.ownElement(inputValues[row]));
            groupPositions[groups[row]].push_back(position);
        }
    });
}

// The cell sibling of collectFold, for a chunk holding a value in every row: every row of
// the input joins its group's list.
template <typename Cell>
void collectCellFold(Column* values,
                     const Column* input,
                     const std::vector<size_t>& groups,
                     std::vector<std::vector<size_t>>& groupPositions,
                     const NLGroupDistinctTally* distinct,
                     NLValueStore& store) {
    auto& valuesRaw = static_cast<ColumnVector<Cell>*>(values)->getRaw();
    const auto& inputRaw = static_cast<const ColumnVector<Cell>*>(input)->getRaw();

    for (size_t row = 0; row < inputRaw.size(); row++) {
        groupPositions[groups[row]].push_back(valuesRaw.size());
        valuesRaw.push_back(store.ownElement(inputRaw[row]));
    }
}

// The ID sibling of collectFold: an entity an OPTIONAL MATCH did not match is an invalid
// ID, which is how a null entity is spelled, so it is dropped the way collectFold drops an
// absent optional rather than collected as the value 2^64-1.
template <typename IDType>
void collectValidIDFold(Column* values,
                        const Column* input,
                        const std::vector<size_t>& groups,
                        std::vector<std::vector<size_t>>& groupPositions,
                        const NLGroupDistinctTally* distinct,
                        NLValueStore& store) {
    auto& valuesRaw = static_cast<ColumnVector<IDType>*>(values)->getRaw();
    const auto& inputRaw = static_cast<const ColumnVector<IDType>*>(input)->getRaw();

    const size_t base = valuesRaw.size();
    const auto isValid = [](const IDType id) { return id.isValid(); };

    if (std::ranges::all_of(inputRaw, isValid)) {
        valuesRaw.insert(valuesRaw.end(), inputRaw.begin(), inputRaw.end());

        for (size_t row = 0; row < inputRaw.size(); row++) {
            groupPositions[groups[row]].push_back(base + row);
        }

        return;
    }

    valuesRaw.reserve(base + inputRaw.size());

    for (size_t row = 0; row < inputRaw.size(); row++) {
        if (!inputRaw[row].isValid()) {
            continue;
        }

        const size_t position = valuesRaw.size();
        valuesRaw.push_back(inputRaw[row]);
        groupPositions[groups[row]].push_back(position);
    }
}

// The ID sibling of collectFoldDistinct: an entity repeated within its group joins the
// list once, keyed by the ID's underlying integer, and the null an unmatched pattern left
// is no key of its own.
template <typename IDType>
void collectValidIDFoldDistinct(Column* values,
                                const Column* input,
                                const std::vector<size_t>& groups,
                                std::vector<std::vector<size_t>>& groupPositions,
                                const NLGroupDistinctTally* distinct,
                                NLValueStore& store) {
    auto& valuesRaw = static_cast<ColumnVector<IDType>*>(values)->getRaw();
    const auto& inputRaw = static_cast<const ColumnVector<IDType>*>(input)->getRaw();

    for (size_t row = 0; row < inputRaw.size(); row++) {
        if (!inputRaw[row].isValid()) {
            continue;
        }

        const size_t group = groups[row];

        if (!distinct->isNew(row)) {
            continue;
        }

        const size_t position = valuesRaw.size();
        valuesRaw.push_back(inputRaw[row]);
        groupPositions[group].push_back(position);
    }
}

// The path sibling of collectValidIDFold: the path an OPTIONAL MATCH left where its pattern
// missed holds no node, which is how a null path is spelled, so it is dropped. A kept path is
// copied into the store as a tagged cell, so its lists emit as a type-erased collect's do.
void collectPathFold(Column* values,
                     const Column* input,
                     const std::vector<size_t>& groups,
                     std::vector<std::vector<size_t>>& groupPositions,
                     const NLGroupDistinctTally* distinct,
                     NLValueStore& store) {
    auto& valuesRaw = static_cast<ColumnVector<CellView>*>(values)->getRaw();
    const std::vector<GraphPath>& inputRaw = static_cast<const ColumnVector<GraphPath>*>(input)->getRaw();

    for (size_t row = 0; row < inputRaw.size(); row++) {
        const GraphPath& path = inputRaw[row];
        if (path.isNull()) {
            continue;
        }

        ListBuffer<>::ListItemVariant item {PathView {path.getIDs()}};

        const size_t position = valuesRaw.size();
        valuesRaw.push_back(store.ownCell(item));
        groupPositions[groups[row]].push_back(position);
    }
}

void collectPathFoldDistinct(Column* values,
                             const Column* input,
                             const std::vector<size_t>& groups,
                             std::vector<std::vector<size_t>>& groupPositions,
                             const NLGroupDistinctTally* distinct,
                             NLValueStore& store) {
    auto& valuesRaw = static_cast<ColumnVector<CellView>*>(values)->getRaw();
    const std::vector<GraphPath>& inputRaw = static_cast<const ColumnVector<GraphPath>*>(input)->getRaw();

    for (size_t row = 0; row < inputRaw.size(); row++) {
        const GraphPath& path = inputRaw[row];
        if (path.isNull()) {
            continue;
        }

        const size_t group = groups[row];

        if (!distinct->isNew(row)) {
            continue;
        }

        ListBuffer<>::ListItemVariant item {PathView {path.getIDs()}};

        const size_t position = valuesRaw.size();
        valuesRaw.push_back(store.ownCell(item));
        groupPositions[group].push_back(position);
    }
}

// The type-erased sibling of collectFold: a tagged cell is there in every row, but one
// tagged null is the null Cypher's collect drops, so only the rest join the group's list.
void collectTaggedFold(Column* values,
                       const Column* input,
                       const std::vector<size_t>& groups,
                       std::vector<std::vector<size_t>>& groupPositions,
                       const NLGroupDistinctTally* distinct,
                       NLValueStore& store) {
    auto& valuesRaw = static_cast<ColumnVector<CellView>*>(values)->getRaw();
    const std::vector<CellView>& inputRaw = static_cast<const ColumnVector<CellView>*>(input)->getRaw();

    for (size_t row = 0; row < inputRaw.size(); row++) {
        const std::optional<CellView> element = presentCell(inputRaw[row]);
        if (!element) {
            continue;
        }

        const size_t position = valuesRaw.size();
        valuesRaw.push_back(store.ownElement(*element));
        groupPositions[groups[row]].push_back(position);
    }
}

// The dedup sibling: a tagged cell keys by the value its tag names, so the same number
// reached under two tags keys once - the ordering rules read these cells the same way.
void collectTaggedFoldDistinct(Column* values,
                               const Column* input,
                               const std::vector<size_t>& groups,
                               std::vector<std::vector<size_t>>& groupPositions,
                               const NLGroupDistinctTally* distinct,
                               NLValueStore& store) {
    auto& valuesRaw = static_cast<ColumnVector<CellView>*>(values)->getRaw();
    const std::vector<CellView>& inputRaw = static_cast<const ColumnVector<CellView>*>(input)->getRaw();

    for (size_t row = 0; row < inputRaw.size(); row++) {
        const std::optional<CellView> element = presentCell(inputRaw[row]);
        if (!element) {
            continue;
        }

        const size_t group = groups[row];

        if (!distinct->isNew(row)) {
            continue;
        }

        const size_t position = valuesRaw.size();
        valuesRaw.push_back(store.ownElement(*element));
        groupPositions[group].push_back(position);
    }
}

// The type-erased sibling of collectListEmit: each buffered cell goes into the list
// buffer as the value its own tag names, so the list keeps the types it gathered.
void collectTaggedListEmit(const Column* values,
                           const std::vector<std::vector<size_t>>& groupPositions,
                           size_t begin,
                           size_t count,
                           ListBuffer<>& listBuffer,
                           Column* output) {
    const auto& valuesRaw = static_cast<const ColumnVector<CellView>*>(values)->getRaw();
    auto& outputRaw = static_cast<ColumnVector<ListView>*>(output)->getRaw();

    outputRaw.clear();
    outputRaw.reserve(count);

    std::vector<ListBuffer<>::ListItemVariant> elements;
    for (size_t index = 0; index < count; index++) {
        const std::vector<size_t>& positions = groupPositions[begin + index];

        elements.clear();
        elements.reserve(positions.size());
        for (const size_t position : positions) {
            elements.push_back(taggedItem<ListBuffer<>::ListItemVariant>(valuesRaw[position]));
        }

        outputRaw.push_back(listBuffer.insert(elements));
    }
}

// The list and map sibling of collectValidIDFoldDistinct: two cells are the same value
// when their elements are, so a cell keys by the container serialized element by element
// rather than by its view's own span, which two equal containers never share.
template <typename Container>
void collectContainerFoldDistinct(Column* values,
                                  const Column* input,
                                  const std::vector<size_t>& groups,
                                  std::vector<std::vector<size_t>>& groupPositions,
                                  const NLGroupDistinctTally* distinct,
                                  NLValueStore& store) {
    auto& valuesRaw = static_cast<ColumnVector<Container>*>(values)->getRaw();
    const auto& inputRaw = static_cast<const ColumnVector<Container>*>(input)->getRaw();

    for (size_t row = 0; row < inputRaw.size(); row++) {
        const size_t group = groups[row];

        if (!distinct->isNew(row)) {
            continue;
        }

        const size_t position = valuesRaw.size();
        valuesRaw.push_back(store.ownElement(inputRaw[row]));
        groupPositions[group].push_back(position);
    }
}

// The nullable siblings of collectCellFold and collectContainerFoldDistinct for a list or a
// map chunk: a row an OPTIONAL MATCH left with no container is dropped, as collectFold drops
// an absent optional, so the buffer they fill is a plain container column either way.
template <typename Container>
void collectOptContainerFold(Column* values,
                             const Column* input,
                             const std::vector<size_t>& groups,
                             std::vector<std::vector<size_t>>& groupPositions,
                             const NLGroupDistinctTally* distinct,
                             NLValueStore& store) {
    auto& valuesRaw = static_cast<ColumnVector<Container>*>(values)->getRaw();
    const ColumnOptVector<Container>* typedInput = static_cast<const ColumnOptVector<Container>*>(input);
    const std::vector<Container>& inputValues = typedInput->getValues();

    typedInput->forEachPresentRun(0, inputValues.size(), [&](size_t first, size_t last) {
        for (size_t row = first; row < last; row++) {
            const size_t position = valuesRaw.size();
            valuesRaw.push_back(store.ownElement(inputValues[row]));
            groupPositions[groups[row]].push_back(position);
        }
    });
}

template <typename Container>
void collectOptContainerFoldDistinct(Column* values,
                                     const Column* input,
                                     const std::vector<size_t>& groups,
                                     std::vector<std::vector<size_t>>& groupPositions,
                                     const NLGroupDistinctTally* distinct,
                                     NLValueStore& store) {
    auto& valuesRaw = static_cast<ColumnVector<Container>*>(values)->getRaw();
    const ColumnOptVector<Container>* typedInput = static_cast<const ColumnOptVector<Container>*>(input);
    const std::vector<Container>& inputValues = typedInput->getValues();

    typedInput->forEachPresentRun(0, inputValues.size(), [&](size_t first, size_t last) {
        for (size_t row = first; row < last; row++) {
            if (!distinct->isNew(row)) {
                continue;
            }

            const size_t position = valuesRaw.size();
            valuesRaw.push_back(store.ownElement(inputValues[row]));
            groupPositions[groups[row]].push_back(position);
        }
    });
}

// Emit a chunk of unwound values (nl.unwind_collect): for each flat-buffer position this chunk
// covers, write the present value into the nullable value output. collect dropped
// nulls, so every emitted element is present.
template <typename Primitive>
void unwindCollectValueEmit(const Column* values,
                     const ColumnVector<size_t>* positions,
                     Column* output) {
    const auto& valuesRaw = static_cast<const ColumnVector<Primitive>*>(values)->getRaw();
    ColumnOptVector<Primitive>* typedOutput = static_cast<ColumnOptVector<Primitive>*>(output);
    const auto& positionsRaw = positions->getRaw();

    resetForRowWrites(typedOutput, positionsRaw.size());
    std::vector<Primitive>& outputRaw = typedOutput->getValues();

    for (size_t row = 0; row < positionsRaw.size(); row++) {
        outputRaw[row] = valuesRaw[positionsRaw[row]];
    }
}

// Emit a chunk of per-group lists (nl.collect): for each group in [begin, begin+count),
// gather its elements from the flat buffer into the list buffer as one contiguous run
// and store the resulting ListView in the list output.
template <typename Primitive>
void collectListEmit(const Column* values,
                     const std::vector<std::vector<size_t>>& groupPositions,
                     size_t begin,
                     size_t count,
                     ListBuffer<>& listBuffer,
                     Column* output) {
    const auto& valuesRaw = static_cast<const ColumnVector<Primitive>*>(values)->getRaw();
    auto& outputRaw = static_cast<ColumnVector<ListView>*>(output)->getRaw();

    outputRaw.clear();
    outputRaw.reserve(count);

    std::vector<ListBuffer<>::ListItemVariant> elements;
    for (size_t index = 0; index < count; index++) {
        const std::vector<size_t>& positions = groupPositions[begin + index];

        elements.clear();
        elements.reserve(positions.size());
        for (const size_t position : positions) {
            elements.push_back(ListBuffer<>::ListItemVariant {valuesRaw[position]});
        }

        outputRaw.push_back(listBuffer.insert(elements));
    }
}

// Invoke handler with the element a collect of a nullable chunk of @param kind buffers: a
// scalar, an embedding, a schema ID or a list. A map collects through handlers of its own.
template <typename Handler>
void dispatchCollectedChunkKind(NLChunkKind kind, Handler&& handler) {
    switch (kind) {
        case NLChunkKind::EdgeTypeID:
            return handler.template operator()<EdgeTypeID>();
        break;

        case NLChunkKind::LabelID:
            return handler.template operator()<LabelID>();
        break;

        case NLChunkKind::PropertyTypeID:
            return handler.template operator()<PropertyTypeID>();
        break;

        case NLChunkKind::ValueTypeCode:
            return handler.template operator()<ValueType>();
        break;

        case NLChunkKind::Int64:
            return handler.template operator()<types::Int64::Primitive>();
        break;

        case NLChunkKind::UInt64:
            return handler.template operator()<types::UInt64::Primitive>();
        break;

        case NLChunkKind::Double:
            return handler.template operator()<types::Double::Primitive>();
        break;

        case NLChunkKind::Bool:
            return handler.template operator()<types::Bool::Primitive>();
        break;

        case NLChunkKind::String:
            return handler.template operator()<types::String::Primitive>();
        break;

        case NLChunkKind::Embedding:
            return handler.template operator()<types::Embedding::Primitive>();
        break;

        case NLChunkKind::List:
            return handler.template operator()<ListView>();
        break;

        case NLChunkKind::DateTime:
            return handler.template operator()<types::DateTime::Primitive>();
        break;

        case NLChunkKind::Duration:
            return handler.template operator()<types::Duration::Primitive>();
        break;

        case NLChunkKind::Point:
            return handler.template operator()<types::Point::Primitive>();
        break;

        case NLChunkKind::Map:
        case NLChunkKind::NodeID:
        case NLChunkKind::EdgeID:
        case NLChunkKind::LabelSetID:
        case NLChunkKind::Path:
        case NLChunkKind::PathRef:
            throw IRException("collect does not support this value type");
        break;
    }

    bioassert(false, "Unknown NLChunkKind");
}

// The fold and the list emit an entity collect of this ID reads. The list emits through
// the value path's template - its elements are the IDs the fold appended - so there is
// no entity emit of its own.
template <typename IDType>
void selectCollectIDHandlers(bool distinctValues,
                             NLCollectFoldFunction& fold,
                             NLCollectListEmitFunction& listEmit) {
    fold = distinctValues ? &collectValidIDFoldDistinct<IDType> : &collectValidIDFold<IDType>;
    listEmit = &collectListEmit<IDType>;
}

// The write buffer knows a node this change wrote by its offset, so a row naming one is
// resolved back to it; every other row names a node the graph holds.
CommitWriteBuffer::ExistingOrPendingNode resolveNode(const ColumnNodeIDs* column,
                                                     size_t row,
                                                     const PendingRows& pendingRows,
                                                     size_t firstPendingNodeID) {
    const NodeID nodeID = (*column)[row];
    if (pendingRows.has(row, nodeID.getValue())) {
        return CommitWriteBuffer::PendingNodeOffset(nodeID.getValue() - firstPendingNodeID);
    } else {
        return nodeID;
    }
}

// The write buffer builds one column per property of a pending entity, so a second entry
// for the same property would be a second value for the same cell: ON CREATE writes over
// the value the pattern wrote a moment earlier rather than beside it.
void setPendingProperty(CommitWriteBuffer::UntypedProperties& properties,
                        const CommitWriteBuffer::UntypedProperty& property) {
    for (CommitWriteBuffer::UntypedProperty& held : properties) {
        if (held.propertyID == property.propertyID) {
            held.value = property.value;
            return;
        }
    }

    properties.push_back(property);
}

template <TypedInternalID IDT>
bool setWritesRow(const ColumnVector<CustomBool>* rows, IDT entity, size_t row) {
    return writeTouchesRow(rows, row) && entity.isValid();
}

template <typename IDColumn>
void markWrittenRows(const IDColumn* entities, const ColumnVector<CustomBool>* rows, ColumnVector<CustomBool>& written) {
    const auto& raw = entities->getRaw();
    written.resize(raw.size());

    for (size_t row = 0; row < raw.size(); row++) {
        written[row] = setWritesRow(rows, raw[row], row);
    }
}

// Stages the values a SET writes in @param buffer and answers the property it writes them
// to: invalid where it writes nothing
template <typename SetData>
PropertyTypeID stageSetProperty(MetadataBuilder* metadataBuilder,
                                SetData* setData,
                                size_t rowCount,
                                CommitWriteBuffer::UntypedProperties& buffer) {
    NLRuntimeProperty* runtimeProperty = setData->getRuntimeProperty();
    const PropertyTypeID propertyTypeID = setData->getPropertyTypeID();
    const Column* values = setData->getValue();

    if (!runtimeProperty) {
        extractColumnProperties(values, rowCount, propertyTypeID, buffer);
        return propertyTypeID;
    } else if (setData->writesNull()) {
        const bool staged = runtimeProperty->stageNulls(metadataBuilder, rowCount, buffer);
        return staged ? runtimeProperty->getType()._id : PropertyTypeID {};
    }

    ColumnVector<CustomBool>* written = setData->getWrittenRows();
    markWrittenRows(setData->getInput(), setData->getRows(), *written);

    const bool staged = runtimeProperty->stage(metadataBuilder, values, rowCount, written, buffer);

    return staged ? runtimeProperty->getType()._id : PropertyTypeID {};
}

// False for a chunk of nulls held in cells reaching a property with no type, which writes
// it nothing
bool stageCreatedProperty(MetadataBuilder* metadataBuilder,
                          const NLCreatedProperty& property,
                          size_t rowCount,
                          CommitWriteBuffer::UntypedProperties& buffer) {
    if (property._runtimeProperty) {
        return property._runtimeProperty->stage(metadataBuilder, property._values, rowCount, nullptr, buffer);
    }

    extractColumnProperties(property._values, rowCount, property._propertyTypeID, buffer);

    return true;
}

bool isSameMap(const WrittenMap& lhs, const WrittenMap& rhs) {
    const bool holdsAnotherKind = lhs._tag != rhs._tag || lhs._map.has_value() != rhs._map.has_value();

    if (holdsAnotherKind) {
        return false;
    } else if (!lhs._map) {
        return true;
    }

    const std::span<const MapEntryView> lhsEntries = lhs._map->entries();
    const std::span<const MapEntryView> rhsEntries = rhs._map->entries();

    return lhsEntries.data() == rhsEntries.data() && lhsEntries.size() == rhsEntries.size();
}

template <typename IDs, typename Visit>
void visitWrittenMapRows(const std::vector<WrittenMap>& maps,
                         const ColumnVector<CustomBool>* rows,
                         const IDs& raw,
                         size_t rowCount,
                         Visit visit) {
    const WrittenMap* previousMap = nullptr;

    for (size_t row = 0; row < rowCount; row++) {
        if (!setWritesRow(rows, raw[row], row)) {
            continue;
        }

        const bool holdsAnotherMap = !previousMap || !isSameMap(*previousMap, maps[row]);
        visit(row, holdsAnotherMap);

        previousMap = &maps[row];
    }
}

template <typename IDColumn>
void runSetProperties(NLExecutionContext* context, NLFunctionData* data) {
    constexpr bool WRITES_NODES = std::is_same_v<IDColumn, ColumnNodeIDs>;

    NLSetPropertiesData<IDColumn>* setData = static_cast<NLSetPropertiesData<IDColumn>*>(data);
    CommitWriteBuffer* writeBuffer = context->getWriteBuffer();
    bioassert(writeBuffer, "A properties write requires an active write transaction");

    const IDColumn* entities = setData->getInput();
    const size_t rowCount = entities->size();

    std::vector<WrittenMap>& maps = setData->getMaps();
    if (setData->writesNull()) {
        maps.assign(rowCount, WrittenMap {std::nullopt, CellTag::Null});
    } else {
        extractWrittenMaps(setData->getValue(), rowCount, maps);
    }

    const GraphView* view = context->getView();
    MetadataBuilder* metadataBuilder = context->getMetadataBuilder();
    const ColumnVector<CustomBool>* rows = setData->getRows();
    const bool replaces = setData->getReplaces();
    const auto& raw = entities->getRaw();

    std::vector<MapKeyPropertyType>& keyTypes = setData->getKeyTypes();
    std::vector<MapKeyCreatedType> createdTypes;

    forgetUntypedMapKeys(keyTypes);
    visitWrittenMapRows(maps, rows, raw, rowCount, [&](size_t row, bool holdsAnotherMap) {
        if (holdsAnotherMap) {
            widenCreatedMapKeyTypes(maps[row], view, metadataBuilder, keyTypes, createdTypes);
        }
    });

    for (const MapKeyCreatedType& created : createdTypes) {
        metadataBuilder->getOrCreatePropertyType(created._key, created._type);
    }

    CommitWriteBuffer::UntypedProperties& mapProperties = setData->getMapProperties();
    std::vector<StagedRange>& mapRanges = setData->getMapRanges();

    mapProperties.clear();
    mapRanges.assign(rowCount, StagedRange {});

    StagedRange stagedRange;

    forgetUntypedMapKeys(keyTypes);
    visitWrittenMapRows(maps, rows, raw, rowCount, [&](size_t row, bool holdsAnotherMap) {
        if (holdsAnotherMap) {
            const size_t begin = mapProperties.size();
            stageMapProperties(maps[row], view, metadataBuilder, keyTypes, mapProperties);

            stagedRange = StagedRange {begin, mapProperties.size()};
        }

        mapRanges[row] = stagedRange;
    });

    const size_t firstPendingID = WRITES_NODES ? committedNodeCount(view) : committedEdgeCount(view);
    const size_t pendingCount = WRITES_NODES ? writeBuffer->numPendingNodes() : writeBuffer->numPendingEdges();
    const PendingRows pendingRows(setData->getPending(), setData->isAllPending(), firstPendingID, pendingCount);
    NLWrittenValues& written = context->getWrittenValues();

    std::optional<NLEntityProperties> heldProperties;
    if (replaces) {
        heldProperties.emplace(context, nullptr);
    }

    CommitWriteBuffer::UntypedProperties& removals = setData->getRemovals();
    std::vector<PropertyTypeID>& held = setData->getHeld();
    std::vector<ValueType>& heldTypes = setData->getHeldTypes();

    for (size_t row = 0; row < rowCount; row++) {
        if (!setWritesRow(rows, raw[row], row)) {
            continue;
        }

        const StagedRange& range = mapRanges[row];
        const std::span<CommitWriteBuffer::UntypedProperty> writes {mapProperties.begin() + range._begin,
                                                                    mapProperties.begin() + range._end};

        removals.clear();
        if (replaces) {
            heldProperties->readIDs(raw[row], held);
            stageRemovals(held, writes, view, metadataBuilder, heldTypes, removals);
        }

        const uint64_t id = raw[row].getValue();

        if (pendingRows.has(row, id)) {
            const size_t offset = id - firstPendingID;

            if constexpr (WRITES_NODES) {
                CommitWriteBuffer::PendingNode& node = writeBuffer->getPendingNode(offset);
                const auto writeProperty = [&](const CommitWriteBuffer::UntypedProperty& property) {
                    setPendingProperty(node.properties, property);
                    written.addPendingNodeUpdate(offset, property.propertyID);
                };

                std::ranges::for_each(writes, writeProperty);
                std::ranges::for_each(removals, writeProperty);
            } else {
                CommitWriteBuffer::PendingEdge& edge = writeBuffer->getPendingEdge(offset);
                const auto writeProperty = [&](const CommitWriteBuffer::UntypedProperty& property) {
                    setPendingProperty(edge.properties, property);
                };

                std::ranges::for_each(writes, writeProperty);
                std::ranges::for_each(removals, writeProperty);
            }
        } else {
            const auto writeProperty = [&](CommitWriteBuffer::UntypedProperty& property) {
                if constexpr (WRITES_NODES) {
                    writeBuffer->addNodeUpdate(raw[row], property);
                } else {
                    writeBuffer->addEdgeUpdate(raw[row], property);
                }
            };

            std::ranges::for_each(writes, writeProperty);
            std::ranges::for_each(removals, writeProperty);
        }
    }
}

// A node an OPTIONAL MATCH did not match is an invalid ID, and no edge can hang off one.
// The whole column is read before anything is staged, so a CREATE naming a null endpoint
// writes none of its edges rather than leaving the commit one it cannot resolve. A pending
// endpoint is an offset into this change's new nodes rather than an ID, so only an existing
// one can be null.
void throwIfAnyNodeIsNull(const ColumnNodeIDs* column, bool isPending, std::string_view error) {
    if (isPending) {
        return;
    }

    for (const NodeID nodeID : column->getRaw()) {
        if (!nodeID.isValid()) {
            throw IRException(std::string(error));
        }
    }
}

void throwIfNodesHaveEdges(const GraphView& view, const ColumnNodeIDs* nodes) {
    const GetOutEdgesRange outEdges(view, nodes);
    for (const EdgeRecord& record : outEdges) {
        if (!view.isDeleted(record._edgeID)) {
            throw IRException("Cannot delete a node with relationships; use DETACH DELETE");
        }
    }

    const GetInEdgesRange inEdges(view, nodes);
    for (const EdgeRecord& record : inEdges) {
        if (!view.isDeleted(record._edgeID)) {
            throw IRException("Cannot delete a node with relationships; use DETACH DELETE");
        }
    }
}

// A merge keys the value a row asks for against the value the graph holds, and those
// arrive in a plain and a nullable column respectively: the two have to serialize alike,
// which is why a plain row keys its value as a nullable column keys a present one.
template <typename ElementType>
void mergeKeyAppendPlainColumn(const Column* column, size_t row, NLSerializedKey& key) {
    const auto& raw = static_cast<const ColumnVector<ElementType>*>(column)->getRaw();

    distinctAppendPresentValue(key, raw[row]);
}

template <typename ElementType>
void mergeKeyAppendConstColumn(const Column* column, size_t row, NLSerializedKey& key) {
    distinctAppendPresentValue(key, (*static_cast<const ColumnConst<ElementType>*>(column))[row]);
}

// The numeric siblings of the three above, keying the row's value in KeyType - the type
// the schema holds the property as - rather than in the one its column holds. The
// analyzer lets an integer constrain a double-typed property, and the graph side keys
// the double it reads back, so the two only compare equal once the row's value is
// converted to it.
template <typename ElementType, typename KeyType>
void mergeKeyAppendNumericPlainColumn(const Column* column, size_t row, NLSerializedKey& key) {
    const auto& raw = static_cast<const ColumnVector<ElementType>*>(column)->getRaw();

    distinctAppendPresentValue(key, static_cast<KeyType>(raw[row]));
}

template <typename ElementType, typename KeyType>
void mergeKeyAppendNumericConstColumn(const Column* column, size_t row, NLSerializedKey& key) {
    distinctAppendPresentValue(key, static_cast<KeyType>((*static_cast<const ColumnConst<ElementType>*>(column))[row]));
}

template <typename ElementType, typename KeyType>
void mergeKeyAppendNumericOptColumn(const Column* column, size_t row, NLSerializedKey& key) {
    const ColumnOptVector<ElementType>* typedColumn = static_cast<const ColumnOptVector<ElementType>*>(column);

    if (!typedColumn->isValid(row)) {
        key.push_back('\0');
        return;
    }

    distinctAppendPresentValue(key, static_cast<KeyType>(typedColumn->getValue(row)));
}

// Keys the way a nullable column's null row does, so a merge on a null value binds the
// nodes that lack the property
void mergeKeyAppendConstNull(const Column* column, size_t row, NLSerializedKey& key) {
    key.push_back('\0');
}

// Which of the three shapes a merge property value column comes in, so one selector
// serves all of them
enum class MergeColumnShape {
    Plain,
    Const,
    Opt,
};

template <typename ElementType, typename KeyType, MergeColumnShape Shape>
NLKeyAppendFunction mergeKeyAppendFor() {
    if constexpr (Shape == MergeColumnShape::Plain) {
        return &mergeKeyAppendNumericPlainColumn<ElementType, KeyType>;
    } else if constexpr (Shape == MergeColumnShape::Const) {
        return &mergeKeyAppendNumericConstColumn<ElementType, KeyType>;
    } else {
        return &mergeKeyAppendNumericOptColumn<ElementType, KeyType>;
    }
}

// The appender for a numeric value column, keying its rows in the type the schema holds
// the property as. Identical types convert for free, so one path covers both.
template <typename ElementType, MergeColumnShape Shape>
NLKeyAppendFunction selectNumericMergeKeyAppend(ValueType keyType) {
    switch (keyType) {
        case ValueType::Int64:
            return mergeKeyAppendFor<ElementType, types::Int64::Primitive, Shape>();
        break;

        case ValueType::UInt64:
            return mergeKeyAppendFor<ElementType, types::UInt64::Primitive, Shape>();
        break;

        case ValueType::Double:
            return mergeKeyAppendFor<ElementType, types::Double::Primitive, Shape>();
        break;

        default:
            throw IRException("a MERGE pattern cannot constrain a non-numeric property to a number");
        break;
    }
}

// A non-numeric value converts to no other type the schema might hold the property as,
// so it can only key against a property of its own type
void throwUnlessKeyedAsItsOwnType(ValueType valueType, ValueType keyType) {
    if (valueType != keyType) {
        throw IRException("a MERGE pattern cannot constrain a property to a value of another type");
    }
}

}

NLExecutor::NLExecutor(const GraphView* view,
                       const NLProgram* prog,
                       NLOutputSink* sink,
                       CommitWriteBuffer* writeBuffer,
                       const NLSystemContext* system,
                       MetadataBuilder* metadataBuilder)
    : _ctxt(view, sink, prog->getChunkSize(), writeBuffer, system, metadataBuilder),
    _prog(prog)
{
}

NLExecutor::~NLExecutor() {
}

void NLExecutor::run() {
    NLOutputSink* const sink = _ctxt.getSink();
    if (sink) {
        NLOutputData* const outputData = _prog->getOutputData();
        std::span<const Column* const> outputs;
        if (outputData) {
            outputs = outputData->outputs();
        }

        sink->declareOutput(_prog->columnNames(), outputs);
    }

    runBody(&_ctxt, _prog->getStmts());
}

void NLExecutor::runCreateNode(NLExecutionContext* context, NLFunctionData* data) {
    NLCreateNodeData* createData = static_cast<NLCreateNodeData*>(data);
    CommitWriteBuffer* writeBuffer = context->getWriteBuffer();
    bioassert(writeBuffer, "nl.create_node requires an active write transaction");

    const size_t rowCount = createData->getRowCount();
    const LabelSetHandle labelsetHandle = createData->getLabelSetHandle();

    MetadataBuilder* metadataBuilder = context->getMetadataBuilder();

    const std::vector<NLCreateNodeData::Property>& properties = createData->properties();
    std::vector<CommitWriteBuffer::UntypedProperties> extracted;
    extracted.reserve(properties.size());

    for (const NLCreateNodeData::Property& property : properties) {
        CommitWriteBuffer::UntypedProperties& values = extracted.emplace_back();

        if (!stageCreatedProperty(metadataBuilder, property, rowCount, values)) {
            extracted.pop_back();
        }
    }

    // Must extract this value before adding in the loop
    const size_t numPendingNodes = writeBuffer->numPendingNodes();

    for (size_t row = 0; row < rowCount; row++) {
        CommitWriteBuffer::PendingNode& node = writeBuffer->newPendingNode();
        node.labelsetHandle = labelsetHandle;

        for (const CommitWriteBuffer::UntypedProperties& values : extracted) {
            node.properties.push_back(values[row]);
        }
    }

    const GraphView* view = context->getView();
    const NodeID nextNodeID = committedNodeCount(view) + numPendingNodes;

    ColumnNodeIDs* result = createData->getResult();
    result->resize(rowCount);
    auto& raw = result->getRaw();
    for (size_t row = 0; row < rowCount; row++) {
        raw[row] = NodeID(nextNodeID + row);
    }
}

void NLExecutor::runMerge(NLExecutionContext* context, NLFunctionData* data) {
    NLMergeExecutor merge(context, static_cast<NLMergeData*>(data));
    merge.run();
}

void NLExecutor::runCreateEdge(NLExecutionContext* context, NLFunctionData* data) {
    NLCreateEdgeData* createData = static_cast<NLCreateEdgeData*>(data);
    CommitWriteBuffer* writeBuffer = context->getWriteBuffer();
    bioassert(writeBuffer, "nl.create_edge requires an active write transaction");

    const ColumnNodeIDs* src = createData->getSrc();
    const ColumnNodeIDs* tgt = createData->getTgt();
    const bool srcIsPending = createData->isSrcPending();
    const bool tgtIsPending = createData->isTgtPending();
    const size_t rowCount = src->size();
    const EdgeTypeID edgeTypeID = createData->getEdgeTypeID();

    const ColumnVector<CustomBool>* srcPending = createData->getSrcPendingMask();
    const ColumnVector<CustomBool>* tgtPending = createData->getTgtPendingMask();

    throwIfAnyNodeIsNull(src, srcIsPending, "Cannot create an edge from a null node");
    throwIfAnyNodeIsNull(tgt, tgtIsPending, "Cannot create an edge to a null node");

    const GraphView* view = context->getView();
    const size_t firstPendingNodeID = committedNodeCount(view);
    const size_t numPendingNodes = writeBuffer->numPendingNodes();

    const PendingRows srcRows(srcPending, srcIsPending, firstPendingNodeID, numPendingNodes);
    const PendingRows tgtRows(tgtPending, tgtIsPending, firstPendingNodeID, numPendingNodes);

    MetadataBuilder* metadataBuilder = context->getMetadataBuilder();

    const std::vector<NLCreateEdgeData::Property>& properties = createData->properties();
    std::vector<CommitWriteBuffer::UntypedProperties> extracted;
    extracted.reserve(properties.size());

    for (const NLCreateEdgeData::Property& property : properties) {
        CommitWriteBuffer::UntypedProperties& values = extracted.emplace_back();

        if (!stageCreatedProperty(metadataBuilder, property, rowCount, values)) {
            extracted.pop_back();
        }
    }

    // Must extract this value before adding in the loop
    const size_t numPendingEdges = writeBuffer->numPendingEdges();

    for (size_t row = 0; row < rowCount; row++) {
        const CommitWriteBuffer::ExistingOrPendingNode srcNode = resolveNode(src, row, srcRows, firstPendingNodeID);
        const CommitWriteBuffer::ExistingOrPendingNode tgtNode = resolveNode(tgt, row, tgtRows, firstPendingNodeID);

        CommitWriteBuffer::PendingEdge& edge = writeBuffer->newPendingEdge(srcNode, tgtNode);
        edge.edgeType = edgeTypeID;

        for (const CommitWriteBuffer::UntypedProperties& values : extracted) {
            edge.properties.push_back(values[row]);
        }
    }

    const EdgeID nextEdgeID = committedEdgeCount(view) + numPendingEdges;

    ColumnEdgeIDs* result = createData->getResult();
    result->resize(rowCount);
    for (size_t row = 0; row < rowCount; row++) {
        (*result)[row] = EdgeID(nextEdgeID + row);
    }
}

void NLExecutor::runSetNodeProperty(NLExecutionContext* context, NLFunctionData* data) {
    NLSetNodePropertyData* setData = static_cast<NLSetNodePropertyData*>(data);
    CommitWriteBuffer* writeBuffer = context->getWriteBuffer();
    bioassert(writeBuffer, "nl.set_node_property requires an active write transaction");

    const ColumnNodeIDs* nodes = setData->getInput();
    const size_t rowCount = nodes->size();

    CommitWriteBuffer::UntypedProperties propsBuffer;
    const PropertyTypeID propID = stageSetProperty(context->getMetadataBuilder(), setData, rowCount, propsBuffer);
    if (!propID.isValid()) {
        return;
    }

    const ColumnVector<CustomBool>* pending = setData->getPending();
    const bool allPending = setData->isAllPending();
    const ColumnVector<CustomBool>* rows = setData->getRows();

    const size_t firstPendingNodeID = committedNodeCount(context->getView());
    const PendingRows pendingRows(pending, allPending, firstPendingNodeID, writeBuffer->numPendingNodes());
    NLWrittenValues& written = context->getWrittenValues();

    // A node an OPTIONAL MATCH did not match is an invalid ID, which Cypher writes nothing
    // for: staging it would have the commit look the ID up among the nodes this change
    // wrote, where it is not.
    const auto& raw = nodes->getRaw();
    for (size_t row = 0; row < rowCount; row++) {
        if (!setWritesRow(rows, raw[row], row)) {
            continue;
        }

        if (pendingRows.has(row, raw[row].getValue())) {
            const size_t offset = raw[row].getValue() - firstPendingNodeID;
            CommitWriteBuffer::PendingNode& node = writeBuffer->getPendingNode(offset);
            setPendingProperty(node.properties, propsBuffer[row]);
            written.addPendingNodeUpdate(offset, propID);
        } else {
            writeBuffer->addNodeUpdate(raw[row], propsBuffer[row]);
        }
    }
}

void NLExecutor::runSetEdgeProperty(NLExecutionContext* context, NLFunctionData* data) {
    NLSetEdgePropertyData* setData = static_cast<NLSetEdgePropertyData*>(data);
    CommitWriteBuffer* writeBuffer = context->getWriteBuffer();
    bioassert(writeBuffer, "nl.set_edge_property requires an active write transaction");

    const ColumnEdgeIDs* edges = setData->getInput();
    const size_t rowCount = edges->size();

    CommitWriteBuffer::UntypedProperties propsBuffer;
    const PropertyTypeID propID = stageSetProperty(context->getMetadataBuilder(), setData, rowCount, propsBuffer);
    if (!propID.isValid()) {
        return;
    }

    const ColumnVector<CustomBool>* pending = setData->getPending();
    const bool allPending = setData->isAllPending();
    const ColumnVector<CustomBool>* rows = setData->getRows();

    const size_t firstPendingEdgeID = committedEdgeCount(context->getView());
    const PendingRows pendingRows(pending, allPending, firstPendingEdgeID, writeBuffer->numPendingEdges());

    const auto& raw = edges->getRaw();
    for (size_t row = 0; row < rowCount; row++) {
        if (!setWritesRow(rows, raw[row], row)) {
            continue;
        }

        if (pendingRows.has(row, raw[row].getValue())) {
            CommitWriteBuffer::PendingEdge& edge =
                writeBuffer->getPendingEdge(raw[row].getValue() - firstPendingEdgeID);
            setPendingProperty(edge.properties, propsBuffer[row]);
        } else {
            writeBuffer->addEdgeUpdate(raw[row], propsBuffer[row]);
        }
    }
}

void NLExecutor::runSetNodeProperties(NLExecutionContext* context, NLFunctionData* data) {
    runSetProperties<ColumnNodeIDs>(context, data);
}

void NLExecutor::runSetEdgeProperties(NLExecutionContext* context, NLFunctionData* data) {
    runSetProperties<ColumnEdgeIDs>(context, data);
}

void NLExecutor::runDeleteNode(NLExecutionContext* context, NLFunctionData* data) {
    NLDeleteNodeData* deleteData = static_cast<NLDeleteNodeData*>(data);
    CommitWriteBuffer* writeBuffer = context->getWriteBuffer();
    bioassert(writeBuffer, "nl.delete_node requires an active write transaction");

    const ColumnNodeIDs* nodes = deleteData->getInput();
    const GraphView* view = context->getView();
    const ColumnVector<CustomBool>* pending = deleteData->getPending();
    const bool allPending = deleteData->isAllPending();
    const bool detaching = deleteData->isDetaching();

    ColumnNodeIDs* committed = deleteData->getCommitted();
    committed->clear();

    const size_t firstPendingNodeID = committedNodeCount(view);
    const PendingRows pendingRows(pending, allPending, firstPendingNodeID, writeBuffer->numPendingNodes());

    const auto& raw = nodes->getRaw();

    // A node an OPTIONAL MATCH did not match is an invalid ID, which Cypher deletes nothing
    // for. Staging it would tombstone an ID the graph never held, and the node count reads
    // the tombstones off what it allocated: it would report one node too few.
    for (size_t row = 0; row < raw.size(); row++) {
        if (!raw[row].isValid()) {
            continue;
        }

        const CommitWriteBuffer::ExistingOrPendingNode node =
            resolveNode(nodes, row, pendingRows, firstPendingNodeID);

        // A relationship this query wrote counts as much as one the graph holds, whether
        // it hangs off a node the query wrote or off one already committed
        if (!detaching && writeBuffer->hasPendingEdgesOn(node)) {
            throw IRException("Cannot delete a node with relationships; use DETACH DELETE");
        }

        if (std::holds_alternative<CommitWriteBuffer::PendingNodeOffset>(node)) {
            writeBuffer->addDeletedPendingNode(std::get<CommitWriteBuffer::PendingNodeOffset>(node));
        } else {
            committed->push_back(raw[row]);
        }
    }

    if (!detaching) {
        throwIfNodesHaveEdges(*view, committed);
    }

    writeBuffer->addDeletedNodes(committed->getRaw());

    if (!detaching) {
        return;
    }

    writeBuffer->addHangingEdges(*view);
    writeBuffer->addHangingPendingEdges();
}

void NLExecutor::runDeleteEdge(NLExecutionContext* context, NLFunctionData* data) {
    NLDeleteEdgeData* deleteData = static_cast<NLDeleteEdgeData*>(data);
    CommitWriteBuffer* writeBuffer = context->getWriteBuffer();
    bioassert(writeBuffer, "nl.delete_edge requires an active write transaction");

    const ColumnEdgeIDs* edges = deleteData->getInput();
    const ColumnVector<CustomBool>* pending = deleteData->getPending();
    const bool allPending = deleteData->isAllPending();

    ColumnEdgeIDs* committed = deleteData->getCommitted();
    committed->clear();

    const size_t firstPendingEdgeID = committedEdgeCount(context->getView());
    const PendingRows pendingRows(pending, allPending, firstPendingEdgeID, writeBuffer->numPendingEdges());

    const auto& raw = edges->getRaw();
    for (size_t row = 0; row < raw.size(); row++) {
        if (!raw[row].isValid()) {
            continue;
        }

        if (pendingRows.has(row, raw[row].getValue())) {
            writeBuffer->addDeletedPendingEdge(raw[row].getValue() - firstPendingEdgeID);
        } else {
            committed->push_back(raw[row]);
        }
    }

    writeBuffer->addDeletedEdges(committed->getRaw());
}

void NLExecutor::runBroadcastConstant(NLExecutionContext*, NLFunctionData* data) {
    const NLBroadcastConstantData* broadcast = static_cast<NLBroadcastConstantData*>(data);
    const Column* cardinality = broadcast->getCardinality();

    // The driving relation's chunk sizes this step, exactly as it sizes an output of
    // constants alone; a projection no relation drives is the single row the constant
    // is. The fill writes that many rows of the constant's value.
    const size_t rowCount = cardinality ? cardinality->size() : 1;

    broadcast->getFill()(broadcast->getValue(), rowCount, broadcast->getOutput());
}

void NLExecutor::runBinary(NLExecutionContext*, NLFunctionData* data) {
    const NLBinaryData* binary = static_cast<NLBinaryData*>(data);
    NLStepBuffers* const stepBuffers = binary->getStepBuffers();
    stepBuffers->clear();

    binary->getFn()(binary->getResult(), binary->getLhs(), binary->getRhs(), stepBuffers);
}

void NLExecutor::runUnary(NLExecutionContext*, NLFunctionData* data) {
    const NLUnaryData* unary = static_cast<NLUnaryData*>(data);
    unary->getFn()(unary->getResult(), unary->getOperand());
}

void NLExecutor::runFillNull(NLExecutionContext*, NLFunctionData* data) {
    const NLFillNullData* fillNull = static_cast<NLFillNullData*>(data);

    fillNull->getFill()(fillNull->getOutput(), fillNull->getRows()->size());
}

void NLExecutor::runCase(NLExecutionContext*, NLFunctionData* data) {
    const NLCaseData* caseData = static_cast<NLCaseData*>(data);

    Column* const result = caseData->getResult();
    const size_t rowCount = caseData->getCardinality()->size();

    NLStepBuffers* const stepBuffers = caseData->getStepBuffers();
    stepBuffers->clear();
    NLValueStore& values = stepBuffers->valueStore();

    caseData->getReset()(result, rowCount);

    const std::vector<NLCaseData::Branch>& branches = caseData->branches();
    const Column* const defaultValue = caseData->getDefaultValue();
    const NLCellWrite& writeDefault = caseData->getWriteDefault();

    // A row takes the first branch whose condition holds, so the branches are walked per
    // row rather than the rows per branch: the first match ends the row
    for (size_t row = 0; row < rowCount; row++) {
        bool matched = false;

        for (const NLCaseData::Branch& branch : branches) {
            if (!branch._test(branch._condition, row)) {
                continue;
            }

            writeCell(result, row, branch._write, branch._value, row, stepBuffers, values);
            matched = true;
            break;
        }

        if (!matched && defaultValue) {
            writeCell(result, row, writeDefault, defaultValue, row, stepBuffers, values);
        }
    }

    const NLCaseFinishFn finish = caseData->getFinish();
    if (finish) {
        finish(result);
    }
}

void NLExecutor::runCaseMerge(NLExecutionContext*, NLFunctionData* data) {
    NLCaseData* caseData = static_cast<NLCaseData*>(data);

    Column* const result = caseData->getResult();
    const size_t rowCount = caseData->getCardinality()->size();

    NLStepBuffers* const stepBuffers = caseData->getStepBuffers();
    stepBuffers->clear();
    NLValueStore& values = stepBuffers->valueStore();

    caseData->getReset()(result, rowCount);

    const std::vector<NLCaseData::Branch>& branches = caseData->branches();
    const Column* const defaultValue = caseData->getDefaultValue();
    const NLCellWrite& writeDefault = caseData->getWriteDefault();

    std::vector<NLCaseData::BranchCursor>& cursors = caseData->cursors();
    std::fill(cursors.begin(), cursors.end(), NLCaseData::BranchCursor {});

    size_t defaultRow = 0;

    for (size_t row = 0; row < rowCount; row++) {
        bool matched = false;

        for (size_t branchIndex = 0; branchIndex < branches.size(); branchIndex++) {
            const NLCaseData::Branch& branch = branches[branchIndex];
            NLCaseData::BranchCursor& cursor = cursors[branchIndex];

            const bool holds = branch._test(branch._condition, cursor._conditionRow);
            cursor._conditionRow++;

            if (!holds) {
                continue;
            }

            writeCell(result, row, branch._write, branch._value, cursor._valueRow, stepBuffers, values);
            cursor._valueRow++;
            matched = true;
            break;
        }

        if (!matched && defaultValue) {
            writeCell(result, row, writeDefault, defaultValue, defaultRow, stepBuffers, values);
            defaultRow++;
        }
    }

    const NLCaseFinishFn finish = caseData->getFinish();
    if (finish) {
        finish(result);
    }
}

void NLExecutor::runFillNulls(NLExecutionContext*, NLFunctionData* data) {
    const NLFillNullsData* fillNulls = static_cast<NLFillNullsData*>(data);

    fillNulls->getFillNullRows()(fillNulls->getResult(),
                                 fillNulls->getValue(),
                                 fillNulls->getNullRows(),
                                 fillNulls->getFill());
}

void NLExecutor::runToAny(NLExecutionContext*, NLFunctionData* data) {
    const NLToAnyData* toAny = static_cast<NLToAnyData*>(data);

    NLStepBuffers* const stepBuffers = toAny->getStepBuffers();
    stepBuffers->clear();
    NLValueStore& values = stepBuffers->valueStore();

    const Column* const operand = toAny->getOperand();
    const NLListItemReadFunction read = toAny->getRead();
    const size_t rowCount = operand->size();

    std::vector<CellView>& results = static_cast<ColumnVector<CellView>*>(toAny->getResult())->getRaw();
    results.resize(rowCount);

    for (size_t row = 0; row < rowCount; row++) {
        ListBuffer<>::ListItemVariant item = read(operand, row, stepBuffers);
        results[row] = values.ownCell(item);
    }
}

void NLExecutor::runRowListsReset(NLExecutionContext* context, NLFunctionData* data) {
    NLRowListsResetData* reset = static_cast<NLRowListsResetData*>(data);

    // No cardinality chunk: nothing was in flight where the lists are read, which is the
    // single empty row Cypher starts from
    const Column* const cardinality = reset->getCardinality();
    const size_t rowCount = cardinality ? cardinality->size() : 1;

    reset->getState()->reset(rowCount);

    // The tag holds each row's position, so the body carries it the way it carries any
    // other column and the collect reads the row each value belongs to off it.
    std::vector<uint64_t>& tagRaw = reset->getTag()->getRaw();
    tagRaw.resize(rowCount);
    std::iota(tagRaw.begin(), tagRaw.end(), uint64_t {0});
}

void NLExecutor::runRowListsCollect(NLExecutionContext* context, NLFunctionData* data) {
    NLRowListsCollectData* collect = static_cast<NLRowListsCollectData*>(data);

    NLRowListsState* state = collect->getState();
    const Column* const value = collect->getValue();
    const NLListItemReadFunction valueRead = collect->getValueRead();
    NLStepBuffers* const memory = collect->getStepBuffers();
    memory->clear();

    const ColumnVector<uint64_t>* const tag = collect->getTag();

    if (!tag) {
        const uint64_t row = untaggedRow(collect->getRow());

        for (size_t match = 0; match < value->size(); match++) {
            state->stage(row, valueRead(value, match, memory));
        }

        return;
    }

    const std::vector<uint64_t>& tagRaw = tag->getRaw();

    bioassert(value->size() == tagRaw.size(),
              "Yielded value of a row lists body is not row-aligned with its row tags.");

    for (size_t match = 0; match < tagRaw.size(); match++) {
        state->stage(tagRaw[match], valueRead(value, match, memory));
    }
}

void NLExecutor::runRowListsResult(NLExecutionContext* context, NLFunctionData* data) {
    NLRowListsResultData* result = static_cast<NLRowListsResultData*>(data);

    NLStepBuffers* const memory = result->getStepBuffers();
    memory->clear();

    result->getState()->buildLists(memory->listBuffer(), result->getResult()->getRaw());
}

NLCaseResetFn NLExecutor::selectTaggedCellReset() {
    return &resetTaggedCells;
}

NLCaseWriteFn NLExecutor::selectNullCellWrite() {
    return &caseWriteNullTaggedCell;
}

NLCaseResetFn NLExecutor::selectCaseReset(NLChunkKind kind) {
    NLCaseResetFn reset = nullptr;
    dispatchOptChunkKind(kind, [&]<typename Element>() { reset = &fillNullOptColumn<Element>; });

    return reset;
}

NLCaseFinishFn NLExecutor::selectCaseFinish(NLChunkKind kind) {
    NLCaseFinishFn finish = nullptr;
    dispatchOptChunkKind(kind, [&]<typename Element>() { finish = &dropAllValidOptMask<Element>; });

    return finish;
}

template <bool TESTS_NULL>
NLUnaryFn NLExecutor::selectNullTest(const Column* operand, LocalMemory* memory, Column*& result) {
    NullTestSelector<TESTS_NULL> selector {._memory = memory};
    ColumnSingleDispatcher<InternalKind::Types::Tuple, NullTestSelector<TESTS_NULL>>::dispatch(operand, selector);

    result = selector._result;
    return selector._fn;
}

NLCaseResetFn NLExecutor::selectOwnNullCaseReset(NLChunkKind kind) {
    if (kind == NLChunkKind::NodeID) {
        return &fillNullColumn<NodeID>;
    } else if (kind == NLChunkKind::EdgeID) {
        return &fillNullColumn<EdgeID>;
    } else if (kind == NLChunkKind::Path) {
        return &fillNullColumn<GraphPath>;
    }

    throw IRException("Only a node, an edge or a path column holds its own null");
}

NLCaseWriteFn NLExecutor::selectOwnNullCaseWrite(NLChunkKind kind, bool untypedNull) {
    if (kind == NLChunkKind::NodeID) {
        return untypedNull ? &caseWriteOwnNull<NodeID> : &caseWriteOwnNullCell<NodeID>;
    } else if (kind == NLChunkKind::EdgeID) {
        return untypedNull ? &caseWriteOwnNull<EdgeID> : &caseWriteOwnNullCell<EdgeID>;
    } else if (kind == NLChunkKind::Path) {
        return untypedNull ? &caseWriteOwnNull<GraphPath> : &caseWriteOwnNullCell<GraphPath>;
    }

    throw IRException("Only a node, an edge or a path column holds its own null");
}

NLFillNullRowsFn NLExecutor::selectFillNullRows(NLChunkKind kind) {
    NLFillNullRowsFn selected = nullptr;
    dispatchOptChunkKind(kind, [&]<typename Element>() { selected = &fillNullOptRows<Element>; });

    return selected;
}

NLFillNullRowsFn NLExecutor::selectOwnNullFillNullRows(NLChunkKind kind) {
    if (kind == NLChunkKind::NodeID) {
        return &fillNullRows<NodeID>;
    } else if (kind == NLChunkKind::EdgeID) {
        return &fillNullRows<EdgeID>;
    } else if (kind == NLChunkKind::Path) {
        return &fillNullRows<GraphPath>;
    }

    throw IRException("Only a node, an edge or a path column holds its own null");
}

NLFillNullRowsFn NLExecutor::selectTaggedCellFillNullRows() {
    return &fillNullRows<CellView>;
}

NLCaseWriteFn NLExecutor::selectCaseWrite(NLChunkKind kind,
                                          const Column* value,
                                          bool nullable,
                                          bool untypedNull) {
    NLCaseWriteFn write = nullptr;
    dispatchOptChunkKind(kind, [&]<typename Element>() {
        if (untypedNull) {
            write = &caseWriteNull<Element>;
        } else if (nullable) {
            write = &caseWriteOptCell<Element>;
        } else {
            write = &caseWritePlainCell<Element>;
        }
    });

    return write;
}

NLUnaryFn NLExecutor::selectEntityToNullable(NLChunkKind kind, LocalMemory* memory, Column*& result) {
    const bool readsIDs = kind == NLChunkKind::NodeID || kind == NLChunkKind::EdgeID;
    if (!readsIDs) {
        throw IRException("Only a node or an edge column can be read as a nullable ID column");
    }

    result = memory->alloc<ColumnOptVector<types::UInt64::Primitive>>();

    if (kind == NLChunkKind::NodeID) {
        return &entityToNullableColumn<NodeID>;
    }

    return &entityToNullableColumn<EdgeID>;
}

NLUnaryFn NLExecutor::selectCheckedCast(NLChunkKind kind, const Column* operand, LocalMemory* memory, Column*& result) {
    if (kind == NLChunkKind::NodeID) {
        return selectCheckedCastOf<NodeID>(operand, memory, result);
    } else if (kind == NLChunkKind::EdgeID) {
        return selectCheckedCastOf<EdgeID>(operand, memory, result);
    }

    throw IRException("A tagged cell is checked to hold a node or an edge only");
}

NLUnaryFn NLExecutor::selectToNullable(NLChunkKind kind, const Column* operand, LocalMemory* memory, Column*& result) {
    NLUnaryFn toNullable = nullptr;
    dispatchOptChunkKind(kind, [&]<typename Element>() {
        toNullable = selectToNullableOf<Element>(operand, memory, result);
    });

    return toNullable;
}

template <ColumnOperator Op>
NLBinaryFn NLExecutor::selectBinary(const Column* lhs,
                                    const Column* rhs,
                                    LocalMemory* memory,
                                    Column*& result) {
    using Pairs = PairRestrictions<Op>;

    BinaryOpSelector<Op> selector {._memory = memory};
    ColumnDoubleDispatcher<typename Pairs::Allowed,
                           BinaryOpSelector<Op>,
                           typename Pairs::Excluded>::dispatch(lhs, rhs, selector);

    result = selector._result;
    return selector._fn;
}

void NLExecutor::runOptionalReset(NLExecutionContext* context, NLFunctionData* data) {
    NLOptionalResetData* reset = static_cast<NLOptionalResetData*>(data);
    NLOptionalState* state = reset->getState();

    state->reset();

    // The tag holds each input row's position, so the pattern carries it the way it
    // carries any other column and the collect reads the rows it matched off it.
    std::vector<uint64_t>& tagRaw = reset->getTag()->getRaw();
    tagRaw.resize(state->getRowCount());
    std::iota(tagRaw.begin(), tagRaw.end(), uint64_t {0});
}

void NLExecutor::runOptionalCollect(NLExecutionContext* context, NLFunctionData* data) {
    NLOptionalCollectData* collect = static_cast<NLOptionalCollectData*>(data);
    NLOptionalState* state = collect->getState();

    for (const NLSortCollectData::Append& append : collect->appends()) {
        appendCollectedChunk(append, collect->getState()->values());
    }

    const ColumnVector<uint64_t>* tag = collect->getTag();
    if (!tag) {
        // No input row to tag: the step is the single empty row, which this step's rows
        // match - and a predicate that cut them all leaves nothing to match it.
        const std::vector<NLSortCollectData::Append>& appends = collect->appends();
        bioassert(!appends.empty(), "nl.optional_collect needs at least one column");

        if (appends.front()._input->size() > 0) {
            state->markMatched(0);
        }

        return;
    }

    for (const uint64_t row : tag->getRaw()) {
        state->markMatched(row);
    }
}

void NLExecutor::runOptionalDrainLoop(NLExecutionContext* context, NLFunctionData* data) {
    NLOptionalDrainLoopData* loopData = static_cast<NLOptionalDrainLoopData*>(data);
    NLOptionalState* state = loopData->getState();

    const NLStmtContainer* loopBody = loopData->getStmts();
    const size_t chunkSize = context->getChunkSize();
    ColumnVector<size_t>* indices = loopData->getIndices();
    std::vector<size_t>& indicesRaw = indices->getRaw();

    const NLLimitState* limit = loopData->getLimit();
    const auto budgetLeft = [&]() { return !limit || limit->getRemaining() > 0; };

    // The rows the pattern matched, re-chunked out of the buffers the collect grew. A
    // chunk never mixes them with the padded ones: the two are gathered from different
    // columns, so each step reads one or the other.
    const size_t matchedRows = state->getMatchedRowCount();
    for (size_t offset = 0; offset < matchedRows && budgetLeft(); offset += chunkSize) {
        const size_t stepRows = std::min(chunkSize, matchedRows - offset);

        indicesRaw.resize(stepRows);
        std::iota(indicesRaw.begin(), indicesRaw.end(), offset);

        for (const NLOptionalDrainLoopData::DrainColumn& column : loopData->columns()) {
            column._gather(column._buffer, indices, column._output);
        }

        runBody(context, loopBody);
    }

    // Then one row per input row nothing matched: its own values in the columns the
    // pattern joined onto, and null in the ones the pattern binds.
    const std::vector<size_t>& missedRaw = state->missedRows().getRaw();
    const size_t missedRows = missedRaw.size();
    for (size_t offset = 0; offset < missedRows && budgetLeft(); offset += chunkSize) {
        const size_t stepRows = std::min(chunkSize, missedRows - offset);

        indicesRaw.assign(missedRaw.begin() + offset, missedRaw.begin() + offset + stepRows);

        for (const NLOptionalDrainLoopData::DrainColumn& column : loopData->columns()) {
            if (column._input) {
                column._gather(column._input, indices, column._output);
            } else {
                column._fillNull(column._output, stepRows);
            }
        }

        runBody(context, loopBody);
    }
}

void NLExecutor::runSubqueryExpressionReset(NLExecutionContext* context, NLFunctionData* data) {
    NLSubqueryExpressionResetData* reset = static_cast<NLSubqueryExpressionResetData*>(data);
    NLSubqueryExpressionState* state = reset->getState();

    state->reset();

    // The tag holds each input row's position, so the body carries it the way it carries
    // any other column and the mark reads the rows it reached off it.
    std::vector<uint64_t>& tagRaw = reset->getTag()->getRaw();
    tagRaw.resize(state->getRowCount());
    std::iota(tagRaw.begin(), tagRaw.end(), uint64_t {0});
}

void NLExecutor::runExistsMark(NLExecutionContext* context, NLFunctionData* data) {
    NLExistsMarkData* mark = static_cast<NLExistsMarkData*>(data);
    NLExistsState* state = mark->getState();

    const ColumnVector<uint64_t>* tag = mark->getTag();
    if (!tag) {
        // Every row of this step answers for one input row, which they mark - and a
        // predicate that cut them all leaves nothing to mark it.
        const std::vector<const Column*>& columns = mark->columns();
        bioassert(!columns.empty(), "nl.exists_mark needs at least one column when it has no tag");

        if (columns.front()->size() > 0) {
            state->markMatched(untaggedRow(mark->getRow()));
        }

        return;
    }

    for (const uint64_t row : tag->getRaw()) {
        state->markMatched(row);
    }
}

void NLExecutor::runExistsResult(NLExecutionContext* context, NLFunctionData* data) {
    NLExistsResultData* result = static_cast<NLExistsResultData*>(data);
    const NLExistsState* state = result->getState();

    const std::vector<bool>& matched = state->matched();

    std::vector<CustomBool>& answer = result->getResult()->getRaw();
    answer.resize(matched.size());

    std::copy(matched.begin(), matched.end(), answer.begin());
}

void NLExecutor::runCountSubqueryTally(NLExecutionContext* context, NLFunctionData* data) {
    NLCountSubqueryTallyData* tally = static_cast<NLCountSubqueryTallyData*>(data);
    NLCountSubqueryState* state = tally->getState();

    const ColumnVector<uint64_t>* tag = tally->getTag();
    if (!tag) {
        const Column* rows = tally->getRows();
        state->addRows(untaggedRow(tally->getRow()), rows->size());

        return;
    }

    state->addTaggedRows(tag->getRaw());
}

void NLExecutor::runCountSubqueryResult(NLExecutionContext* context, NLFunctionData* data) {
    NLCountSubqueryResultData* result = static_cast<NLCountSubqueryResultData*>(data);
    const NLCountSubqueryState* state = result->getState();

    const std::vector<uint64_t>& counts = state->getCounts();

    std::vector<uint64_t>& answer = result->getResult()->getRaw();
    answer.resize(counts.size());

    std::copy(counts.begin(), counts.end(), answer.begin());
}

void NLExecutor::runCollectReset(NLExecutionContext* context, NLFunctionData* data) {
    const NLCollectResetData* reset = static_cast<NLCollectResetData*>(data);
    reset->getState()->reset();
}

void NLExecutor::runCollectUpdate(NLExecutionContext* context, NLFunctionData* data) {
    NLCollectUpdateData* update = static_cast<NLCollectUpdateData*>(data);
    NLCollectState* state = update->getState();

    NLGroupTable& groupTable = state->groupTable();
    const std::vector<NLGroupTable::KeyColumn>& keyColumns = groupTable.keyColumns();
    std::vector<NLCollectState::ValueColumn>& valueColumns = state->valueColumns();

    // Every column is row-aligned. With grouping keys the first key sizes this step's
    // rows; ungrouped (no key), the first collected value column sizes it.
    const size_t rowCount = keyColumns.empty() ? valueColumns.front()._input->size()
                                               : keyColumns.front()._input->size();

    // That alignment is this step's precondition, not a property of the op's types:
    // the group assignment is computed once per key row, then indexed by the fold as it
    // walks the value column, so a column of another length appends values to the wrong
    // group - or, when the value column is the longer one, reads past the assignments
    // entirely. Generated IR places the update where every column is bound together, so
    // a mismatch here means malformed IR.
    for (const NLGroupTable::KeyColumn& keyColumn : keyColumns) {
        bioassert(keyColumn._input->size() == rowCount,
                  "nl.collect_update grouping keys must be row-aligned");
    }

    for (const NLCollectState::ValueColumn& valueColumn : valueColumns) {
        bioassert(valueColumn._input->size() == rowCount,
                  "nl.collect_update value columns must be row-aligned with the grouping keys");
    }

    NLGroupAggregates& aggregates = state->aggregates();
    for (const NLGroupAggregates::Aggregate& aggregate : aggregates.getAggregates()) {
        bioassert(aggregate._input->size() == rowCount,
                  "nl.collect_update aggregate inputs must be row-aligned with the grouping keys");
    }

    if (rowCount == 0) {
        return;
    }

    groupTable.assign(rowCount);

    const size_t groupCount = groupTable.getGroupCount();
    const ColumnVector<size_t>* rowGroups = groupTable.getRowGroupColumn();
    const std::vector<size_t>& groupIndices = rowGroups->getRaw();

    // Grow the per-group position lists to the new group count, then append this step's
    // present values to their groups' lists.
    for (NLCollectState::ValueColumn& valueColumn : valueColumns) {
        valueColumn._groupPositions.resize(groupCount);

        if (valueColumn._distinct) {
            valueColumn._distinct->assign(rowGroups, valueColumn._input);
        }

        valueColumn._fold(valueColumn._buffer,
                          valueColumn._input,
                          groupIndices,
                          valueColumn._groupPositions,
                          valueColumn._distinct,
                          state->collectedValues());
    }

    // The reductions taken over the same groups fold beside the list, off the group
    // assignment computed once above, exactly as nl.group_aggregate_update folds them.
    aggregates.grow(groupCount);
    aggregates.fold(rowGroups);
    aggregates.compactValues();
}

NLCollectFoldFunction NLExecutor::selectCollectFold(NLChunkKind kind) {
    NLCollectFoldFunction selected = nullptr;
    dispatchCollectedChunkKind(kind, [&]<typename Element>() { selected = &collectFold<Element>; });

    return selected;
}

// The collect(DISTINCT x) fold for a column of this kind: collect's own domain, with each of
// a group's values buffered once
NLCollectFoldFunction NLExecutor::selectCollectDistinctFold(NLChunkKind kind) {
    NLCollectFoldFunction selected = nullptr;
    dispatchCollectedChunkKind(kind, [&]<typename Element>() { selected = &collectFoldDistinct<Element>; });

    return selected;
}

NLUnwindCollectValueEmitFunction NLExecutor::selectUnwindCollectValueEmit(NLChunkKind kind) {
    NLUnwindCollectValueEmitFunction selected = nullptr;
    dispatchCollectedChunkKind(kind, [&]<typename Element>() { selected = &unwindCollectValueEmit<Element>; });

    return selected;
}

NLCollectListEmitFunction NLExecutor::selectCollectListEmit(NLChunkKind kind) {
    NLCollectListEmitFunction selected = nullptr;
    dispatchCollectedChunkKind(kind, [&]<typename Element>() { selected = &collectListEmit<Element>; });

    return selected;
}

void NLExecutor::selectCollectEntityHandlers(NLChunkKind kind,
                                             bool distinctValues,
                                             NLCollectFoldFunction& fold,
                                             NLCollectListEmitFunction& listEmit) {
    switch (kind) {
        case NLChunkKind::NodeID:
            return selectCollectIDHandlers<NodeID>(distinctValues, fold, listEmit);
        break;

        case NLChunkKind::EdgeID:
            return selectCollectIDHandlers<EdgeID>(distinctValues, fold, listEmit);
        break;

        default:
            throw IRException("collect does not support this chunk kind");
        break;
    }
}

// A list cell is present in every row, so it folds with no null to drop; only the dedup
// differs, keying on the elements rather than on the cell.
template <typename Container>
void NLExecutor::selectCollectContainerHandlers(bool distinctValues,
                                                NLCollectFoldFunction& fold,
                                                NLCollectListEmitFunction& listEmit) {
    fold = distinctValues ? &collectContainerFoldDistinct<Container> : &collectCellFold<Container>;
    listEmit = &collectListEmit<Container>;
}

template <typename Container>
void NLExecutor::selectCollectOptContainerHandlers(bool distinctValues,
                                                   NLCollectFoldFunction& fold,
                                                   NLCollectListEmitFunction& listEmit) {
    fold = distinctValues ? &collectOptContainerFoldDistinct<Container> : &collectOptContainerFold<Container>;
    listEmit = &collectListEmit<Container>;
}

template void NLExecutor::selectCollectContainerHandlers<ListView>(bool distinctValues,
                                                                   NLCollectFoldFunction& fold,
                                                                   NLCollectListEmitFunction& listEmit);
template void NLExecutor::selectCollectContainerHandlers<MapView>(bool distinctValues,
                                                                  NLCollectFoldFunction& fold,
                                                                  NLCollectListEmitFunction& listEmit);
template void NLExecutor::selectCollectOptContainerHandlers<ListView>(bool distinctValues,
                                                                      NLCollectFoldFunction& fold,
                                                                      NLCollectListEmitFunction& listEmit);
template void NLExecutor::selectCollectOptContainerHandlers<MapView>(bool distinctValues,
                                                                     NLCollectFoldFunction& fold,
                                                                     NLCollectListEmitFunction& listEmit);

// A type-erased cell carries its own type, so the fold drops the ones tagged null and the
// emit writes each survivor back under the type its tag names.
void NLExecutor::selectCollectTaggedHandlers(bool distinctValues,
                                             NLCollectFoldFunction& fold,
                                             NLCollectListEmitFunction& listEmit) {
    fold = distinctValues ? &collectTaggedFoldDistinct
                          : &collectTaggedFold;
    listEmit = &collectTaggedListEmit;
}

void NLExecutor::selectCollectPathHandlers(bool distinctValues,
                                           NLCollectFoldFunction& fold,
                                           NLCollectListEmitFunction& listEmit) {
    fold = distinctValues ? &collectPathFoldDistinct : &collectPathFold;
    listEmit = &collectTaggedListEmit;
}

void NLExecutor::runUnwindCollectLoop(NLExecutionContext* context, NLFunctionData* data) {
    NLUnwindCollectLoopData* loopData = static_cast<NLUnwindCollectLoopData*>(data);
    NLCollectState* state = loopData->getState();

    const size_t chunkSize = context->getChunkSize();
    const NLStmtContainer* loopBody = loopData->getStmts();

    NLGroupTable& groupTable = state->groupTable();
    NLCollectState::ValueColumn& unwound = state->unwoundColumn();
    const std::vector<std::vector<size_t>>& groupPositions = unwound._groupPositions;
    const size_t totalGroups = groupPositions.size();

    ColumnVector<size_t>* groupIndices = loopData->getGroupIndices();
    ColumnVector<size_t>* positions = loopData->getPositions();

    // Walk every (group, element) pair in group order. currentGroup / indexInGroup is
    // the cursor into that flattened sequence; empty groups (all values null) are
    // skipped, so they contribute no row - matching UNWIND of an empty list.
    size_t currentGroup = 0;
    size_t indexInGroup = 0;
    while (currentGroup < totalGroups && groupPositions[currentGroup].empty()) {
        currentGroup++;
    }

    while (currentGroup < totalGroups) {
        std::vector<size_t>& groupIndicesRaw = groupIndices->getRaw();
        std::vector<size_t>& positionsRaw = positions->getRaw();
        groupIndicesRaw.clear();
        positionsRaw.clear();

        // Fill up to chunkSize rows, each the next (group, element) pair.
        while (groupIndicesRaw.size() < chunkSize && currentGroup < totalGroups) {
            const std::vector<size_t>& groupPos = groupPositions[currentGroup];

            groupIndicesRaw.push_back(currentGroup);
            positionsRaw.push_back(groupPos[indexInGroup]);
            indexInGroup++;

            if (indexInGroup == groupPos.size()) {
                indexInGroup = 0;
                currentGroup++;
                while (currentGroup < totalGroups && groupPositions[currentGroup].empty()) {
                    currentGroup++;
                }
            }
        }

        // The key values repeat once per element (gather by the per-row group index);
        // the element values come from the flat buffer at this chunk's positions.
        groupTable.gatherKeys(groupIndices);

        unwound._unwindCollectEmit(unwound._buffer, positions, unwound._output);

        runBody(context, loopBody);
    }
}

void NLExecutor::runCollectLoop(NLExecutionContext* context, NLFunctionData* data) {
    NLCollectLoopData* loopData = static_cast<NLCollectLoopData*>(data);
    NLCollectState* state = loopData->getState();

    NLGroupTable& groupTable = state->groupTable();
    const size_t totalGroups = groupTable.getGroupCount();
    const size_t chunkSize = context->getChunkSize();
    const NLStmtContainer* loopBody = loopData->getStmts();

    NLGroupAggregates& aggregates = state->aggregates();
    NLStepBuffers* const stepBuffers = loopData->getStepBuffers();

    // Re-chunk the groups: each step slices the next chunk of key values from the key
    // buffers and materializes one list cell per group (a ListView over that group's
    // run in the list buffer), then runs the body. A grouped collect over no row has no
    // group, so the body runs zero times and it emits nothing; an ungrouped collect
    // always carries the single group the reset created, so it emits exactly one row -
    // holding the empty list when no value was folded.
    for (size_t offset = 0; offset < totalGroups; offset += chunkSize) {
        const size_t stepGroups = std::min(chunkSize, totalGroups - offset);

        groupTable.emitKeys(offset, stepGroups);

        for (NLCollectState::ValueColumn& valueColumn : state->valueColumns()) {
            valueColumn._listEmit(valueColumn._buffer,
                                  valueColumn._groupPositions,
                                  offset,
                                  stepGroups,
                                  state->listBuffer(),
                                  valueColumn._output);
        }

        stepBuffers->clear();
        aggregates.emit(offset, stepGroups, stepBuffers);

        runBody(context, loopBody);
    }
}

NLFillNullFunction NLExecutor::selectFillNullFunction(NLChunkKind kind) {
    NLFillNullFunction selected = nullptr;
    dispatchChunkKind(kind, [&]<typename ElementType>() { selected = &fillNullColumn<ElementType>; });

    return selected;
}

NLFillNullFunction NLExecutor::selectOptFillNullFunction(NLChunkKind kind) {
    NLFillNullFunction fill = nullptr;
    dispatchOptChunkKind(kind, [&]<typename Element>() { fill = &fillNullOptColumn<Element>; });

    return fill;
}

NLFillNullFunction NLExecutor::selectOptListFillNull() {
    return &fillNullOptColumn<ListView>;
}

NLFillNullFunction NLExecutor::selectCellFillNull() {
    return &resetTaggedCells;
}

NLGatherFunction NLExecutor::selectGatherFunction(NLChunkKind kind) {
    NLGatherFunction selected = nullptr;
    dispatchChunkKind(kind, [&]<typename ElementType>() { selected = &gatherColumn<ElementType>; });

    return selected;
}

NLGatherFunction NLExecutor::selectCountGatherFunction() {
    return &gatherColumn<uint64_t>;
}

NLBroadcastFunction NLExecutor::selectBlockRepeatFunction(NLChunkKind kind) {
    NLBroadcastFunction selected = nullptr;
    dispatchChunkKind(kind, [&]<typename ElementType>() { selected = &blockRepeatColumn<ElementType>; });

    return selected;
}

NLBroadcastFunction NLExecutor::selectCountBlockRepeatFunction() {
    return &blockRepeatColumn<uint64_t>;
}

NLBroadcastFunction NLExecutor::selectConstBlockRepeatFunction() {
    return &blockRepeatConstColumn;
}

NLBroadcastFunction NLExecutor::selectTileFunction(NLChunkKind kind) {
    NLBroadcastFunction selected = nullptr;
    dispatchChunkKind(kind, [&]<typename ElementType>() { selected = &tileColumn<ElementType>; });

    return selected;
}

NLBroadcastConstantFunction NLExecutor::selectNullConstantBroadcast() {
    return &broadcastNullColumn;
}

NLBroadcastConstantFunction NLExecutor::selectConstantListBroadcast() {
    return &broadcastConstantPlainColumn<ListView>;
}

NLBroadcastConstantFunction NLExecutor::selectConstantMapBroadcast() {
    return &broadcastConstantPlainColumn<MapView>;
}

NLBroadcastConstantFunction NLExecutor::selectConstantCellBroadcast() {
    return &broadcastConstantPlainColumn<CellView>;
}

NLBroadcastConstantFunction NLExecutor::selectConstantPlainBroadcast(NLChunkKind kind, const Column* value) {
    bioassert(value->getContainerKind() == ContainerKind::code<ColumnConst>(),
              "A boolean, a node, an edge, a path or a walk laid out over rows is read as the constant nl.row_constant made of it");

    NLBroadcastConstantFunction fill = nullptr;
    dispatchChunkKind(kind, [&]<typename Element>() { fill = &broadcastConstantPlainColumn<Element>; });

    return fill;
}

NLBroadcastConstantFunction NLExecutor::selectConstantBroadcast(NLChunkKind kind, const Column* value) {
    const bool isConst = value->getContainerKind() == ContainerKind::code<ColumnConst>();

    NLBroadcastConstantFunction fill = nullptr;
    dispatchOptChunkKind(kind, [&]<typename Element>() {
        if (!isConst) {
            fill = &broadcastSingleRowColumn<Element>;
        } else if (value->getKind() == ColumnConst<std::optional<Element>>::staticKind()) {
            fill = &broadcastNullableConstantColumn<Element>;
        } else {
            fill = &broadcastConstantColumn<Element>;
        }
    });

    return fill;
}

NLUnaryFn NLExecutor::selectRowConstant(NLChunkKind kind, const Column* row, LocalMemory* memory, Column*& result) {
    NLUnaryFn read = nullptr;
    dispatchChunkKind(kind, [&]<typename Element>() { read = selectRowConstantOf<Element>(row, memory, result); });

    return read;
}

NLUnaryFn NLExecutor::selectOptRowConstant(NLChunkKind kind, const Column* row, LocalMemory* memory, Column*& result) {
    NLUnaryFn read = nullptr;
    dispatchOptChunkKind(kind, [&]<typename Element>() {
        read = selectOptRowConstantOf<Element>(row, memory, result);
    });

    return read;
}

NLUnaryFn NLExecutor::selectCellRowConstant(const Column* row, LocalMemory* memory, Column*& result) {
    return selectRowConstantOf<CellView>(row, memory, result);
}

// A !storage.any chunk is a ColumnVector<CellView> of fixed-width tagged
// scalars, so the same broadcast templates carry the tag along with the value.
NLBroadcastFunction NLExecutor::selectCellBlockRepeatFunction() {
    return &blockRepeatColumn<CellView>;
}

NLBroadcastFunction NLExecutor::selectCellTileFunction() {
    return &tileColumn<CellView>;
}

NLAppendFunction NLExecutor::selectCellAppendFunction() {
    return &appendColumn<CellView>;
}

NLGatherFunction NLExecutor::selectCellGatherFunction() {
    return &gatherColumn<CellView>;
}

NLCompareFunction NLExecutor::selectCellCompareFunction() {
    return &compareCellColumn;
}

NLKeyAppendFunction NLExecutor::selectCellKeyAppendFunction() {
    return &distinctKeyAppendCellColumn;
}

NLGroupKeyGatherFunction NLExecutor::selectCellGroupKeyGatherFunction() {
    return &groupGatherAppendColumn<CellView>;
}

NLCopyFunction NLExecutor::selectCellCopyFunction() {
    return &copyRangeColumn<CellView>;
}

NLCompareFunction NLExecutor::selectListCompareFunction() {
    return &compareContainerColumn<ListView>;
}

// The nullable list family, which labels() produces: a node an OPTIONAL MATCH did not
// match holds no list, where a collected list column holds one in every row.
NLBroadcastFunction NLExecutor::selectOptListBlockRepeatFunction() {
    return &blockRepeatOptColumn<ListView>;
}

NLBroadcastFunction NLExecutor::selectOptListTileFunction() {
    return &tileOptColumn<ListView>;
}

NLAppendFunction NLExecutor::selectOptListAppendFunction() {
    return &appendOptColumn<ListView>;
}

NLOwnedAppendFunction NLExecutor::selectOwnedAppendFunction(const Column* buffer) {
    NLOwnedAppendFunction append = nullptr;
    dispatchViewColumn(buffer, Overloaded {
        [&]<typename ElementType>(const ColumnVector<ElementType>* column) { append = &appendOwnedColumn<ElementType>; },
        [&]<typename Element>(const ColumnOptVector<Element>* column) { append = &appendOwnedOptColumn<Element>; },
    });

    return append;
}

NLReownFunction NLExecutor::selectReownFunction(const Column* column) {
    NLReownFunction reown = nullptr;
    dispatchViewColumn(column, Overloaded {
        [&]<typename ElementType>(const ColumnVector<ElementType>* viewColumn) { reown = &reownColumn<ColumnVector<ElementType>>; },
        [&]<typename Element>(const ColumnOptVector<Element>* viewColumn) { reown = &reownColumn<ColumnOptVector<Element>>; },
    });

    return reown;
}

NLReownRowsFunction NLExecutor::selectReownRowsFunction(const Column* column) {
    NLReownRowsFunction reown = nullptr;
    dispatchViewColumn(column, Overloaded {
        [&]<typename ElementType>(const ColumnVector<ElementType>* viewColumn) { reown = &reownColumnRows<ColumnVector<ElementType>>; },
        [&]<typename Element>(const ColumnOptVector<Element>* viewColumn) { reown = &reownColumnRows<ColumnOptVector<Element>>; },
    });

    return reown;
}

NLGatherFunction NLExecutor::selectOptListGatherFunction() {
    return &gatherOptColumn<ListView>;
}

NLCompareFunction NLExecutor::selectOptListCompareFunction() {
    return &compareOptContainerColumn<ListView>;
}

NLKeyAppendFunction NLExecutor::selectOptListKeyAppendFunction() {
    return &distinctKeyAppendOptListColumn;
}

NLGroupKeyGatherFunction NLExecutor::selectOptListGroupKeyGatherFunction() {
    return &groupGatherAppendOptColumn<ListView>;
}

NLCopyFunction NLExecutor::selectOptListCopyFunction() {
    return &copyRangeOptColumn<ListView>;
}

NLCopyFunction NLExecutor::selectCopyFunction(NLChunkKind kind) {
    NLCopyFunction selected = nullptr;
    dispatchChunkKind(kind, [&]<typename ElementType>() { selected = &copyRangeColumn<ElementType>; });

    return selected;
}

NLCopyFunction NLExecutor::selectCountCopyFunction() {
    return &copyRangeColumn<uint64_t>;
}

NLCopyFunction NLExecutor::selectConstCopyFunction() {
    return &copyRangeConstColumn;
}

NLGatherFunction NLExecutor::selectConstGatherFunction() {
    return &gatherConstColumn;
}

NLGatherFunction NLExecutor::selectOptGatherFunction(NLChunkKind kind) {
    NLGatherFunction gather = nullptr;
    dispatchOptChunkKind(kind, [&]<typename Element>() { gather = &gatherOptColumn<Element>; });

    return gather;
}

NLAppendFunction NLExecutor::selectOptAppendFunction(NLChunkKind kind) {
    NLAppendFunction append = nullptr;
    dispatchOptChunkKind(kind, [&]<typename Element>() { append = &appendOptColumn<Element>; });

    return append;
}

NLCopyFunction NLExecutor::selectOptCopyFunction(NLChunkKind kind) {
    NLCopyFunction copy = nullptr;
    dispatchOptChunkKind(kind, [&]<typename Element>() { copy = &copyRangeOptColumn<Element>; });

    return copy;
}

NLGroupKeyGatherFunction NLExecutor::selectOptGroupKeyGather(NLChunkKind kind) {
    NLGroupKeyGatherFunction gather = nullptr;
    dispatchOptChunkKind(kind, [&]<typename Element>() { gather = &groupGatherAppendOptColumn<Element>; });

    return gather;
}

NLBroadcastFunction NLExecutor::selectOptBlockRepeatFunction(NLChunkKind kind) {
    NLBroadcastFunction broadcast = nullptr;
    dispatchOptChunkKind(kind, [&]<typename Element>() { broadcast = &blockRepeatOptColumn<Element>; });

    return broadcast;
}

NLBroadcastFunction NLExecutor::selectOptTileFunction(NLChunkKind kind) {
    NLBroadcastFunction broadcast = nullptr;
    dispatchOptChunkKind(kind, [&]<typename Element>() { broadcast = &tileOptColumn<Element>; });

    return broadcast;
}

NLAppendFunction NLExecutor::selectAppendFunction(NLChunkKind kind) {
    NLAppendFunction selected = nullptr;
    dispatchChunkKind(kind, [&]<typename ElementType>() { selected = &appendColumn<ElementType>; });

    return selected;
}

NLAppendFunction NLExecutor::selectCountAppendFunction() {
    return &appendColumn<uint64_t>;
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

        case NLChunkKind::LabelSetID:
            return &distinctKeyAppendColumn<LabelSetID>;
        break;

        case NLChunkKind::LabelID:
            return &distinctKeyAppendColumn<LabelID>;
        break;

        case NLChunkKind::PropertyTypeID:
            return &distinctKeyAppendColumn<PropertyTypeID>;
        break;

        case NLChunkKind::ValueTypeCode:
            return &distinctKeyAppendPlainColumn<ValueType>;
        break;

        case NLChunkKind::UInt64:
            return &distinctKeyAppendPlainColumn<types::UInt64::Primitive>;
        break;

        case NLChunkKind::Int64:
            return &distinctKeyAppendPlainColumn<types::Int64::Primitive>;
        break;

        case NLChunkKind::Double:
            return &distinctKeyAppendPlainColumn<types::Double::Primitive>;
        break;

        case NLChunkKind::Bool:
            return &distinctKeyAppendPlainColumn<types::Bool::Primitive>;
        break;

        case NLChunkKind::String:
            return &distinctKeyAppendPlainColumn<types::String::Primitive>;
        break;

        case NLChunkKind::Embedding:
            return &distinctKeyAppendPlainColumn<types::Embedding::Primitive>;
        break;

        case NLChunkKind::List:
            return &distinctKeyAppendListColumn;
        break;

        case NLChunkKind::Map:
            return &distinctKeyAppendMapColumn;
        break;

        case NLChunkKind::Path:
            return &distinctKeyAppendPathColumn;
        break;

        case NLChunkKind::DateTime:
            return &distinctKeyAppendPlainColumn<types::DateTime::Primitive>;
        break;

        case NLChunkKind::Duration:
            return &distinctKeyAppendPlainColumn<types::Duration::Primitive>;
        break;

        case NLChunkKind::Point:
            return &distinctKeyAppendPlainColumn<types::Point::Primitive>;
        break;

        case NLChunkKind::PathRef:
            throw IRException("A path column cannot be a DISTINCT or grouping key: expand it into its list first");
        break;
    }

    bioassert(false, "Unknown NLChunkKind");
    return nullptr;
}

NLKeyAppendFunction NLExecutor::selectOptKeyAppendFunction(NLChunkKind kind) {
    switch (kind) {
        case NLChunkKind::EdgeTypeID:
            return &distinctKeyAppendOptColumn<EdgeTypeID>;
        break;

        case NLChunkKind::LabelID:
            return &distinctKeyAppendOptColumn<LabelID>;
        break;

        case NLChunkKind::PropertyTypeID:
            return &distinctKeyAppendOptColumn<PropertyTypeID>;
        break;

        case NLChunkKind::ValueTypeCode:
            return &distinctKeyAppendOptColumn<ValueType>;
        break;

        case NLChunkKind::Int64:
            return &distinctKeyAppendOptColumn<types::Int64::Primitive>;
        break;

        case NLChunkKind::UInt64:
            return &distinctKeyAppendOptColumn<types::UInt64::Primitive>;
        break;

        case NLChunkKind::Double:
            return &distinctKeyAppendOptColumn<types::Double::Primitive>;
        break;

        case NLChunkKind::Bool:
            return &distinctKeyAppendOptColumn<types::Bool::Primitive>;
        break;

        case NLChunkKind::String:
            return &distinctKeyAppendOptColumn<types::String::Primitive>;
        break;

        case NLChunkKind::Embedding:
            return &distinctKeyAppendOptColumn<types::Embedding::Primitive>;
        break;

        case NLChunkKind::List:
            return &distinctKeyAppendOptListColumn;
        break;

        case NLChunkKind::Map:
            return &distinctKeyAppendOptMapColumn;
        break;

        case NLChunkKind::DateTime:
            return &distinctKeyAppendOptColumn<types::DateTime::Primitive>;
        break;

        case NLChunkKind::Duration:
            return &distinctKeyAppendOptColumn<types::Duration::Primitive>;
        break;

        case NLChunkKind::Point:
            return &distinctKeyAppendOptColumn<types::Point::Primitive>;
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

NLKeyPacking NLExecutor::selectKeyPacking(NLChunkKind kind) {
    switch (kind) {
        case NLChunkKind::NodeID:
            return keyPacking<NodeID>();
        break;

        case NLChunkKind::EdgeID:
            return keyPacking<EdgeID>();
        break;

        case NLChunkKind::EdgeTypeID:
            return keyPacking<EdgeTypeID>();
        break;

        case NLChunkKind::LabelSetID:
            return keyPacking<LabelSetID>();
        break;

        case NLChunkKind::LabelID:
            return keyPacking<LabelID>();
        break;

        case NLChunkKind::PropertyTypeID:
            return keyPacking<PropertyTypeID>();
        break;

        case NLChunkKind::ValueTypeCode:
            return keyPacking<ValueType>();
        break;

        case NLChunkKind::UInt64:
            return keyPacking<types::UInt64::Primitive>();
        break;

        case NLChunkKind::Int64:
            return keyPacking<types::Int64::Primitive>();
        break;

        case NLChunkKind::Double:
            return keyPacking<types::Double::Primitive>();
        break;

        case NLChunkKind::Bool:
            return keyPacking<types::Bool::Primitive>();
        break;

        case NLChunkKind::DateTime:
            return keyPacking<types::DateTime::Primitive>();
        break;

        case NLChunkKind::Duration:
            return keyPacking<types::Duration::Primitive>();
        break;

        case NLChunkKind::Point:
            return keyPacking<types::Point::Primitive>();
        break;

        case NLChunkKind::String:
        case NLChunkKind::Embedding:
        case NLChunkKind::List:
        case NLChunkKind::Map:
        case NLChunkKind::Path:
        case NLChunkKind::PathRef:
            return NLKeyPacking {};
        break;
    }

    bioassert(false, "Unknown NLChunkKind");
    return NLKeyPacking {};
}

NLKeyPacking NLExecutor::selectOptKeyPacking(NLChunkKind kind) {
    switch (kind) {
        case NLChunkKind::EdgeTypeID:
            return optKeyPacking<EdgeTypeID>();
        break;

        case NLChunkKind::LabelID:
            return optKeyPacking<LabelID>();
        break;

        case NLChunkKind::PropertyTypeID:
            return optKeyPacking<PropertyTypeID>();
        break;

        case NLChunkKind::ValueTypeCode:
            return optKeyPacking<ValueType>();
        break;

        case NLChunkKind::Int64:
            return optKeyPacking<types::Int64::Primitive>();
        break;

        case NLChunkKind::UInt64:
            return optKeyPacking<types::UInt64::Primitive>();
        break;

        case NLChunkKind::Double:
            return optKeyPacking<types::Double::Primitive>();
        break;

        case NLChunkKind::Bool:
            return optKeyPacking<types::Bool::Primitive>();
        break;

        case NLChunkKind::DateTime:
            return optKeyPacking<types::DateTime::Primitive>();
        break;

        case NLChunkKind::Duration:
            return optKeyPacking<types::Duration::Primitive>();
        break;

        case NLChunkKind::Point:
            return optKeyPacking<types::Point::Primitive>();
        break;

        case NLChunkKind::String:
        case NLChunkKind::Embedding:
        case NLChunkKind::List:
        case NLChunkKind::Map:
            return NLKeyPacking {};
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
    return NLKeyPacking {};
}

NLGroupKeyGatherFunction NLExecutor::selectGroupKeyGather(NLChunkKind kind) {
    NLGroupKeyGatherFunction selected = nullptr;
    dispatchChunkKind(kind, [&]<typename ElementType>() { selected = &groupGatherAppendColumn<ElementType>; });

    return selected;
}

NLKeyAppendFunction NLExecutor::selectPlainMergeKeyAppendFunction(NLChunkKind kind, ValueType keyType) {
    switch (kind) {
        case NLChunkKind::UInt64:
            return selectNumericMergeKeyAppend<types::UInt64::Primitive, MergeColumnShape::Plain>(keyType);
        break;

        case NLChunkKind::Int64:
            return selectNumericMergeKeyAppend<types::Int64::Primitive, MergeColumnShape::Plain>(keyType);
        break;

        case NLChunkKind::Double:
            return selectNumericMergeKeyAppend<types::Double::Primitive, MergeColumnShape::Plain>(keyType);
        break;

        case NLChunkKind::Bool:
            throwUnlessKeyedAsItsOwnType(ValueType::Bool, keyType);
            return &mergeKeyAppendPlainColumn<types::Bool::Primitive>;
        break;

        case NLChunkKind::String:
            throwUnlessKeyedAsItsOwnType(ValueType::String, keyType);
            return &mergeKeyAppendPlainColumn<types::String::Primitive>;
        break;

        case NLChunkKind::NodeID:
        case NLChunkKind::EdgeID:
        case NLChunkKind::EdgeTypeID:
        case NLChunkKind::LabelSetID:
        case NLChunkKind::LabelID:
        case NLChunkKind::PropertyTypeID:
        case NLChunkKind::ValueTypeCode:
        case NLChunkKind::Embedding:
        case NLChunkKind::Path:
        case NLChunkKind::PathRef:
            throw IRException("a MERGE pattern cannot constrain a property to this value");
        break;

        case NLChunkKind::List:
            throwUnlessKeyedAsItsOwnType(ValueType::List, keyType);
            return &mergeKeyAppendPlainColumn<ListView>;
        break;

        case NLChunkKind::Map:
            throwUnlessKeyedAsItsOwnType(ValueType::Map, keyType);
            return &mergeKeyAppendPlainColumn<MapView>;
        break;

        case NLChunkKind::DateTime:
            throwUnlessKeyedAsItsOwnType(ValueType::DateTime, keyType);
            return &mergeKeyAppendPlainColumn<types::DateTime::Primitive>;
        break;

        case NLChunkKind::Duration:
            throwUnlessKeyedAsItsOwnType(ValueType::Duration, keyType);
            return &mergeKeyAppendPlainColumn<types::Duration::Primitive>;
        break;

        case NLChunkKind::Point:
            throwUnlessKeyedAsItsOwnType(ValueType::Point, keyType);
            return &mergeKeyAppendPlainColumn<types::Point::Primitive>;
        break;
    }

    bioassert(false, "Unknown NLChunkKind");
    return nullptr;
}

NLKeyAppendFunction NLExecutor::selectConstMergeKeyAppendFunction(ValueType valueType, ValueType keyType) {
    switch (valueType) {
        case ValueType::Int64:
            return selectNumericMergeKeyAppend<types::Int64::Primitive, MergeColumnShape::Const>(keyType);
        break;

        case ValueType::UInt64:
            return selectNumericMergeKeyAppend<types::UInt64::Primitive, MergeColumnShape::Const>(keyType);
        break;

        case ValueType::Double:
            return selectNumericMergeKeyAppend<types::Double::Primitive, MergeColumnShape::Const>(keyType);
        break;

        case ValueType::Bool:
            throwUnlessKeyedAsItsOwnType(ValueType::Bool, keyType);
            return &mergeKeyAppendConstColumn<types::Bool::Primitive>;
        break;

        case ValueType::String:
            throwUnlessKeyedAsItsOwnType(ValueType::String, keyType);
            return &mergeKeyAppendConstColumn<types::String::Primitive>;
        break;

        case ValueType::Embedding:
            throwUnlessKeyedAsItsOwnType(ValueType::Embedding, keyType);
            return &mergeKeyAppendConstColumn<types::Embedding::Primitive>;
        break;

        case ValueType::List:
            throwUnlessKeyedAsItsOwnType(ValueType::List, keyType);
            return &mergeKeyAppendConstColumn<ListView>;
        break;

        case ValueType::DateTime:
            throwUnlessKeyedAsItsOwnType(ValueType::DateTime, keyType);
            return &mergeKeyAppendConstColumn<types::DateTime::Primitive>;
        break;

        case ValueType::Duration:
            throwUnlessKeyedAsItsOwnType(ValueType::Duration, keyType);
            return &mergeKeyAppendConstColumn<types::Duration::Primitive>;
        break;

        case ValueType::Point:
            throwUnlessKeyedAsItsOwnType(ValueType::Point, keyType);
            return &mergeKeyAppendConstColumn<types::Point::Primitive>;
        break;

        case ValueType::Map:
            throwUnlessKeyedAsItsOwnType(ValueType::Map, keyType);
            return &mergeKeyAppendConstColumn<MapView>;
        break;

        case ValueType::Invalid:
        case ValueType::_SIZE:
            throw IRException("invalid MERGE property value type");
        break;
    }

    bioassert(false, "Unhandled value type");
    return nullptr;
}

NLKeyAppendFunction NLExecutor::selectOptMergeKeyAppendFunction(NLChunkKind kind, ValueType keyType) {
    switch (kind) {
        case NLChunkKind::Int64:
            return selectNumericMergeKeyAppend<types::Int64::Primitive, MergeColumnShape::Opt>(keyType);
        break;

        case NLChunkKind::UInt64:
            return selectNumericMergeKeyAppend<types::UInt64::Primitive, MergeColumnShape::Opt>(keyType);
        break;

        case NLChunkKind::Double:
            return selectNumericMergeKeyAppend<types::Double::Primitive, MergeColumnShape::Opt>(keyType);
        break;

        case NLChunkKind::Bool:
            throwUnlessKeyedAsItsOwnType(ValueType::Bool, keyType);
            return selectOptKeyAppendFunction(kind);
        break;

        case NLChunkKind::String:
            throwUnlessKeyedAsItsOwnType(ValueType::String, keyType);
            return selectOptKeyAppendFunction(kind);
        break;

        case NLChunkKind::Embedding:
            throwUnlessKeyedAsItsOwnType(ValueType::Embedding, keyType);
            return selectOptKeyAppendFunction(kind);
        break;

        case NLChunkKind::List:
            throwUnlessKeyedAsItsOwnType(ValueType::List, keyType);
            return selectOptKeyAppendFunction(kind);
        break;

        case NLChunkKind::Map:
            throwUnlessKeyedAsItsOwnType(ValueType::Map, keyType);
            return selectOptKeyAppendFunction(kind);
        break;

        case NLChunkKind::DateTime:
            throwUnlessKeyedAsItsOwnType(ValueType::DateTime, keyType);
            return selectOptKeyAppendFunction(kind);
        break;

        case NLChunkKind::Duration:
            throwUnlessKeyedAsItsOwnType(ValueType::Duration, keyType);
            return selectOptKeyAppendFunction(kind);
        break;

        case NLChunkKind::Point:
            throwUnlessKeyedAsItsOwnType(ValueType::Point, keyType);
            return selectOptKeyAppendFunction(kind);
        break;

        case NLChunkKind::NodeID:
        case NLChunkKind::EdgeID:
        case NLChunkKind::EdgeTypeID:
        case NLChunkKind::LabelSetID:
        case NLChunkKind::LabelID:
        case NLChunkKind::PropertyTypeID:
        case NLChunkKind::ValueTypeCode:
        case NLChunkKind::Path:
        case NLChunkKind::PathRef:
            throw IRException("a MERGE pattern cannot constrain a property to this value");
        break;
    }

    bioassert(false, "Unknown NLChunkKind");
    return nullptr;
}

NLKeyAppendFunction NLExecutor::selectNullMergeKeyAppendFunction() {
    return &mergeKeyAppendConstNull;
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

        case NLChunkKind::LabelSetID:
            return &compareColumn<LabelSetID>;
        break;

        case NLChunkKind::LabelID:
            return &compareColumn<LabelID>;
        break;

        case NLChunkKind::PropertyTypeID:
            return &compareColumn<PropertyTypeID>;
        break;

        case NLChunkKind::ValueTypeCode:
            return &compareColumn<ValueType>;
        break;

        case NLChunkKind::UInt64:
            return &compareColumn<types::UInt64::Primitive>;
        break;

        case NLChunkKind::Int64:
            return &compareColumn<types::Int64::Primitive>;
        break;

        case NLChunkKind::Double:
            return &compareColumn<types::Double::Primitive>;
        break;

        case NLChunkKind::Bool:
            return &compareColumn<types::Bool::Primitive>;
        break;

        case NLChunkKind::String:
            return &compareColumn<types::String::Primitive>;
        break;

        case NLChunkKind::Embedding:
            throw IRException("cannot sort by an embedding column");
        break;

        case NLChunkKind::List:
            return &compareContainerColumn<ListView>;
        break;

        case NLChunkKind::Map:
            return &compareContainerColumn<MapView>;
        break;

        case NLChunkKind::Path:
            return &comparePathColumn;
        break;

        case NLChunkKind::DateTime:
            return &compareColumn<types::DateTime::Primitive>;
        break;

        case NLChunkKind::Duration:
            return &compareColumn<types::Duration::Primitive>;
        break;

        case NLChunkKind::Point:
            return &compareColumn<types::Point::Primitive>;
        break;

        case NLChunkKind::PathRef:
            throw IRException("A path column cannot be a sort key: expand it into its list first");
        break;
    }

    bioassert(false, "Unknown NLChunkKind");
    return nullptr;
}

// A manual switch, not dispatchOptChunkKind, because an embedding has no order:
// dispatching would instantiate the comparator for std::span<const float>, which does not
// compile. The embedding case throws instead, so that instantiation is never named.
NLCompareFunction NLExecutor::selectOptCompareFunction(NLChunkKind kind) {
    switch (kind) {
        case NLChunkKind::EdgeTypeID:
            return &compareOptColumn<EdgeTypeID>;
        break;

        case NLChunkKind::LabelID:
            return &compareOptColumn<LabelID>;
        break;

        case NLChunkKind::PropertyTypeID:
            return &compareOptColumn<PropertyTypeID>;
        break;

        case NLChunkKind::ValueTypeCode:
            return &compareOptColumn<ValueType>;
        break;

        case NLChunkKind::Int64:
            return &compareOptColumn<types::Int64::Primitive>;
        break;

        case NLChunkKind::UInt64:
            return &compareOptColumn<types::UInt64::Primitive>;
        break;

        case NLChunkKind::Double:
            return &compareOptColumn<types::Double::Primitive>;
        break;

        case NLChunkKind::Bool:
            return &compareOptColumn<types::Bool::Primitive>;
        break;

        case NLChunkKind::String:
            return &compareOptColumn<types::String::Primitive>;
        break;

        case NLChunkKind::Embedding:
            throw IRException("cannot sort by an embedding column");
        break;

        case NLChunkKind::List:
            return &compareOptContainerColumn<ListView>;
        break;

        case NLChunkKind::Map:
            return &compareOptContainerColumn<MapView>;
        break;

        case NLChunkKind::DateTime:
            return &compareOptColumn<types::DateTime::Primitive>;
        break;

        case NLChunkKind::Duration:
            return &compareOptColumn<types::Duration::Primitive>;
        break;

        case NLChunkKind::Point:
            return &compareOptColumn<types::Point::Primitive>;
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

template NLUnaryFn NLExecutor::selectNullTest<true>(const Column* operand, LocalMemory* memory, Column*& result);
template NLUnaryFn NLExecutor::selectNullTest<false>(const Column* operand, LocalMemory* memory, Column*& result);

template NLBinaryFn NLExecutor::selectBinary<OP_ADD>(const Column* lhs, const Column* rhs, LocalMemory* memory, Column*& result);
template NLBinaryFn NLExecutor::selectBinary<OP_CONCAT>(const Column* lhs, const Column* rhs, LocalMemory* memory, Column*& result);
template NLBinaryFn NLExecutor::selectBinary<OP_INDEX>(const Column* lhs, const Column* rhs, LocalMemory* memory, Column*& result);
template NLBinaryFn NLExecutor::selectBinary<OP_SUB>(const Column* lhs, const Column* rhs, LocalMemory* memory, Column*& result);
template NLBinaryFn NLExecutor::selectBinary<OP_MUL>(const Column* lhs, const Column* rhs, LocalMemory* memory, Column*& result);
template NLBinaryFn NLExecutor::selectBinary<OP_DIV>(const Column* lhs, const Column* rhs, LocalMemory* memory, Column*& result);
template NLBinaryFn NLExecutor::selectBinary<OP_MOD>(const Column* lhs, const Column* rhs, LocalMemory* memory, Column*& result);
template NLBinaryFn NLExecutor::selectBinary<OP_POW>(const Column* lhs, const Column* rhs, LocalMemory* memory, Column*& result);
template NLBinaryFn NLExecutor::selectBinary<OP_EQUAL>(const Column* lhs, const Column* rhs, LocalMemory* memory, Column*& result);
template NLBinaryFn NLExecutor::selectBinary<OP_GREATER_THAN>(const Column* lhs, const Column* rhs, LocalMemory* memory, Column*& result);
template NLBinaryFn NLExecutor::selectBinary<OP_LESS_THAN>(const Column* lhs, const Column* rhs, LocalMemory* memory, Column*& result);
template NLBinaryFn NLExecutor::selectBinary<OP_GREATER_THAN_OR_EQUAL>(const Column* lhs, const Column* rhs, LocalMemory* memory, Column*& result);
template NLBinaryFn NLExecutor::selectBinary<OP_LESS_THAN_OR_EQUAL>(const Column* lhs, const Column* rhs, LocalMemory* memory, Column*& result);
template NLBinaryFn NLExecutor::selectBinary<OP_AND>(const Column* lhs, const Column* rhs, LocalMemory* memory, Column*& result);
template NLBinaryFn NLExecutor::selectBinary<OP_OR>(const Column* lhs, const Column* rhs, LocalMemory* memory, Column*& result);
template NLBinaryFn NLExecutor::selectBinary<OP_NOT_EQUAL>(const Column* lhs, const Column* rhs, LocalMemory* memory, Column*& result);
template NLBinaryFn NLExecutor::selectBinary<OP_XOR>(const Column* lhs, const Column* rhs, LocalMemory* memory, Column*& result);
template NLBinaryFn NLExecutor::selectBinary<OP_STARTS_WITH>(const Column* lhs, const Column* rhs, LocalMemory* memory, Column*& result);
template NLBinaryFn NLExecutor::selectBinary<OP_ENDS_WITH>(const Column* lhs, const Column* rhs, LocalMemory* memory, Column*& result);
template NLBinaryFn NLExecutor::selectBinary<OP_CONTAINS>(const Column* lhs, const Column* rhs, LocalMemory* memory, Column*& result);
template NLBinaryFn NLExecutor::selectBinary<OP_IN>(const Column* lhs, const Column* rhs, LocalMemory* memory, Column*& result);
template NLBinaryFn NLExecutor::selectBinary<OP_INDEX_OF>(const Column* lhs, const Column* rhs, LocalMemory* memory, Column*& result);
template NLBinaryFn NLExecutor::selectBinary<OP_FUNC_COSINE_SIMILARITY>(const Column* lhs, const Column* rhs, LocalMemory* memory, Column*& result);
template NLBinaryFn NLExecutor::selectBinary<OP_FUNC_EUCLIDEAN_DISTANCE>(const Column* lhs, const Column* rhs, LocalMemory* memory, Column*& result);

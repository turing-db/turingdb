#include "NLExecutor.h"

#include <stdint.h>
#include <algorithm>
#include <span>
#include <string_view>
#include <type_traits>

#include <spdlog/fmt/bundled/format.h>

#include "columns/ContainerKind.h"
#include "ID.h"
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

#include "NLProgram.h"
#include "NLRuntimeProperty.h"
#include "NLOutputSink.h"
#include "NLColumnKernels.h"
#include "NLLoopSteps.h"
#include "NLSortOrder.h"

#include "LocalMemory.h"
#include "IRException.h"
#include "BioAssert.h"
#include "Overloaded.h"

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

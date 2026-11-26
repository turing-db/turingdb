#include "NLExecutor.h"

#include <span>
#include <string_view>
#include <type_traits>

#include "TypeUtils.h"
#include "columns/ContainerKind.h"
#include "ID.h"
#include "columns/ColumnOptVector.h"
#include "columns/ColumnVector.h"
#include "columns/ColumnConst.h"
#include "columns/Functions.h"
#include "CellOrder.h"
#include "metadata/PropertyNull.h"
#include "metadata/PropertyType.h"
#include "columns/ColumnOperand.h"

#include "versioning/CommitWriteBuffer.h"
#include "views/GraphView.h"

#include "NLProgram.h"
#include "NLRuntimeProperty.h"
#include "NLPropertyFunctions.h"
#include "NLColumnKernels.h"

#include "LocalMemory.h"
#include "IRException.h"
#include "BioAssert.h"

using namespace db;

namespace {

template <typename Functor>
Functor makeFunctor(NLExecutionContext* context, NLStepBuffers* memory) {
    if constexpr (std::is_constructible_v<Functor, NLExecutionContext*, NLStepBuffers*>) {
        return Functor(context, memory);
    } else if constexpr (std::is_constructible_v<Functor, GraphView, QueryListBuffer*, const CommitWriteBuffer*>) {
        return Functor(*context->getView(), &memory->listBuffer(), context->getWriteBuffer());
    } else if constexpr (std::is_constructible_v<Functor, GraphView, StringBuffer*, const CommitWriteBuffer*>) {
        return Functor(*context->getView(), &memory->stringBuffer(), context->getWriteBuffer());
    } else if constexpr (std::is_constructible_v<Functor, GraphView, const CommitWriteBuffer*>) {
        return Functor(*context->getView(), context->getWriteBuffer());
    } else if constexpr (std::is_constructible_v<Functor, GraphView>) {
        return Functor(*context->getView());
    } else if constexpr (std::is_constructible_v<Functor, QueryListBuffer*, StringBuffer*>) {
        return Functor(&memory->listBuffer(), &memory->stringBuffer());
    } else if constexpr (std::is_constructible_v<Functor, QueryListBuffer*>) {
        return Functor(&memory->listBuffer());
    } else if constexpr (std::is_constructible_v<Functor, StringBuffer*>) {
        return Functor(&memory->stringBuffer());
    } else {
        return Functor {};
    }
}

template <typename Functor>
void functionConstKernel(NLExecutionContext* context, Column* result, const Column* input, NLStepBuffers* memory) {
    using Arg = typename Functor::ArgType;
    using Res = typename Functor::ResultType;

    ColumnConst<Res>* output = static_cast<ColumnConst<Res>*>(result);
    Functor functor = makeFunctor<Functor>(context, memory);

    bioassert(input->getKind() == ColumnConst<Arg>::staticKind(), "Function operand has an unexpected column type.");
    const ColumnConst<Arg>* typedInput = static_cast<const ColumnConst<Arg>*>(input);

    writeConstant(output, functor(typedInput->getRaw()));
}

// The constant whose single cell can be absent: the functor is handed the optional and
// answers the absence itself, as functionNullReadingKernel hands it one per row.
template <typename Functor>
void functionOptConstKernel(NLExecutionContext* context, Column* result, const Column* input, NLStepBuffers* memory) {
    using Arg = typename Functor::ArgType;
    using Res = typename Functor::ResultType;

    bioassert(input->getKind() == ColumnConst<std::optional<Arg>>::staticKind(), "Function operand has an unexpected column type.");
    const ColumnConst<std::optional<Arg>>* typedInput = static_cast<const ColumnConst<std::optional<Arg>>*>(input);
    ColumnConst<Res>* output = static_cast<ColumnConst<Res>*>(result);

    Functor functor = makeFunctor<Functor>(context, memory);
    output->set(functor(typedInput->get(0)));
}

template <typename Functor, typename Element>
void applyFunctionOverNullableConst(Functor& functor,
                                    const ColumnConst<std::optional<Element>>* input,
                                    ColumnConst<std::optional<TypeUtils::unwrap_optional_t<typename Functor::ResultType>>>* output) {
    if (!input->isValid(0)) {
        output->setOptional(std::nullopt);
        return;
    }

    output->setOptional(functor(input->getValue(0)));
}

// The same constant for a function that does not read its own nulls: an absent cell
// answers null, as functionOptKernel answers one per row.
template <typename Functor>
void functionNullableConstKernel(NLExecutionContext* context, Column* result, const Column* input, NLStepBuffers* memory) {
    using Arg = typename Functor::ArgType;
    using JustRes = TypeUtils::unwrap_optional_t<typename Functor::ResultType>;

    bioassert(input->getKind() == ColumnConst<std::optional<Arg>>::staticKind(), "Function operand has an unexpected column type.");
    const ColumnConst<std::optional<Arg>>* typedInput = static_cast<const ColumnConst<std::optional<Arg>>*>(input);
    ColumnConst<std::optional<JustRes>>* output = static_cast<ColumnConst<std::optional<JustRes>>*>(result);

    Functor functor = makeFunctor<Functor>(context, memory);
    applyFunctionOverNullableConst(functor, typedInput, output);
}

template <typename Arg>
bool isNullableConstantOf(const Column* input) {
    return input->getKind() == ColumnConst<std::optional<Arg>>::staticKind();
}

// A null constant argument converts to a null result whatever the function; the
// ColumnConst<PropertyNull> result already reads as null, so nothing is computed.
void functionNullKernel(NLExecutionContext*, Column*, const Column*, NLStepBuffers*) {
}

template <typename Functor, typename Element>
void applyFunctionOverVector(Functor& functor,
                             const ColumnVector<Element>* input,
                             ColumnVectorOf<typename Functor::ResultType>* output) {
    const auto& inputRaw = input->getRaw();
    const size_t size = inputRaw.size();

    resetForRowWrites(output, size);

    for (size_t row = 0; row < size; row++) {
        writeRow(output, row, functor(inputRaw[row]));
    }
}

// Whether a column holds one type-erased cell per row, which a list function reads
// through its tagged counterpart rather than as the list column it was written against.
bool readsTaggedCells(const Column* input) {
    const ColumnKind::Code kind = input->getKind();

    return kind == ColumnVector<CellView>::staticKind() || kind == ColumnConst<CellView>::staticKind();
}

template <typename Functor>
void functionVectorKernel(NLExecutionContext* context, Column* result, const Column* input, NLStepBuffers* memory) {
    using Arg = typename Functor::ArgType;
    using Res = typename Functor::ResultType;

    ColumnVectorOf<Res>* output = static_cast<ColumnVectorOf<Res>*>(result);
    Functor functor = makeFunctor<Functor>(context, memory);

    bioassert(input->getKind() == ColumnVector<Arg>::staticKind(), "Function operand has an unexpected column type.");
    const ColumnVector<Arg>* typedInput = static_cast<const ColumnVector<Arg>*>(input);

    applyFunctionOverVector(functor, typedInput, output);
}

// The nullable-input kernel of a function that reads its own nulls: the absent value goes
// to the functor as it is, so the result rides the plain column its answers always fill.
template <typename Functor>
void functionNullReadingKernel(NLExecutionContext* context, Column* result, const Column* input, NLStepBuffers* memory) {
    using Arg = typename Functor::ArgType;
    using Res = typename Functor::ResultType;

    bioassert(input->getKind() == ColumnOptVector<Arg>::staticKind(), "Function operand has an unexpected column type.");
    const ColumnOptVector<Arg>* typedInput = static_cast<const ColumnOptVector<Arg>*>(input);
    ColumnVectorOf<Res>* output = static_cast<ColumnVectorOf<Res>*>(result);

    const size_t size = typedInput->size();
    resetForRowWrites(output, size);

    Functor functor = makeFunctor<Functor>(context, memory);
    for (size_t row = 0; row < size; row++) {
        output->set(row, functor(typedInput->get(row)));
    }
}

// The kernel serving a column of type-erased cells: a cell per row, or the single cell a
// constant is. The functor reads the null a cell's own tag carries, so its answer fills a
// plain column either way.
template <typename Functor>
NLUnaryFunctionKernel selectTaggedCellFunction(const Column* input, LocalMemory* memory, Column*& result) {
    using Res = typename Functor::ResultType;

    if (input->getContainerKind() == ContainerKind::code<ColumnConst>()) {
        result = memory->alloc<ColumnConst<Res>>();
        return &functionConstKernel<Functor>;
    }

    result = memory->alloc<ColumnVectorOf<Res>>();
    return &functionVectorKernel<Functor>;
}

// The kernel serving a function that reads its own nulls over a column that can be null:
// the absent value goes to the functor as it is, so its answer fills a plain column.
template <typename Functor>
NLUnaryFunctionKernel selectNullReadingFunction(const Column* input, bool inputNullable, LocalMemory* memory, Column*& result) {
    using Arg = typename Functor::ArgType;
    using Res = typename Functor::ResultType;

    if (input->getKind() == ColumnConst<std::optional<Arg>>::staticKind()) {
        result = memory->alloc<ColumnConst<Res>>();
        return &functionOptConstKernel<Functor>;
    } else if (input->getContainerKind() == ContainerKind::code<ColumnConst>()) {
        result = memory->alloc<ColumnConst<Res>>();
        return &functionConstKernel<Functor>;
    } else if (inputNullable) {
        result = memory->alloc<ColumnVector<Res>>();
        return &functionNullReadingKernel<Functor>;
    }

    result = memory->alloc<ColumnVector<Res>>();
    return &functionVectorKernel<Functor>;
}

// A column holding no number names no node or edge on any row
template <TypedInternalID IDType>
void namesNoEntityKernel(NLExecutionContext* context, Column* result, const Column* input, NLStepBuffers* memory) {
    std::vector<IDType>& resultRaw = static_cast<ColumnVector<IDType>*>(result)->getRaw();
    resultRaw.assign(input->size(), IDType {});
}

template <TypedInternalID IDType>
void namesNoEntityConstKernel(NLExecutionContext* context, Column* result, const Column* input, NLStepBuffers* memory) {
    static_cast<ColumnConst<IDType>*>(result)->set(IDType {});
}

// A node or edge an OPTIONAL MATCH did not match is an invalid ID, so a function reading
// one produces a null rather than reading the graph at an ID that is not in it. The
// entity sibling of functionOptKernel: the input is a plain ID column, which carries its
// null in the ID itself instead of in an optional.
template <typename Functor>
void functionEntityKernel(NLExecutionContext* context, Column* result, const Column* input, NLStepBuffers* memory) {
    using Arg = typename Functor::ArgType;
    using Res = typename Functor::ResultType;
    using JustRes = TypeUtils::unwrap_optional_t<Res>;

    bioassert(input->getKind() == ColumnVector<Arg>::staticKind(), "Function operand has an unexpected column type.");
    const ColumnVector<Arg>* typedInput = static_cast<const ColumnVector<Arg>*>(input);
    ColumnOptVector<JustRes>* output = static_cast<ColumnOptVector<JustRes>*>(result);

    const std::vector<Arg>& inputRaw = typedInput->getRaw();
    const size_t size = inputRaw.size();

    resetForRowWrites(output, size);

    Functor functor = makeFunctor<Functor>(context, memory);
    for (size_t row = 0; row < size; row++) {
        if (inputRaw[row].isValid()) {
            output->set(row, functor(inputRaw[row]));
        } else {
            output->setNull(row);
        }
    }
}

// The sibling of functionEntityKernel for a function whose answer is an entity too: a node
// column spells its null as an invalid ID, so an unmatched edge writes one straight out
// rather than needing a nullable column to hold it.
template <typename Functor>
void functionEntityToEntityKernel(NLExecutionContext* context, Column* result, const Column* input, NLStepBuffers* memory) {
    using Arg = typename Functor::ArgType;
    using Res = typename Functor::ResultType;

    bioassert(input->getKind() == ColumnVector<Arg>::staticKind(), "Function operand has an unexpected column type.");
    const ColumnVector<Arg>* typedInput = static_cast<const ColumnVector<Arg>*>(input);
    ColumnVector<Res>* output = static_cast<ColumnVector<Res>*>(result);

    const std::vector<Arg>& inputRaw = typedInput->getRaw();
    const size_t size = inputRaw.size();

    output->resize(size);
    auto& outputRaw = output->getRaw();

    Functor functor = makeFunctor<Functor>(context, memory);
    for (size_t row = 0; row < size; row++) {
        if (inputRaw[row].isValid()) {
            outputRaw[row] = functor(inputRaw[row]);
        } else {
            outputRaw[row] = Res {};
        }
    }
}

template <typename Functor, typename Element>
void applyFunctionOverOptVector(Functor& functor,
                                const ColumnOptVector<Element>* input,
                                ColumnOptVector<TypeUtils::unwrap_optional_t<typename Functor::ResultType>>* output) {
    const size_t size = input->size();
    resetForRowWrites(output, size);

    for (size_t row = 0; row < size; row++) {
        if (input->isValid(row)) {
            writeRow(output, row, functor(input->getValue(row)));
        } else {
            output->setNull(row);
        }
    }
}

template <typename Functor>
void functionOptKernel(NLExecutionContext* context, Column* result, const Column* input, NLStepBuffers* memory) {
    using Arg = typename Functor::ArgType;
    using Res = typename Functor::ResultType;
    using JustRes = TypeUtils::unwrap_optional_t<Res>;

    bioassert(input->getKind() == ColumnOptVector<Arg>::staticKind(), "Function operand has an unexpected column type.");
    const ColumnOptVector<Arg>* typedInput = static_cast<const ColumnOptVector<Arg>*>(input);
    ColumnOptVector<JustRes>* output = static_cast<ColumnOptVector<JustRes>*>(result);

    Functor functor = makeFunctor<Functor>(context, memory);
    applyFunctionOverOptVector(functor, typedInput, output);
}

// Whether a column holds this element, in any of the shapes a chunk column takes: a plain
// vector, a nullable one, or a constant of either.
template <typename Element>
bool columnHoldsElement(const Column* column) {
    const ColumnKind::Code kind = column->getKind();
    const bool holdsPlainElement = kind == ColumnVector<Element>::staticKind() || kind == ColumnConst<Element>::staticKind();

    if constexpr (InternalKind::Types::contains<std::optional<Element>>()) {
        const bool holdsNullableElement = kind == ColumnOptVector<Element>::staticKind()
                                          || kind == ColumnConst<std::optional<Element>>::staticKind();
        return holdsPlainElement || holdsNullableElement;
    } else {
        return holdsPlainElement;
    }
}

// The key one column of a dynamic map key read holds at @param row, or nothing where the
// row holds none.
template <typename ColumnT>
std::optional<types::String::Primitive> stringRead(const Column* input, size_t row) {
    return operandRow(static_cast<const ColumnT*>(input), row);
}

template <typename ColumnT>
std::optional<types::String::Primitive> stringValueArgument(const Column* input, size_t row, std::string_view) {
    return stringRead<ColumnT>(input, row);
}

template <typename ColumnT>
std::optional<types::String::Primitive> cellStringArgument(const Column* input, size_t row, std::string_view functionName) {
    return cellString((*static_cast<const ColumnT*>(input))[row], functionName);
}

bool listStrings(const ListView list, std::string_view functionName, std::vector<types::String::Primitive>& strings) {
    for (const CellView element : list) {
        const std::optional<types::String::Primitive> string = cellString(element, functionName);
        if (!string.has_value()) {
            return false;
        }

        strings.push_back(*string);
    }

    return true;
}

template <typename ColumnT>
bool stringAsStringListArgument(const Column* input,
                                size_t row,
                                std::string_view,
                                std::vector<types::String::Primitive>& strings) {
    strings.clear();

    const std::optional<types::String::Primitive> string = operandRow(static_cast<const ColumnT*>(input), row);
    if (!string.has_value()) {
        return false;
    }

    strings.push_back(*string);

    return true;
}

template <typename ColumnT>
bool listAsStringListArgument(const Column* input,
                              size_t row,
                              std::string_view functionName,
                              std::vector<types::String::Primitive>& strings) {
    strings.clear();

    const std::optional<ListView> list = operandRow(static_cast<const ColumnT*>(input), row);

    return list.has_value() && listStrings(*list, functionName, strings);
}

template <typename ColumnT>
bool cellAsStringListArgument(const Column* input,
                              size_t row,
                              std::string_view functionName,
                              std::vector<types::String::Primitive>& strings) {
    strings.clear();

    const CellView cell = (*static_cast<const ColumnT*>(input))[row];
    const CellTag tag = cell.getTag();

    if (tag == CellTag::String) {
        strings.push_back(cell.getAs<types::String::Primitive>());
        return true;
    } else if (tag == CellTag::ListView) {
        return listStrings(cell.getAs<ListView>(), functionName, strings);
    } else if (tag == CellTag::Null) {
        return false;
    }

    throwCellTypeError(functionName, "a string or a list of strings", tag);
}

std::optional<types::Int64::Primitive> integerValueArgument(const Column* input, size_t row, std::string_view) {
    return static_cast<const ColumnOptVector<types::Int64::Primitive>*>(input)->get(row);
}

std::optional<types::Int64::Primitive> unsignedValueArgument(const Column* input, size_t row, std::string_view functionName) {
    const std::optional<types::UInt64::Primitive> cell =
        static_cast<const ColumnOptVector<types::UInt64::Primitive>*>(input)->get(row);
    if (!cell.has_value()) {
        return std::nullopt;
    }

    return unsignedCharacterArgument(*cell, functionName);
}

template <typename ColumnT>
std::optional<types::Point::Primitive> pointValueArgument(const Column* input, size_t row, std::string_view) {
    return operandRow(static_cast<const ColumnT*>(input), row);
}

template <typename ColumnT>
std::optional<types::Point::Primitive> cellPointArgument(const Column* input, size_t row, std::string_view functionName) {
    const CellView cell = (*static_cast<const ColumnT*>(input))[row];
    const CellTag tag = cell.getTag();

    if (tag == CellTag::Point) {
        return cell.getAs<types::Point::Primitive>();
    } else if (tag == CellTag::Null) {
        return std::nullopt;
    }

    throwCellTypeError(functionName, "a point", tag);
}

std::optional<types::Int64::Primitive> cellIntegerArgument(const Column* input, size_t row, std::string_view functionName) {
    return cellInteger((*static_cast<const ColumnVector<CellView>*>(input))[row], functionName);
}

// A key spelled null in the query holds no value to name an entry by, on any row
std::optional<types::String::Primitive> nullStringRead(const Column*, size_t) {
    return std::nullopt;
}

}

template <typename Result>
void NLExecutor::runSubstring(NLExecutionContext*, NLFunctionData* data) {
    const NLSubstringData* substring = static_cast<NLSubstringData*>(data);

    const NLSubstringData::StringArgument& string = substring->getString();
    const NLSubstringData::IntegerArgument& start = substring->getStart();
    const NLSubstringData::IntegerArgument& length = substring->getLength();

    const size_t rowCount = string._column->size();
    bioassert(start._column->size() == rowCount, "Start column of a substring is not row-aligned with its string.");
    bioassert(!length._column || length._column->size() == rowCount,
              "Length column of a substring is not row-aligned with its string.");

    ColumnVectorOf<Result>* output = static_cast<ColumnVectorOf<Result>*>(substring->getResult());
    resetForRowWrites(output, rowCount);

    const SubstringFunction function {};
    constexpr std::string_view name = SubstringFunction::NAME;

    constexpr std::optional<types::Int64::Primitive> toTheEnd = MAX_CHARACTER_ARGUMENT;

    for (size_t rowIndex = 0; rowIndex < rowCount; rowIndex++) {
        const std::optional<types::String::Primitive> text = string._read(string._column, rowIndex, name);
        const std::optional<types::Int64::Primitive> from = start._read(start._column, rowIndex, name);
        const std::optional<types::Int64::Primitive> count = length._column ? length._read(length._column, rowIndex, name) : toTheEnd;

        const bool boundIsNull = !from.has_value() || !count.has_value();
        if (!text.has_value()) {
            if constexpr (TypeUtils::is_optional_v<Result>) {
                output->setNull(rowIndex);
            } else {
                throw IRException("substring() typed as never null read a null string");
            }
        } else if (boundIsNull) {
            throw IRException("substring() does not take a null start or length");
        } else {
            output->set(rowIndex, function(*text, *from, *count));
        }
    }
}

template void NLExecutor::runSubstring<types::String::Primitive>(NLExecutionContext* context, NLFunctionData* data);
template void NLExecutor::runSubstring<std::optional<types::String::Primitive>>(NLExecutionContext* context, NLFunctionData* data);

template <typename Result>
void NLExecutor::runSplit(NLExecutionContext*, NLFunctionData* data) {
    const NLSplitData* split = static_cast<NLSplitData*>(data);

    const NLSplitData::StringArgument& string = split->getString();
    const NLSplitData::DelimitersArgument& delimiters = split->getDelimiters();

    const size_t rowCount = string._column->size();
    bioassert(delimiters._column->size() == rowCount, "Delimiter column of a split is not row-aligned with its string.");

    using OutputColumn = ColumnVectorOf<Result>;
    OutputColumn* output = static_cast<OutputColumn*>(split->getResult());
    resetForRowWrites(output, rowCount);

    NLStepBuffers* const stepBuffers = split->getStepBuffers();
    stepBuffers->clear();

    SplitFunction function(&stepBuffers->listBuffer());
    constexpr std::string_view name = SplitFunction::NAME;

    std::vector<types::String::Primitive> separators;

    for (size_t rowIndex = 0; rowIndex < rowCount; rowIndex++) {
        const std::optional<types::String::Primitive> text = string._read(string._column, rowIndex, name);
        const bool readsSeparators = delimiters._read(delimiters._column, rowIndex, name, separators);

        const bool readsANull = !text.has_value() || !readsSeparators;
        if (!readsANull) {
            output->set(rowIndex, function(*text, separators));
        } else if constexpr (TypeUtils::is_optional_v<Result>) {
            output->setNull(rowIndex);
        } else {
            throw IRException("split() typed as never null read a null argument");
        }
    }
}

template void NLExecutor::runSplit<ListView>(NLExecutionContext* context, NLFunctionData* data);
template void NLExecutor::runSplit<std::optional<ListView>>(NLExecutionContext* context, NLFunctionData* data);

template <typename Function, typename Result>
void NLExecutor::runStringEnd(NLExecutionContext*, NLFunctionData* data) {
    const NLStringEndData* stringEnd = static_cast<NLStringEndData*>(data);

    const NLStringEndData::StringArgument& string = stringEnd->getString();
    const NLStringEndData::IntegerArgument& length = stringEnd->getLength();

    const size_t rowCount = string._column->size();
    bioassert(length._column->size() == rowCount, "Length column of a string end is not row-aligned with its string.");

    ColumnVectorOf<Result>* output = static_cast<ColumnVectorOf<Result>*>(stringEnd->getResult());
    resetForRowWrites(output, rowCount);

    const Function function {};
    for (size_t rowIndex = 0; rowIndex < rowCount; rowIndex++) {
        const std::optional<types::String::Primitive> text = string._read(string._column, rowIndex, Function::NAME);
        const std::optional<types::Int64::Primitive> count = length._read(length._column, rowIndex, Function::NAME);

        if (!text.has_value()) {
            if constexpr (TypeUtils::is_optional_v<Result>) {
                output->setNull(rowIndex);
            } else {
                throw IRException(fmt::format("{}() typed as never null read a null string", Function::NAME));
            }
        } else if (!count.has_value()) {
            throw IRException(fmt::format("{}() does not take a null length", Function::NAME));
        } else {
            output->set(rowIndex, function(*text, *count));
        }
    }
}

template void NLExecutor::runStringEnd<LeftFunction, types::String::Primitive>(NLExecutionContext* context, NLFunctionData* data);
template void NLExecutor::runStringEnd<LeftFunction, std::optional<types::String::Primitive>>(NLExecutionContext* context, NLFunctionData* data);
template void NLExecutor::runStringEnd<RightFunction, types::String::Primitive>(NLExecutionContext* context, NLFunctionData* data);
template void NLExecutor::runStringEnd<RightFunction, std::optional<types::String::Primitive>>(NLExecutionContext* context, NLFunctionData* data);

template <typename Result>
void NLExecutor::runReplace(NLExecutionContext*, NLFunctionData* data) {
    const NLReplaceData* replace = static_cast<NLReplaceData*>(data);

    const NLReplaceData::StringArgument& string = replace->getString();
    const NLReplaceData::StringArgument& search = replace->getSearch();
    const NLReplaceData::StringArgument& replacement = replace->getReplacement();

    const size_t rowCount = string._column->size();
    bioassert(search._column->size() == rowCount, "Search column of a replace is not row-aligned with its string.");
    bioassert(replacement._column->size() == rowCount, "Replacement column of a replace is not row-aligned with its string.");

    ColumnVectorOf<Result>* output = static_cast<ColumnVectorOf<Result>*>(replace->getResult());
    resetForRowWrites(output, rowCount);

    NLStepBuffers* const stepBuffers = replace->getStepBuffers();
    stepBuffers->clear();

    ReplaceFunction function(&stepBuffers->stringBuffer());
    for (size_t rowIndex = 0; rowIndex < rowCount; rowIndex++) {
        const std::optional<types::String::Primitive> text = string._read(string._column, rowIndex, ReplaceFunction::NAME);
        const std::optional<types::String::Primitive> searched = search._read(search._column, rowIndex, ReplaceFunction::NAME);
        const std::optional<types::String::Primitive> replacing = replacement._read(replacement._column, rowIndex, ReplaceFunction::NAME);

        const bool readsANull = !text.has_value() || !searched.has_value() || !replacing.has_value();
        if (!readsANull) {
            output->set(rowIndex, function(*text, *searched, *replacing));
        } else if constexpr (TypeUtils::is_optional_v<Result>) {
            output->setNull(rowIndex);
        } else {
            throw IRException("replace() typed as never null read a null argument");
        }
    }
}

template void NLExecutor::runReplace<types::String::Primitive>(NLExecutionContext* context, NLFunctionData* data);
template void NLExecutor::runReplace<std::optional<types::String::Primitive>>(NLExecutionContext* context, NLFunctionData* data);

void NLExecutor::runPointDistance(NLExecutionContext*, NLFunctionData* data) {
    const NLPointFunctionData* function = static_cast<NLPointFunctionData*>(data);

    const std::vector<NLPointFunctionData::PointArgument>& arguments = function->getArguments();
    const NLPointFunctionData::PointArgument& lhs = arguments[0];
    const NLPointFunctionData::PointArgument& rhs = arguments[1];

    const size_t rowCount = lhs._column->size();
    bioassert(rhs._column->size() == rowCount, "Columns of a point.distance are not row-aligned.");

    ColumnOptVector<types::Double::Primitive>* output = static_cast<ColumnOptVector<types::Double::Primitive>*>(function->getResult());
    resetForRowWrites(output, rowCount);

    for (size_t rowIndex = 0; rowIndex < rowCount; rowIndex++) {
        const std::optional<Point> from = lhs._read(lhs._column, rowIndex, "point.distance");
        const std::optional<Point> to = rhs._read(rhs._column, rowIndex, "point.distance");

        if (from.has_value() && to.has_value()) {
            output->setOptional(rowIndex, Point::distance(*from, *to));
        } else {
            output->setNull(rowIndex);
        }
    }
}

void NLExecutor::runPointWithinBBox(NLExecutionContext*, NLFunctionData* data) {
    const NLPointFunctionData* function = static_cast<NLPointFunctionData*>(data);

    const std::vector<NLPointFunctionData::PointArgument>& arguments = function->getArguments();
    const NLPointFunctionData::PointArgument& point = arguments[0];
    const NLPointFunctionData::PointArgument& lowerLeft = arguments[1];
    const NLPointFunctionData::PointArgument& upperRight = arguments[2];

    const size_t rowCount = point._column->size();
    bioassert(lowerLeft._column->size() == rowCount, "Columns of a point.withinBBox are not row-aligned.");
    bioassert(upperRight._column->size() == rowCount, "Columns of a point.withinBBox are not row-aligned.");

    ColumnOptVector<types::Bool::Primitive>* output = static_cast<ColumnOptVector<types::Bool::Primitive>*>(function->getResult());
    resetForRowWrites(output, rowCount);

    for (size_t rowIndex = 0; rowIndex < rowCount; rowIndex++) {
        const std::optional<Point> value = point._read(point._column, rowIndex, "point.withinBBox");
        const std::optional<Point> lower = lowerLeft._read(lowerLeft._column, rowIndex, "point.withinBBox");
        const std::optional<Point> upper = upperRight._read(upperRight._column, rowIndex, "point.withinBBox");

        const bool readsANull = !value.has_value() || !lower.has_value() || !upper.has_value();
        const std::optional<bool> within = readsANull ? std::nullopt : Point::withinBBox(*value, *lower, *upper);

        if (within.has_value()) {
            output->set(rowIndex, types::Bool::Primitive {*within});
        } else {
            output->setNull(rowIndex);
        }
    }
}

NLStringReadFunction NLExecutor::selectStringRead(const Column* input) {
    using String = types::String::Primitive;

    const ColumnKind::Code kind = input->getKind();

    if (kind == ColumnVector<String>::staticKind()) {
        return &stringRead<ColumnVector<String>>;
    } else if (kind == ColumnOptVector<String>::staticKind()) {
        return &stringRead<ColumnOptVector<String>>;
    } else if (kind == ColumnConst<String>::staticKind()) {
        return &stringRead<ColumnConst<String>>;
    } else if (kind == ColumnConst<std::optional<String>>::staticKind()) {
        return &stringRead<ColumnConst<std::optional<String>>>;
    } else if (kind == ColumnConst<PropertyNull>::staticKind()) {
        return &nullStringRead;
    }

    throw IRException("a dynamic map key read reads its key out of a string column");
}

NLStringArgumentRead NLExecutor::selectStringArgumentRead(const Column* input) {
    const ColumnKind::Code kind = input->getKind();

    if (kind == ColumnOptVector<types::String::Primitive>::staticKind()) {
        return &stringValueArgument<ColumnOptVector<types::String::Primitive>>;
    } else if (kind == ColumnConst<types::String::Primitive>::staticKind()) {
        return &stringValueArgument<ColumnConst<types::String::Primitive>>;
    } else if (kind == ColumnConst<std::optional<types::String::Primitive>>::staticKind()) {
        return &stringValueArgument<ColumnConst<std::optional<types::String::Primitive>>>;
    } else if (kind == ColumnVector<CellView>::staticKind()) {
        return &cellStringArgument<ColumnVector<CellView>>;
    } else if (kind == ColumnConst<CellView>::staticKind()) {
        return &cellStringArgument<ColumnConst<CellView>>;
    }

    throw IRException("a string function reads a string argument out of a nullable string column, or a column of tagged cells");
}

NLStringListArgumentRead NLExecutor::selectStringListArgumentRead(const Column* input) {
    using String = types::String::Primitive;

    const ColumnKind::Code kind = input->getKind();

    if (kind == ColumnOptVector<String>::staticKind()) {
        return &stringAsStringListArgument<ColumnOptVector<String>>;
    } else if (kind == ColumnConst<String>::staticKind()) {
        return &stringAsStringListArgument<ColumnConst<String>>;
    } else if (kind == ColumnConst<std::optional<String>>::staticKind()) {
        return &stringAsStringListArgument<ColumnConst<std::optional<String>>>;
    } else if (kind == ColumnVector<ListView>::staticKind()) {
        return &listAsStringListArgument<ColumnVector<ListView>>;
    } else if (kind == ColumnOptVector<ListView>::staticKind()) {
        return &listAsStringListArgument<ColumnOptVector<ListView>>;
    } else if (kind == ColumnConst<ListView>::staticKind()) {
        return &listAsStringListArgument<ColumnConst<ListView>>;
    } else if (kind == ColumnConst<std::optional<ListView>>::staticKind()) {
        return &listAsStringListArgument<ColumnConst<std::optional<ListView>>>;
    } else if (kind == ColumnVector<CellView>::staticKind()) {
        return &cellAsStringListArgument<ColumnVector<CellView>>;
    } else if (kind == ColumnConst<CellView>::staticKind()) {
        return &cellAsStringListArgument<ColumnConst<CellView>>;
    }

    throw IRException("a string function reads a list of strings out of a nullable string column, a list column, or a column of tagged cells");
}

NLPointArgumentRead NLExecutor::selectPointArgumentRead(const Column* input) {
    using PointValue = types::Point::Primitive;

    const ColumnKind::Code kind = input->getKind();

    if (kind == ColumnOptVector<PointValue>::staticKind()) {
        return &pointValueArgument<ColumnOptVector<PointValue>>;
    } else if (kind == ColumnConst<PointValue>::staticKind()) {
        return &pointValueArgument<ColumnConst<PointValue>>;
    } else if (kind == ColumnConst<std::optional<PointValue>>::staticKind()) {
        return &pointValueArgument<ColumnConst<std::optional<PointValue>>>;
    } else if (kind == ColumnVector<CellView>::staticKind()) {
        return &cellPointArgument<ColumnVector<CellView>>;
    } else if (kind == ColumnConst<CellView>::staticKind()) {
        return &cellPointArgument<ColumnConst<CellView>>;
    }

    throw IRException("a point function reads a point argument out of a nullable point column, or a column of tagged cells");
}

NLIntegerArgumentRead NLExecutor::selectIntegerArgumentRead(const Column* input) {
    const ColumnKind::Code kind = input->getKind();

    if (kind == ColumnOptVector<types::Int64::Primitive>::staticKind()) {
        return &integerValueArgument;
    } else if (kind == ColumnOptVector<types::UInt64::Primitive>::staticKind()) {
        return &unsignedValueArgument;
    } else if (kind == ColumnVector<CellView>::staticKind()) {
        return &cellIntegerArgument;
    }

    throw IRException("a string function reads an integer argument out of a nullable integer column, or a column of tagged cells");
}

void NLExecutor::runUnaryFunction(NLExecutionContext* context, NLFunctionData* data) {
    const NLUnaryFunctionData* funcData = static_cast<NLUnaryFunctionData*>(data);
    NLStepBuffers* const stepBuffers = funcData->getStepBuffers();
    stepBuffers->clear();

    funcData->getKernel()(context, funcData->getResult(), funcData->getInput(), stepBuffers);
}

template <typename Functor>
NLUnaryFunctionKernel NLExecutor::selectFunction(const Column* input, bool inputNullable, LocalMemory* memory, Column*& result) {
    using Arg = typename Functor::ArgType;
    using Res = typename Functor::ResultType;
    using JustRes = TypeUtils::unwrap_optional_t<Res>;

    // Early exit noop for NULL literals
    if (input->getKind() == ColumnConst<PropertyNull>::staticKind()) {
        result = memory->alloc<ColumnConst<PropertyNull>>();
        return &functionNullKernel;
    }

    // A column of type-erased cells is served by the counterpart reading one, which
    // answers over the same rows but reads its argument - and its nulls - out of the tag
    if constexpr (HasTaggedCounterpart<Functor>) {
        if (readsTaggedCells(input)) {
            return selectTaggedCellFunction<typename Functor::TaggedCounterpart>(input, memory, result);
        }
    }

    // A constant that can be null - what a conversion of a constant answers - is the one-row
    // form of a nullable column, and is read the way the column is. A function reading its
    // own nulls answers a plain result, so only the argument needs a nullable kind
    constexpr bool hasNullableConstant = InternalKind::Types::contains<std::optional<Arg>>() && !TypedInternalID<Res>;

    if constexpr (hasNullableConstant) {
        if (isNullableConstantOf<Arg>(input)) {
            if constexpr (ReadsItsNulls<Functor>) {
                result = memory->alloc<ColumnConst<Res>>();
                return &functionOptConstKernel<Functor>;
            } else {
                result = memory->alloc<ColumnConst<std::optional<JustRes>>>();
                return &functionNullableConstKernel<Functor>;
            }
        }
    }

    if (input->getContainerKind() == ContainerKind::code<ColumnConst>()) {
        result = memory->alloc<ColumnConst<Res>>();
        return &functionConstKernel<Functor>;
    }

    // An entity answers with the invalid ID where it is absent, so a function answering one
    // needs no nullable column to carry that: it fills the plain ID column every other
    // entity column in the engine is, and its rows say for themselves which matched nothing
    if constexpr (TypedInternalID<Res>) {
        result = memory->alloc<ColumnVector<Res>>();
        return &functionEntityToEntityKernel<Functor>;
    } else {
        constexpr bool hasNullableColumn = InternalKind::Types::contains<std::optional<Arg>>();

        if constexpr (hasNullableColumn) {
            if (inputNullable) {
                if constexpr (ReadsItsNulls<Functor>) {
                    result = memory->alloc<ColumnVectorOf<Res>>();
                    return &functionNullReadingKernel<Functor>;
                } else {
                    result = memory->alloc<ColumnOptVector<JustRes>>();
                    return &functionOptKernel<Functor>;
                }
            }
        }

        // An entity column carries its null in the ID rather than in an optional, so it is
        // read row by row for validity and its result is nullable all the same
        if constexpr (TypedInternalID<typename Functor::ArgType>) {
            result = memory->alloc<ColumnOptVector<JustRes>>();
            return &functionEntityKernel<Functor>;
        }

        result = memory->alloc<ColumnVectorOf<Res>>();
        return &functionVectorKernel<Functor>;
    }
}

template <typename Conversion>
NLUnaryFunctionKernel NLExecutor::selectConversion(const Column* input, bool inputNullable, LocalMemory* memory, Column*& result) {
    NLUnaryFunctionKernel kernel = nullptr;

    const auto selectForElement = [&]<typename Element>() {
        using Functor = ConversionFunctorFor<Conversion, Element>::Type;

        if constexpr (!std::is_same_v<Functor, Conversion>) {
            if (!kernel && columnHoldsElement<Element>(input)) {
                kernel = selectFunction<Functor>(input, inputNullable, memory, result);
            }
        }
    };

    selectForElement.template operator()<types::Int64::Primitive>();
    selectForElement.template operator()<types::UInt64::Primitive>();
    selectForElement.template operator()<types::Double::Primitive>();
    selectForElement.template operator()<types::Bool::Primitive>();
    selectForElement.template operator()<types::DateTime::Primitive>();
    selectForElement.template operator()<types::Duration::Primitive>();
    selectForElement.template operator()<MapView>();
    selectForElement.template operator()<types::Point::Primitive>();

    if (kernel) {
        return kernel;
    }

    return selectFunction<Conversion>(input, inputNullable, memory, result);
}

NLUnaryFunctionKernel NLExecutor::selectSize(const Column* input, bool inputNullable, LocalMemory* memory, Column*& result) {
    if (columnHoldsElement<types::String::Primitive>(input)) {
        return selectFunction<StringSizeFunction>(input, inputNullable, memory, result);
    }

    return selectFunction<ListSizeFunction>(input, inputNullable, memory, result);
}

NLUnaryFunctionKernel NLExecutor::selectIsEmpty(const Column* input, bool inputNullable, LocalMemory* memory, Column*& result) {
    if (columnHoldsElement<types::String::Primitive>(input)) {
        return selectFunction<StringIsEmptyFunction>(input, inputNullable, memory, result);
    } else if (columnHoldsElement<MapView>(input)) {
        return selectFunction<MapIsEmptyFunction>(input, inputNullable, memory, result);
    }

    return selectFunction<ListIsEmptyFunction>(input, inputNullable, memory, result);
}

NLUnaryFunctionKernel NLExecutor::selectId(const Column* input, bool inputNullable, LocalMemory* memory, Column*& result) {
    if (columnHoldsElement<NodeID>(input)) {
        return selectFunction<IdFunction<NodeID>>(input, inputNullable, memory, result);
    } else if (columnHoldsElement<EdgeID>(input)) {
        return selectFunction<IdFunction<EdgeID>>(input, inputNullable, memory, result);
    }

    return selectFunction<TaggedIdFunction>(input, inputNullable, memory, result);
}

template <TypedInternalID IDType>
NLUnaryFunctionKernel NLExecutor::selectEntityIDOf(const Column* input, bool inputNullable, LocalMemory* memory, Column*& result) {
    if (columnHoldsElement<NodeID>(input)) {
        return selectFunction<EntityIDOfEntityFunction<IDType, NodeID>>(input, inputNullable, memory, result);
    } else if (columnHoldsElement<EdgeID>(input)) {
        return selectFunction<EntityIDOfEntityFunction<IDType, EdgeID>>(input, inputNullable, memory, result);
    } else if (readsTaggedCells(input)) {
        return selectTaggedCellFunction<TaggedEntityIDOfFunction<IDType>>(input, memory, result);
    } else if (columnHoldsElement<types::Int64::Primitive>(input)) {
        return selectNullReadingFunction<EntityIDOfNumberFunction<IDType, types::Int64::Primitive>>(input, inputNullable, memory, result);
    } else if (columnHoldsElement<types::UInt64::Primitive>(input)) {
        return selectNullReadingFunction<EntityIDOfNumberFunction<IDType, types::UInt64::Primitive>>(input, inputNullable, memory, result);
    } else if (columnHoldsElement<types::Double::Primitive>(input)) {
        return selectNullReadingFunction<EntityIDOfNumberFunction<IDType, types::Double::Primitive>>(input, inputNullable, memory, result);
    } else if (input->getContainerKind() == ContainerKind::code<ColumnConst>()) {
        result = memory->alloc<ColumnConst<IDType>>();
        return &namesNoEntityConstKernel<IDType>;
    }

    result = memory->alloc<ColumnVector<IDType>>();
    return &namesNoEntityKernel<IDType>;
}

template NLUnaryFunctionKernel NLExecutor::selectEntityIDOf<NodeID>(const Column* input, bool inputNullable, LocalMemory* memory, Column*& result);
template NLUnaryFunctionKernel NLExecutor::selectEntityIDOf<EdgeID>(const Column* input, bool inputNullable, LocalMemory* memory, Column*& result);

NLUnaryFunctionKernel NLExecutor::selectReverse(const Column* input, bool inputNullable, LocalMemory* memory, Column*& result) {
    if (columnHoldsElement<types::String::Primitive>(input)) {
        return selectFunction<ReverseFunction>(input, inputNullable, memory, result);
    } else if (readsTaggedCells(input)) {
        return selectTaggedCellFunction<TaggedReverseFunction>(input, memory, result);
    }

    return selectFunction<ListReverseFunction>(input, inputNullable, memory, result);
}

NLUnaryFunctionKernel NLExecutor::selectKeys(const Column* input, bool inputNullable, LocalMemory* memory, Column*& result) {
    if (columnHoldsElement<NodeID>(input)) {
        return selectFunction<NLKeysFunction<NodeID>>(input, inputNullable, memory, result);
    } else if (columnHoldsElement<EdgeID>(input)) {
        return selectFunction<NLKeysFunction<EdgeID>>(input, inputNullable, memory, result);
    } else if (readsTaggedCells(input)) {
        return selectTaggedCellFunction<NLTaggedKeysFunction>(input, memory, result);
    } else if (columnHoldsElement<MapView>(input)) {
        return selectFunction<MapKeysFunction>(input, inputNullable, memory, result);
    }

    throw IRException("keys() reads a node, a relationship or a map column");
}

NLUnaryFunctionKernel NLExecutor::selectLabels(const Column* input, bool inputNullable, LocalMemory* memory, Column*& result) {
    if (columnHoldsElement<NodeID>(input)) {
        return selectFunction<LabelsFunction>(input, inputNullable, memory, result);
    } else if (readsTaggedCells(input)) {
        return selectTaggedCellFunction<NLTaggedLabelsFunction>(input, memory, result);
    }

    throw IRException("labels() reads a node column");
}

NLUnaryFunctionKernel NLExecutor::selectEdgeType(const Column* input, bool inputNullable, LocalMemory* memory, Column*& result) {
    if (columnHoldsElement<EdgeID>(input)) {
        return selectFunction<EdgeTypesFunction>(input, inputNullable, memory, result);
    } else if (readsTaggedCells(input)) {
        return selectTaggedCellFunction<NLTaggedEdgeTypeFunction>(input, memory, result);
    }

    throw IRException("type() reads a relationship column");
}

NLUnaryFunctionKernel NLExecutor::selectProperties(const Column* input, bool inputNullable, LocalMemory* memory, Column*& result) {
    if (columnHoldsElement<NodeID>(input)) {
        return selectFunction<NLPropertiesFunction<NodeID>>(input, inputNullable, memory, result);
    } else if (columnHoldsElement<EdgeID>(input)) {
        return selectFunction<NLPropertiesFunction<EdgeID>>(input, inputNullable, memory, result);
    } else if (readsTaggedCells(input)) {
        return selectTaggedCellFunction<NLTaggedPropertiesFunction>(input, memory, result);
    } else if (columnHoldsElement<MapView>(input)) {
        return selectFunction<MapPropertiesFunction>(input, inputNullable, memory, result);
    }

    throw IRException("Reading every property needs a node, a relationship or a map column");
}

template <template <typename> typename NumberFunctor>
NLUnaryFunctionKernel NLExecutor::selectNumberFunction(const Column* input, bool inputNullable, LocalMemory* memory, Column*& result) {
    if (columnHoldsElement<types::Int64::Primitive>(input)) {
        return selectFunction<NumberFunctor<types::Int64::Primitive>>(input, inputNullable, memory, result);
    } else if (columnHoldsElement<types::UInt64::Primitive>(input)) {
        return selectFunction<NumberFunctor<types::UInt64::Primitive>>(input, inputNullable, memory, result);
    } else if (columnHoldsElement<types::Double::Primitive>(input)) {
        return selectFunction<NumberFunctor<types::Double::Primitive>>(input, inputNullable, memory, result);
    } else if (readsTaggedCells(input)) {
        return selectTaggedCellFunction<typename NumberFunctor<types::Int64::Primitive>::TaggedCounterpart>(input, memory, result);
    }

    throw IRException("A numeric function reads an integer or a double column");
}

NLUnaryFunctionKernel NLExecutor::selectNegateFunction(const Column* input, bool inputNullable, LocalMemory* memory, Column*& result) {
    if (columnHoldsElement<types::Int64::Primitive>(input)) {
        return selectFunction<NegateFunction<types::Int64::Primitive>>(input, inputNullable, memory, result);
    } else if (columnHoldsElement<types::UInt64::Primitive>(input)) {
        return selectFunction<NegateFunction<types::UInt64::Primitive>>(input, inputNullable, memory, result);
    } else if (columnHoldsElement<types::Double::Primitive>(input)) {
        return selectFunction<NegateFunction<types::Double::Primitive>>(input, inputNullable, memory, result);
    } else if (columnHoldsElement<types::Duration::Primitive>(input)) {
        return selectFunction<NegateFunction<types::Duration::Primitive>>(input, inputNullable, memory, result);
    } else if (readsTaggedCells(input)) {
        return selectTaggedCellFunction<TaggedNegateFunction>(input, memory, result);
    }

    throw IRException("A unary minus reads an integer, a double or a duration column");
}

NLUnaryFunctionKernel NLExecutor::selectUnaryPlusFunction(const Column* input, bool inputNullable, LocalMemory* memory, Column*& result) {
    if (columnHoldsElement<types::Int64::Primitive>(input)) {
        return selectFunction<UnaryPlusFunction<types::Int64::Primitive>>(input, inputNullable, memory, result);
    } else if (columnHoldsElement<types::UInt64::Primitive>(input)) {
        return selectFunction<UnaryPlusFunction<types::UInt64::Primitive>>(input, inputNullable, memory, result);
    } else if (columnHoldsElement<types::Double::Primitive>(input)) {
        return selectFunction<UnaryPlusFunction<types::Double::Primitive>>(input, inputNullable, memory, result);
    } else if (columnHoldsElement<types::Duration::Primitive>(input)) {
        return selectFunction<UnaryPlusFunction<types::Duration::Primitive>>(input, inputNullable, memory, result);
    } else if (readsTaggedCells(input)) {
        return selectTaggedCellFunction<TaggedUnaryPlusFunction>(input, memory, result);
    }

    throw IRException("A unary plus reads an integer, a double or a duration column");
}

template <FloatFunctionKind Kind>
NLUnaryFunctionKernel NLExecutor::selectFloatFunction(const Column* input, bool inputNullable, LocalMemory* memory, Column*& result) {
    if (columnHoldsElement<types::Int64::Primitive>(input)) {
        return selectFunction<FloatFunction<Kind, types::Int64::Primitive>>(input, inputNullable, memory, result);
    } else if (columnHoldsElement<types::UInt64::Primitive>(input)) {
        return selectFunction<FloatFunction<Kind, types::UInt64::Primitive>>(input, inputNullable, memory, result);
    } else if (columnHoldsElement<types::Double::Primitive>(input)) {
        return selectFunction<FloatFunction<Kind, types::Double::Primitive>>(input, inputNullable, memory, result);
    } else if (readsTaggedCells(input)) {
        return selectTaggedCellFunction<TaggedFloatFunction<Kind>>(input, memory, result);
    }

    throw IRException("A numeric function reads an integer or a double column");
}

template NLUnaryFunctionKernel NLExecutor::selectNumberFunction<AbsFunction>(const Column* input, bool inputNullable, LocalMemory* memory, Column*& result);
template NLUnaryFunctionKernel NLExecutor::selectNumberFunction<SignFunction>(const Column* input, bool inputNullable, LocalMemory* memory, Column*& result);
template NLUnaryFunctionKernel NLExecutor::selectFloatFunction<FloatFunctionKind::Ceil>(const Column* input, bool inputNullable, LocalMemory* memory, Column*& result);
template NLUnaryFunctionKernel NLExecutor::selectFloatFunction<FloatFunctionKind::Floor>(const Column* input, bool inputNullable, LocalMemory* memory, Column*& result);
template NLUnaryFunctionKernel NLExecutor::selectFloatFunction<FloatFunctionKind::Round>(const Column* input, bool inputNullable, LocalMemory* memory, Column*& result);
template NLUnaryFunctionKernel NLExecutor::selectFloatFunction<FloatFunctionKind::Sqrt>(const Column* input, bool inputNullable, LocalMemory* memory, Column*& result);
template NLUnaryFunctionKernel NLExecutor::selectFloatFunction<FloatFunctionKind::Exp>(const Column* input, bool inputNullable, LocalMemory* memory, Column*& result);
template NLUnaryFunctionKernel NLExecutor::selectFloatFunction<FloatFunctionKind::Log>(const Column* input, bool inputNullable, LocalMemory* memory, Column*& result);
template NLUnaryFunctionKernel NLExecutor::selectFloatFunction<FloatFunctionKind::Log10>(const Column* input, bool inputNullable, LocalMemory* memory, Column*& result);
template NLUnaryFunctionKernel NLExecutor::selectFloatFunction<FloatFunctionKind::Sin>(const Column* input, bool inputNullable, LocalMemory* memory, Column*& result);
template NLUnaryFunctionKernel NLExecutor::selectFloatFunction<FloatFunctionKind::Cos>(const Column* input, bool inputNullable, LocalMemory* memory, Column*& result);
template NLUnaryFunctionKernel NLExecutor::selectFloatFunction<FloatFunctionKind::Tan>(const Column* input, bool inputNullable, LocalMemory* memory, Column*& result);
template NLUnaryFunctionKernel NLExecutor::selectFloatFunction<FloatFunctionKind::Cot>(const Column* input, bool inputNullable, LocalMemory* memory, Column*& result);
template NLUnaryFunctionKernel NLExecutor::selectFloatFunction<FloatFunctionKind::Asin>(const Column* input, bool inputNullable, LocalMemory* memory, Column*& result);
template NLUnaryFunctionKernel NLExecutor::selectFloatFunction<FloatFunctionKind::Acos>(const Column* input, bool inputNullable, LocalMemory* memory, Column*& result);
template NLUnaryFunctionKernel NLExecutor::selectFloatFunction<FloatFunctionKind::Atan>(const Column* input, bool inputNullable, LocalMemory* memory, Column*& result);
template NLUnaryFunctionKernel NLExecutor::selectFloatFunction<FloatFunctionKind::Degrees>(const Column* input, bool inputNullable, LocalMemory* memory, Column*& result);
template NLUnaryFunctionKernel NLExecutor::selectFloatFunction<FloatFunctionKind::Radians>(const Column* input, bool inputNullable, LocalMemory* memory, Column*& result);
template NLUnaryFunctionKernel NLExecutor::selectFloatFunction<FloatFunctionKind::Haversin>(const Column* input, bool inputNullable, LocalMemory* memory, Column*& result);

template NLUnaryFunctionKernel NLExecutor::selectFunction<LabelsFunction>(const Column* input, bool inputNullable, LocalMemory* memory, Column*& result);
template NLUnaryFunctionKernel NLExecutor::selectFunction<EdgeTypesFunction>(const Column* input, bool inputNullable, LocalMemory* memory, Column*& result);
template NLUnaryFunctionKernel NLExecutor::selectFunction<StartNodeFunction>(const Column* input, bool inputNullable, LocalMemory* memory, Column*& result);
template NLUnaryFunctionKernel NLExecutor::selectFunction<EndNodeFunction>(const Column* input, bool inputNullable, LocalMemory* memory, Column*& result);
template NLUnaryFunctionKernel NLExecutor::selectFunction<DateTimeComponentFunction<DateTimePart::Year>>(const Column* input, bool inputNullable, LocalMemory* memory, Column*& result);
template NLUnaryFunctionKernel NLExecutor::selectFunction<DateTimeComponentFunction<DateTimePart::Month>>(const Column* input, bool inputNullable, LocalMemory* memory, Column*& result);
template NLUnaryFunctionKernel NLExecutor::selectFunction<DateTimeComponentFunction<DateTimePart::Day>>(const Column* input, bool inputNullable, LocalMemory* memory, Column*& result);
template NLUnaryFunctionKernel NLExecutor::selectFunction<DateTimeComponentFunction<DateTimePart::Hour>>(const Column* input, bool inputNullable, LocalMemory* memory, Column*& result);
template NLUnaryFunctionKernel NLExecutor::selectFunction<DateTimeComponentFunction<DateTimePart::Minute>>(const Column* input, bool inputNullable, LocalMemory* memory, Column*& result);
template NLUnaryFunctionKernel NLExecutor::selectFunction<DateTimeComponentFunction<DateTimePart::Second>>(const Column* input, bool inputNullable, LocalMemory* memory, Column*& result);
template NLUnaryFunctionKernel NLExecutor::selectFunction<DateTimeComponentFunction<DateTimePart::Millisecond>>(const Column* input, bool inputNullable, LocalMemory* memory, Column*& result);
template NLUnaryFunctionKernel NLExecutor::selectFunction<DateTimeComponentFunction<DateTimePart::Microsecond>>(const Column* input, bool inputNullable, LocalMemory* memory, Column*& result);
template NLUnaryFunctionKernel NLExecutor::selectFunction<DurationComponentFunction<DurationPart::Years>>(const Column* input, bool inputNullable, LocalMemory* memory, Column*& result);
template NLUnaryFunctionKernel NLExecutor::selectFunction<DurationComponentFunction<DurationPart::Quarters>>(const Column* input, bool inputNullable, LocalMemory* memory, Column*& result);
template NLUnaryFunctionKernel NLExecutor::selectFunction<DurationComponentFunction<DurationPart::Months>>(const Column* input, bool inputNullable, LocalMemory* memory, Column*& result);
template NLUnaryFunctionKernel NLExecutor::selectFunction<DurationComponentFunction<DurationPart::Weeks>>(const Column* input, bool inputNullable, LocalMemory* memory, Column*& result);
template NLUnaryFunctionKernel NLExecutor::selectFunction<DurationComponentFunction<DurationPart::Days>>(const Column* input, bool inputNullable, LocalMemory* memory, Column*& result);
template NLUnaryFunctionKernel NLExecutor::selectFunction<DurationComponentFunction<DurationPart::Hours>>(const Column* input, bool inputNullable, LocalMemory* memory, Column*& result);
template NLUnaryFunctionKernel NLExecutor::selectFunction<DurationComponentFunction<DurationPart::Minutes>>(const Column* input, bool inputNullable, LocalMemory* memory, Column*& result);
template NLUnaryFunctionKernel NLExecutor::selectFunction<DurationComponentFunction<DurationPart::Seconds>>(const Column* input, bool inputNullable, LocalMemory* memory, Column*& result);
template NLUnaryFunctionKernel NLExecutor::selectFunction<DurationComponentFunction<DurationPart::Milliseconds>>(const Column* input, bool inputNullable, LocalMemory* memory, Column*& result);
template NLUnaryFunctionKernel NLExecutor::selectFunction<DurationComponentFunction<DurationPart::Microseconds>>(const Column* input, bool inputNullable, LocalMemory* memory, Column*& result);
template NLUnaryFunctionKernel NLExecutor::selectFunction<DurationComponentFunction<DurationPart::QuartersOfYear>>(const Column* input, bool inputNullable, LocalMemory* memory, Column*& result);
template NLUnaryFunctionKernel NLExecutor::selectFunction<DurationComponentFunction<DurationPart::MonthsOfYear>>(const Column* input, bool inputNullable, LocalMemory* memory, Column*& result);
template NLUnaryFunctionKernel NLExecutor::selectFunction<DurationComponentFunction<DurationPart::MonthsOfQuarter>>(const Column* input, bool inputNullable, LocalMemory* memory, Column*& result);
template NLUnaryFunctionKernel NLExecutor::selectFunction<DurationComponentFunction<DurationPart::DaysOfWeek>>(const Column* input, bool inputNullable, LocalMemory* memory, Column*& result);
template NLUnaryFunctionKernel NLExecutor::selectFunction<DurationComponentFunction<DurationPart::MinutesOfHour>>(const Column* input, bool inputNullable, LocalMemory* memory, Column*& result);
template NLUnaryFunctionKernel NLExecutor::selectFunction<DurationComponentFunction<DurationPart::SecondsOfMinute>>(const Column* input, bool inputNullable, LocalMemory* memory, Column*& result);
template NLUnaryFunctionKernel NLExecutor::selectFunction<DurationComponentFunction<DurationPart::MillisecondsOfSecond>>(const Column* input, bool inputNullable, LocalMemory* memory, Column*& result);
template NLUnaryFunctionKernel NLExecutor::selectFunction<DurationComponentFunction<DurationPart::MicrosecondsOfSecond>>(const Column* input, bool inputNullable, LocalMemory* memory, Column*& result);
template NLUnaryFunctionKernel NLExecutor::selectFunction<PointComponentFunction<PointPart::X>>(const Column* input, bool inputNullable, LocalMemory* memory, Column*& result);
template NLUnaryFunctionKernel NLExecutor::selectFunction<PointComponentFunction<PointPart::Y>>(const Column* input, bool inputNullable, LocalMemory* memory, Column*& result);
template NLUnaryFunctionKernel NLExecutor::selectFunction<PointComponentFunction<PointPart::Z>>(const Column* input, bool inputNullable, LocalMemory* memory, Column*& result);
template NLUnaryFunctionKernel NLExecutor::selectFunction<PointComponentFunction<PointPart::Longitude>>(const Column* input, bool inputNullable, LocalMemory* memory, Column*& result);
template NLUnaryFunctionKernel NLExecutor::selectFunction<PointComponentFunction<PointPart::Latitude>>(const Column* input, bool inputNullable, LocalMemory* memory, Column*& result);
template NLUnaryFunctionKernel NLExecutor::selectFunction<PointComponentFunction<PointPart::Height>>(const Column* input, bool inputNullable, LocalMemory* memory, Column*& result);
template NLUnaryFunctionKernel NLExecutor::selectFunction<PointComponentFunction<PointPart::CRS>>(const Column* input, bool inputNullable, LocalMemory* memory, Column*& result);
template NLUnaryFunctionKernel NLExecutor::selectFunction<PointComponentFunction<PointPart::SRID>>(const Column* input, bool inputNullable, LocalMemory* memory, Column*& result);
template NLUnaryFunctionKernel NLExecutor::selectFunction<ListHeadFunction>(const Column* input, bool inputNullable, LocalMemory* memory, Column*& result);
template NLUnaryFunctionKernel NLExecutor::selectFunction<ListLastFunction>(const Column* input, bool inputNullable, LocalMemory* memory, Column*& result);
template NLUnaryFunctionKernel NLExecutor::selectFunction<ListTailFunction>(const Column* input, bool inputNullable, LocalMemory* memory, Column*& result);
template NLUnaryFunctionKernel NLExecutor::selectFunction<ToIntegerListFunction>(const Column* input, bool inputNullable, LocalMemory* memory, Column*& result);
template NLUnaryFunctionKernel NLExecutor::selectFunction<ToFloatListFunction>(const Column* input, bool inputNullable, LocalMemory* memory, Column*& result);
template NLUnaryFunctionKernel NLExecutor::selectFunction<ToBooleanListFunction>(const Column* input, bool inputNullable, LocalMemory* memory, Column*& result);
template NLUnaryFunctionKernel NLExecutor::selectFunction<ToStringListFunction>(const Column* input, bool inputNullable, LocalMemory* memory, Column*& result);
template NLUnaryFunctionKernel NLExecutor::selectFunction<CharLengthFunction>(const Column* input, bool inputNullable, LocalMemory* memory, Column*& result);
template NLUnaryFunctionKernel NLExecutor::selectFunction<ToUpperFunction>(const Column* input, bool inputNullable, LocalMemory* memory, Column*& result);
template NLUnaryFunctionKernel NLExecutor::selectFunction<ToLowerFunction>(const Column* input, bool inputNullable, LocalMemory* memory, Column*& result);
template NLUnaryFunctionKernel NLExecutor::selectFunction<TrimFunction<TrimSide::Start>>(const Column* input, bool inputNullable, LocalMemory* memory, Column*& result);
template NLUnaryFunctionKernel NLExecutor::selectFunction<TrimFunction<TrimSide::End>>(const Column* input, bool inputNullable, LocalMemory* memory, Column*& result);
template NLUnaryFunctionKernel NLExecutor::selectFunction<TrimFunction<TrimSide::Both>>(const Column* input, bool inputNullable, LocalMemory* memory, Column*& result);
template NLUnaryFunctionKernel NLExecutor::selectFunction<NormalizeFunction<UnicodeNormalForm::NFC>>(const Column* input, bool inputNullable, LocalMemory* memory, Column*& result);
template NLUnaryFunctionKernel NLExecutor::selectFunction<NormalizeFunction<UnicodeNormalForm::NFD>>(const Column* input, bool inputNullable, LocalMemory* memory, Column*& result);
template NLUnaryFunctionKernel NLExecutor::selectFunction<NormalizeFunction<UnicodeNormalForm::NFKC>>(const Column* input, bool inputNullable, LocalMemory* memory, Column*& result);
template NLUnaryFunctionKernel NLExecutor::selectFunction<NormalizeFunction<UnicodeNormalForm::NFKD>>(const Column* input, bool inputNullable, LocalMemory* memory, Column*& result);
template NLUnaryFunctionKernel NLExecutor::selectFunction<IsNormalizedFunction<UnicodeNormalForm::NFC>>(const Column* input, bool inputNullable, LocalMemory* memory, Column*& result);
template NLUnaryFunctionKernel NLExecutor::selectFunction<IsNormalizedFunction<UnicodeNormalForm::NFD>>(const Column* input, bool inputNullable, LocalMemory* memory, Column*& result);
template NLUnaryFunctionKernel NLExecutor::selectFunction<IsNormalizedFunction<UnicodeNormalForm::NFKC>>(const Column* input, bool inputNullable, LocalMemory* memory, Column*& result);
template NLUnaryFunctionKernel NLExecutor::selectFunction<IsNormalizedFunction<UnicodeNormalForm::NFKD>>(const Column* input, bool inputNullable, LocalMemory* memory, Column*& result);

template NLUnaryFunctionKernel NLExecutor::selectConversion<ToIntegerFunction>(const Column* input, bool inputNullable, LocalMemory* memory, Column*& result);
template NLUnaryFunctionKernel NLExecutor::selectConversion<ToFloatFunction>(const Column* input, bool inputNullable, LocalMemory* memory, Column*& result);
template NLUnaryFunctionKernel NLExecutor::selectConversion<ToStringFunction>(const Column* input, bool inputNullable, LocalMemory* memory, Column*& result);
template NLUnaryFunctionKernel NLExecutor::selectConversion<ToBoolFunction>(const Column* input, bool inputNullable, LocalMemory* memory, Column*& result);
template NLUnaryFunctionKernel NLExecutor::selectConversion<ToDateTimeFunction>(const Column* input, bool inputNullable, LocalMemory* memory, Column*& result);
template NLUnaryFunctionKernel NLExecutor::selectConversion<ToDurationFunction>(const Column* input, bool inputNullable, LocalMemory* memory, Column*& result);
template NLUnaryFunctionKernel NLExecutor::selectConversion<ToPointFunction>(const Column* input, bool inputNullable, LocalMemory* memory, Column*& result);

template <auto StrictSelector, typename TaggedFunctor>
NLUnaryFunctionKernel NLExecutor::selectOrNull(const Column* input, bool inputNullable, LocalMemory* memory, Column*& result) {
    if (readsTaggedCells(input)) {
        return selectTaggedCellFunction<TaggedFunctor>(input, memory, result);
    }

    return StrictSelector(input, inputNullable, memory, result);
}

template NLUnaryFunctionKernel NLExecutor::selectOrNull<&NLExecutor::selectConversion<ToIntegerFunction>, TaggedToIntegerOrNullFunction>(const Column* input, bool inputNullable, LocalMemory* memory, Column*& result);
template NLUnaryFunctionKernel NLExecutor::selectOrNull<&NLExecutor::selectConversion<ToFloatFunction>, TaggedToFloatOrNullFunction>(const Column* input, bool inputNullable, LocalMemory* memory, Column*& result);
template NLUnaryFunctionKernel NLExecutor::selectOrNull<&NLExecutor::selectConversion<ToBoolFunction>, TaggedToBooleanOrNullFunction>(const Column* input, bool inputNullable, LocalMemory* memory, Column*& result);
template NLUnaryFunctionKernel NLExecutor::selectOrNull<&NLExecutor::selectConversion<ToStringFunction>, TaggedToStringOrNullFunction>(const Column* input, bool inputNullable, LocalMemory* memory, Column*& result);

#include "NLExecutor.h"

#include <algorithm>
#include <cmath>
#include <limits>
#include <span>
#include <string>
#include <string_view>
#include <type_traits>

#include <spdlog/fmt/bundled/format.h>

#include "TypeUtils.h"
#include "iterators/GetEdgesIterator.h"
#include "ID.h"
#include "iterators/GetInEdgesIterator.h"
#include "iterators/GetInEdgesByTypeIterator.h"
#include "iterators/GetNodeLabelSetIterator.h"
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
#include "columns/Functions.h"
#include "columns/UnaryPredicates.h"
#include "list/ListElementOrder.h"
#include "list/ListUtils.h"
#include "metadata/PropertyNull.h"
#include "metadata/PropertyType.h"

#include "reader/GraphReader.h"
#include "versioning/CommitWriteBuffer.h"
#include "views/GraphView.h"

#include "CSVParser.h"

#include "SystemAccessor.h"
#include "SystemManager.h"
#include "TuringConfig.h"
#include "VecLibAccessor.h"
#include "VectorDatabase.h"
#include "VectorSearchQuery.h"
#include "VectorSearchResult.h"

#include "NLSystemContext.h"

#include "NLProgram.h"
#include "NLOutputSink.h"

#include "LocalMemory.h"
#include "IRException.h"
#include "BioAssert.h"

using namespace db;

namespace {

// Copy a slice of a ListView's tagged scalars straight into a
// ColumnVector<ListElementView> - the heterogeneous unwind's type-erased column.
void fillListElementChunk(Column* output, const ListView list, size_t offset, size_t rows) {
    ColumnVector<ListElementView>* typed = static_cast<ColumnVector<ListElementView>*>(output);
    const std::span<const ListElementView> elements = list.elements();

    std::vector<ListElementView>& raw = typed->getRaw();
    raw.assign(elements.begin() + offset, elements.begin() + offset + rows);
}

// Fill a slice of a ListView into a nullable value column, extracting each element as
// the homogeneous primitive. The homogeneous unwind's fast path, ported from
// UnwindProcessor::fillHomogeneous onto the ColumnOptVector every other value-chunk
// consumer reads; a literal list holds no null, so every cell is present.
void fillHomogeneousChunk(Column* output, ValueType valueType, const ListView list, size_t offset, size_t rows) {
    const std::span<const ListElementView> elements = list.elements();

    const auto fill = [&]<SupportedType T>() {
        using Primitive = typename T::Primitive;

        ColumnOptVector<Primitive>* typed = static_cast<ColumnOptVector<Primitive>*>(output);

        std::vector<std::optional<Primitive>>& raw = typed->getRaw();
        raw.resize(rows);

        // ListElementView::getAs reinterprets the element's bytes as the primitive
        // without consulting its type tag, so check the tag against the one the
        // primitive is stored under: a homogeneous unwind whose column type disagrees
        // with its literals would otherwise read a value of the wrong type.
        constexpr ListBufferTypeTag expectedTag = TypeToListBufferTag<Primitive>::Tag;

        for (size_t index = 0; index < rows; index++) {
            const ListElementView element = elements[offset + index];
            bioassert(element.getTag() == expectedTag, "Unwound element does not have the unwind's value type.");

            raw[index] = element.getAs<Primitive>();
        }
    };

    ValueTypeDispatcher {valueType}.execute(fill);
}

// The rows one cell of a list column unwinds into: one per element, so an empty list
// contributes none.
size_t unwindListElementCount(const Column* source, size_t row) {
    const auto* lists = static_cast<const ColumnVector<ListView>*>(source);
    return (*lists)[row].size();
}

// The rows one cell of a nullable value column unwinds into: the single row a present
// value is, and none for a null - Cypher's UNWIND of a null.
template <typename Primitive>
size_t unwindOptElementCount(const Column* source, size_t row) {
    const auto* values = static_cast<const ColumnOptVector<Primitive>*>(source);
    return (*values)[row].has_value() ? 1 : 0;
}

// The rows one cell of a column holding a value in every row unwinds into: the single
// row that value is.
size_t unwindValueElementCount(const Column* source, size_t row) {
    return 1;
}

// Copy the tagged elements one step covers out of a list column into the type-erased
// element column: each output row is the element at its position in the list its source
// row holds.
void unwindListElementEmit(const Column* source,
                           const ColumnVector<size_t>* rows,
                           const ColumnVector<size_t>* positions,
                           Column* output) {
    const std::vector<ListView>& lists = static_cast<const ColumnVector<ListView>*>(source)->getRaw();
    const std::vector<size_t>& rowsRaw = rows->getRaw();
    const std::vector<size_t>& positionsRaw = positions->getRaw();

    std::vector<ListElementView>& outputRaw = static_cast<ColumnVector<ListElementView>*>(output)->getRaw();
    outputRaw.resize(rowsRaw.size());

    for (size_t index = 0; index < rowsRaw.size(); index++) {
        outputRaw[index] = lists[rowsRaw[index]].elements()[positionsRaw[index]];
    }
}

// Copy the elements one step covers out of a list column into a typed value column: each
// output row is the element at its position in the list its source row holds. The list's
// element type is the column's, so every element is present and shares that type - the
// tag check is what holds a list whose elements disagree with it to that promise.
template <typename Primitive>
void unwindListValueEmit(const Column* source,
                         const ColumnVector<size_t>* rows,
                         const ColumnVector<size_t>* positions,
                         Column* output) {
    const std::vector<ListView>& lists = static_cast<const ColumnVector<ListView>*>(source)->getRaw();
    const std::vector<size_t>& rowsRaw = rows->getRaw();
    const std::vector<size_t>& positionsRaw = positions->getRaw();

    std::vector<std::optional<Primitive>>& outputRaw = static_cast<ColumnOptVector<Primitive>*>(output)->getRaw();
    outputRaw.resize(rowsRaw.size());

    constexpr ListBufferTypeTag expectedTag = TypeToListBufferTag<Primitive>::Tag;

    for (size_t index = 0; index < rowsRaw.size(); index++) {
        const ListElementView element = lists[rowsRaw[index]].elements()[positionsRaw[index]];
        bioassert(element.getTag() == expectedTag, "Unwound element does not have the unwound list's value type.");

        outputRaw[index] = element.getAs<Primitive>();
    }
}

// The present-in-every-row sibling of unwindListValueEmit: an entity ID and a nested list
// are always there, so the elements drain into a plain column rather than a nullable one.
template <typename Element>
void unwindListPlainEmit(const Column* source,
                         const ColumnVector<size_t>* rows,
                         const ColumnVector<size_t>* positions,
                         Column* output) {
    const std::vector<ListView>& lists = static_cast<const ColumnVector<ListView>*>(source)->getRaw();
    const std::vector<size_t>& rowsRaw = rows->getRaw();
    const std::vector<size_t>& positionsRaw = positions->getRaw();

    std::vector<Element>& outputRaw = static_cast<ColumnVector<Element>*>(output)->getRaw();
    outputRaw.resize(rowsRaw.size());

    constexpr ListBufferTypeTag expectedTag = TypeToListBufferTag<Element>::Tag;

    for (size_t index = 0; index < rowsRaw.size(); index++) {
        const ListElementView element = lists[rowsRaw[index]].elements()[positionsRaw[index]];
        bioassert(element.getTag() == expectedTag, "Unwound element does not have the unwound list's element type.");

        outputRaw[index] = element.getAs<Element>();
    }
}

// The rows one cell of a type-erased column unwinds into: a tagged list its elements, a
// tagged null none, and any other tagged scalar the single row it is.
size_t unwindTaggedElementCount(const Column* source, size_t row) {
    const auto* elements = static_cast<const ColumnVector<ListElementView>*>(source);
    const ListElementView element = (*elements)[row];
    const ListBufferTypeTag tag = element.getTag();

    if (tag == ListBufferTypeTag::ListView) {
        return element.getAs<ListView>().size();
    }

    return tag == ListBufferTypeTag::Null ? 0 : 1;
}

// Fill the element chunk from a type-erased column: a cell holding a nested list gives up
// the element at this row's position, and any other cell is itself the element.
void unwindTaggedElementEmit(const Column* source,
                             const ColumnVector<size_t>* rows,
                             const ColumnVector<size_t>* positions,
                             Column* output) {
    const std::vector<ListElementView>& elements = static_cast<const ColumnVector<ListElementView>*>(source)->getRaw();
    const std::vector<size_t>& rowsRaw = rows->getRaw();
    const std::vector<size_t>& positionsRaw = positions->getRaw();

    std::vector<ListElementView>& outputRaw = static_cast<ColumnVector<ListElementView>*>(output)->getRaw();
    outputRaw.resize(rowsRaw.size());

    for (size_t index = 0; index < rowsRaw.size(); index++) {
        const ListElementView element = elements[rowsRaw[index]];

        outputRaw[index] = element.getTag() == ListBufferTypeTag::ListView
                               ? element.getAs<ListView>().elements()[positionsRaw[index]]
                               : element;
    }
}

template <typename Functor>
Functor makeFunctor(NLExecutionContext* context) {
    if constexpr (std::is_constructible_v<Functor, GraphView>) {
        return Functor(*context->getView());
    } else {
        return Functor {};
    }
}

template <typename Functor>
void functionConstKernel(NLExecutionContext* context, Column* result, const Column* input) {
    using Arg = typename Functor::ArgType;
    using Res = typename Functor::ResultType;

    const auto* typedInput = dynamic_cast<const ColumnConst<Arg>*>(input);
    bioassert(typedInput, "Function operand has an unexpected column type.");
    auto* output = static_cast<ColumnConst<Res>*>(result);

    Functor functor = makeFunctor<Functor>(context);
    output->set(functor(typedInput->getRaw()));
}

// A null constant argument converts to a null result whatever the function; the
// ColumnConst<PropertyNull> result already reads as null, so nothing is computed.
void functionNullKernel(NLExecutionContext*, Column*, const Column*) {
}

template <typename Functor, typename Element>
void applyFunctionOverVector(Functor& functor,
                             const ColumnVector<Element>* input,
                             ColumnVector<typename Functor::ResultType>* output) {
    const auto& inputRaw = input->getRaw();
    const size_t size = inputRaw.size();

    output->resize(size);
    auto& outputRaw = output->getRaw();

    for (size_t row = 0; row < size; row++) {
        outputRaw[row] = functor(inputRaw[row]);
    }
}

template <typename Functor>
void functionVectorKernel(NLExecutionContext* context, Column* result, const Column* input) {
    using Arg = typename Functor::ArgType;
    using Res = typename Functor::ResultType;

    auto* output = static_cast<ColumnVector<Res>*>(result);
    Functor functor = makeFunctor<Functor>(context);

    if (const auto* typedInput = dynamic_cast<const ColumnVector<Arg>*>(input)) {
        applyFunctionOverVector(functor, typedInput, output);
        return;
    }

    // Fallback for functions which take a string, but which may be provdied a column of
    // std::strings or std::string_views
    if constexpr (std::is_same_v<Arg, types::String::Primitive>) {
        if (const auto* ownedInput = dynamic_cast<const ColumnVector<types::String::OwningPrimitive>*>(input)) {
            applyFunctionOverVector(functor, ownedInput, output);
            return;
        }
    }

    bioassert(false, "Function operand has an unexpected column type.");
}

template <typename Functor>
void functionOptKernel(NLExecutionContext* context, Column* result, const Column* input) {
    using Arg = typename Functor::ArgType;
    using Res = typename Functor::ResultType;
    using JustRes = TypeUtils::unwrap_optional_t<Res>;

    const auto* typedInput = dynamic_cast<const ColumnOptVector<Arg>*>(input);
    bioassert(typedInput, "Function operand has an unexpected column type.");
    auto* output = static_cast<ColumnOptVector<JustRes>*>(result);

    const auto& inputRaw = typedInput->getRaw();
    const size_t size = inputRaw.size();

    output->resize(size);
    auto& outputRaw = output->getRaw();

    Functor functor = makeFunctor<Functor>(context);
    for (size_t row = 0; row < size; row++) {
        if (inputRaw[row].has_value()) {
            outputRaw[row] = functor(inputRaw[row].value());
        } else {
            outputRaw[row] = std::nullopt;
        }
    }
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
struct BinaryOpTraits<OP_FUNC_COSINE_SIMILARITY> {
    using Functor = CosineSimilarityFunction;

    template <typename ResCol, typename LhsCol, typename RhsCol>
    static void exec(ResCol* result, const LhsCol* lhs, const RhsCol* rhs) {
        ColumnFunctions::exec<CosineSimilarity>(result, lhs, rhs);
    }
};

template <>
struct BinaryOpTraits<OP_FUNC_EUCLIDEAN_DISTANCE> {
    using Functor = EuclideanDistanceFunction;

    template <typename ResCol, typename LhsCol, typename RhsCol>
    static void exec(ResCol* result, const LhsCol* lhs, const RhsCol* rhs) {
        ColumnFunctions::exec<EuclideanDistance>(result, lhs, rhs);
    }
};

template <ColumnOperator Op, typename ResCol, typename LhsCol, typename RhsCol>
struct BinaryOpKernel {
    static void run(Column* result, const Column* lhs, const Column* rhs, LocalMemory*) {
        BinaryOpTraits<Op>::exec(static_cast<ResCol*>(result),
                                 static_cast<const LhsCol*>(lhs),
                                 static_cast<const RhsCol*>(rhs));
    }
};

template <typename ResCol, typename LhsCol, typename RhsCol>
struct BinaryOpKernel<OP_CONCAT, ResCol, LhsCol, RhsCol> {
    static void run(Column* result, const Column* lhs, const Column* rhs, LocalMemory* mem) {
        BinaryOperators::exec<Concat>(static_cast<ResCol*>(result),
                                      static_cast<const LhsCol*>(lhs),
                                      static_cast<const RhsCol*>(rhs),
                                      Concat {&mem->stringBuffer(), &mem->listBuffer()});
    }
};

template <ColumnOperator Op, typename ResCol, typename LhsCol, typename RhsCol>
void applyBinaryOp(Column* result, const Column* lhs, const Column* rhs, LocalMemory* mem) {
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

// Execute a body of statements
void runBody(NLExecutionContext* context, const NLStmtContainer* body) {
    for (const NLFunctionDescriptor& descriptor : body->stmts()) {
        const auto func = descriptor.getFunction();
        NLFunctionData* funcData = descriptor.getData();
        func(context, funcData);
    }
}

// The position of each field a CSV load produces, in field order: the one it named, or
// the one its header sits at in the file's header line. A position past the file's last
// field, or a header the file does not carry, is a query naming a field that is not
// there, so it is reported rather than left to read an absent column.
void resolveCSVFieldIndices(const NLLoadCSVLoopData& loopData,
                            const CSVFileInfo& fileInfo,
                            std::vector<size_t>& indices) {
    const std::vector<std::string>& headers = fileInfo._headers;

    indices.reserve(loopData.fields().size());

    for (const NLLoadCSVLoopData::Field& field : loopData.fields()) {
        if (field._byHeader) {
            const auto foundIt = std::ranges::find(headers, field._header);
            if (foundIt == end(headers)) {
                throw IRException(fmt::format("CSV header '{}' not found in '{}'",
                                              field._header,
                                              loopData.getPath()));
            }

            indices.push_back(static_cast<size_t>(std::distance(begin(headers), foundIt)));
        } else {
            if (field._index >= fileInfo._fieldCount) {
                throw IRException(fmt::format("CSV field {} is out of range: '{}' carries {} fields",
                                              field._index,
                                              loopData.getPath(),
                                              fileInfo._fieldCount));
            }

            indices.push_back(field._index);
        }
    }
}

// Gather rows of a carried column by applying indices
template <typename ElementType>
void gatherColumn(const Column* input,
                  const ColumnVector<size_t>* indices,
                  Column* output) {
    const ColumnVector<ElementType>* typedInput = static_cast<const ColumnVector<ElementType>*>(input);
    ColumnVector<ElementType>* typedOutput = static_cast<ColumnVector<ElementType>*>(output);

    typedOutput->resize(indices->size());
    const auto& indicesRaw = indices->getRaw();
    auto& typedInputRaw = typedInput->getRaw();
    auto& typedOutputRaw = typedOutput->getRaw();

    for (size_t row = 0; row < indicesRaw.size(); row++) {
        typedOutputRaw[row] = typedInputRaw[indicesRaw[row]];
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

void blockRepeatConstColumn(const Column* input, size_t factor, size_t outputRowCount, Column* output) {
    output->assignFromLine(input, 0, outputRowCount);
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

// Constant broadcast: the one value a ColumnConst holds is laid out over every row
// of the step, as a present value. The two broadcasts above repeat the several
// values of a chunk; a constant column has no rows of its own to repeat, so its row
// count comes from the driving relation and its value from the column itself.
template <typename Primitive>
void broadcastConstantColumn(const Column* value, size_t rowCount, Column* output) {
    const ColumnConst<Primitive>* typedValue = static_cast<const ColumnConst<Primitive>*>(value);
    ColumnOptVector<Primitive>* typedOutput = static_cast<ColumnOptVector<Primitive>*>(output);

    auto& outputRaw = typedOutput->getRaw();
    outputRaw.resize(rowCount);

    std::fill_n(outputRaw.begin(), rowCount, std::optional<Primitive>(typedValue->getRaw()));
}

// The null literal laid out over every row of the step: it holds no value to repeat, so
// each row is the absent value. An untyped null is carried as a null integer, which is
// the column the layout fills.
void broadcastNullColumn(const Column* value, size_t rowCount, Column* output) {
    ColumnOptVector<int64_t>* typedOutput = static_cast<ColumnOptVector<int64_t>*>(output);

    auto& outputRaw = typedOutput->getRaw();
    outputRaw.assign(rowCount, std::optional<int64_t> {});
}

// The list sibling of broadcastConstantColumn: a list cell is a view over the query's
// list buffer and is never absent, so the rows are a plain column of that one view.
void broadcastConstantListColumn(const Column* value, size_t rowCount, Column* output) {
    const ColumnConst<ListView>* typedValue = static_cast<const ColumnConst<ListView>*>(value);
    ColumnVector<ListView>* typedOutput = static_cast<ColumnVector<ListView>*>(output);

    auto& outputRaw = typedOutput->getRaw();
    outputRaw.resize(rowCount);

    std::fill_n(outputRaw.begin(), rowCount, typedValue->getRaw());
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

void copyRangeConstColumn(const Column* input, size_t inputOffset, size_t rowCount, Column* output) {
    output->assignFromLine(input, inputOffset, rowCount);
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

// 3-way compare two rows of a non-null orderable column (an ID column, or a plain scalar
// a procedure yielded): negative if row a sorts before row b, positive if after, zero if
// they are equal.
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

// Copy a plain value column into a nullable one with every row present (nl.to_nullable),
// so a kernel reading a nullable value column takes a column a procedure yielded.
template <typename Primitive>
void toNullableColumn(Column* result, const Column* operand) {
    const auto& values = static_cast<const ColumnVector<Primitive>*>(operand)->getRaw();
    auto& nullables = static_cast<ColumnOptVector<Primitive>*>(result)->getRaw();

    nullables.resize(values.size());
    std::copy(values.begin(), values.end(), nullables.begin());
}

template <typename Primitive>
void toNullableConst(Column* result, const Column* operand) {
    const auto* constant = static_cast<const ColumnConst<Primitive>*>(operand);
    auto* nullable = static_cast<ColumnConst<std::optional<Primitive>>*>(result);

    if (constant->empty()) {
        nullable->clear();
        return;
    }

    *nullable = std::optional<Primitive>((*constant)[0]);
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

// 3-way compare two rows of a type-erased column of tagged scalars. Cells need not share
// a type, so a pair of different types compares by the order those types sort in; nulls
// tie and sort after every value, as they do in a nullable value column.
int compareListElementColumn(const Column* column, size_t a, size_t b) {
    const auto& raw = static_cast<const ColumnVector<ListElementView>*>(column)->getRaw();
    const std::strong_ordering order = raw[a] <=> raw[b];

    if (order == std::strong_ordering::less) {
        return -1;
    } else if (order == std::strong_ordering::greater) {
        return 1;
    }

    return 0;
}

// 3-way compare two rows of a collected list column. Two lists order lexicographically
// on the element order above, so a list can be the key the rows are sorted on.
int compareListColumn(const Column* column, size_t a, size_t b) {
    const auto& raw = static_cast<const ColumnVector<ListView>*>(column)->getRaw();
    const std::strong_ordering order = raw[a] <=> raw[b];

    if (order == std::strong_ordering::less) {
        return -1;
    } else if (order == std::strong_ordering::greater) {
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

// An owned string would otherwise pick the trivially-copyable template and key on the
// object's own bytes - a pointer into its buffer - so two equal strings at different
// addresses would count as two.
void distinctAppendValueBytes(std::string& key, const std::string& value) {
    distinctAppendValueBytes(key, std::string_view(value));
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

// Serialize one row of a chunk that holds its values plainly - a column a procedure
// yielded, a tally, an expression over one - into the row key. The ID sibling reads
// through the ID's integer; here the element is the value, and no row of such a chunk is
// null, so the key carries no tag byte to tell a null from a value.
template <typename ElementType>
void distinctKeyAppendPlainColumn(const Column* column, size_t row, std::string& key) {
    const auto& raw = static_cast<const ColumnVector<ElementType>*>(column)->getRaw();
    distinctAppendValueBytes(key, raw[row]);
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

void distinctAppendElementBytes(std::string& key, ListElementView element);

// Serialize a nested list into the row key as its length then its elements, so a list
// never keys the same as the concatenation of its neighbours.
void distinctAppendListBytes(std::string& key, const ListView list) {
    const size_t size = list.size();
    key.append(reinterpret_cast<const char*>(&size), sizeof(size));

    for (const ListElementView element : list) {
        distinctAppendElementBytes(key, element);
    }
}

// Serialize a number by its value rather than by the type it is tagged with: an integer
// and a float holding the same value are one Cypher value, so a float with no fractional
// part keys as that integer and only a fractional one keys as a double.
void distinctAppendNumberBytes(std::string& key, const ListElementView element) {
    const auto appendInteger = [&key](types::Int64::Primitive value) {
        key.push_back(static_cast<char>(ListBufferTypeTag::Int));
        distinctAppendValueBytes(key, value);
    };

    switch (element.getTag()) {
        case ListBufferTypeTag::Int:
            return appendInteger(element.getAs<types::Int64::Primitive>());
        break;

        case ListBufferTypeTag::UInt: {
            const types::UInt64::Primitive value = element.getAs<types::UInt64::Primitive>();
            if (value <= static_cast<types::UInt64::Primitive>(std::numeric_limits<types::Int64::Primitive>::max())) {
                return appendInteger(static_cast<types::Int64::Primitive>(value));
            }

            key.push_back(static_cast<char>(ListBufferTypeTag::UInt));
            distinctAppendValueBytes(key, value);
            return;
        }
        break;

        case ListBufferTypeTag::Double: {
            constexpr types::Double::Primitive integerBound = 9223372036854775808.0;

            const types::Double::Primitive value = element.getAs<types::Double::Primitive>();
            const types::Double::Primitive truncated = std::trunc(value);
            const bool holdsAnInteger = std::isfinite(value)
                                     && truncated == value
                                     && std::abs(value) < integerBound;

            if (holdsAnInteger) {
                return appendInteger(static_cast<types::Int64::Primitive>(truncated));
            }

            key.push_back(static_cast<char>(ListBufferTypeTag::Double));
            distinctAppendValueBytes(key, value);
            return;
        }
        break;

        default:
            bioassert(false, "Keying a non-numeric element as a number");
        break;
    }
}

// Serialize one tagged scalar into the row key: its tag, then its value's bytes. Cells of
// two types never collide because the tag leads, and a null is the tag alone, so all
// nulls dedup together as they do in a nullable value column.
void distinctAppendElementBytes(std::string& key, const ListElementView element) {
    const ListBufferTypeTag tag = element.getTag();

    switch (tag) {
        case ListBufferTypeTag::Int:
        case ListBufferTypeTag::UInt:
        case ListBufferTypeTag::Double:
            return distinctAppendNumberBytes(key, element);
        break;

        case ListBufferTypeTag::Bool:
            key.push_back(static_cast<char>(tag));
            distinctAppendValueBytes(key, static_cast<bool>(element.getAs<types::Bool::Primitive>()));
            return;
        break;

        case ListBufferTypeTag::String:
            key.push_back(static_cast<char>(tag));
            distinctAppendValueBytes(key, element.getAs<types::String::Primitive>());
            return;
        break;

        case ListBufferTypeTag::ListView:
            key.push_back(static_cast<char>(tag));
            distinctAppendListBytes(key, element.getAs<ListView>());
            return;
        break;

        case ListBufferTypeTag::Null:
            key.push_back(static_cast<char>(tag));
            return;
        break;

        case ListBufferTypeTag::NodeID:
            key.push_back(static_cast<char>(tag));
            distinctAppendValueBytes(key, element.getAs<NodeID>().getValue());
            return;
        break;

        case ListBufferTypeTag::EdgeID:
            key.push_back(static_cast<char>(tag));
            distinctAppendValueBytes(key, element.getAs<EdgeID>().getValue());
            return;
        break;

        case ListBufferTypeTag::Embedding:
            throw IRException("cannot dedup by an embedding element");
        break;

        case ListBufferTypeTag::INVALID:
            throw IRException("cannot dedup by an untagged element");
        break;
    }

    bioassert(false, "Unknown ListBufferTypeTag");
}

// Serialize one row of a type-erased column of tagged scalars into the row key.
void distinctKeyAppendListElementColumn(const Column* column, size_t row, std::string& key) {
    const auto& raw = static_cast<const ColumnVector<ListElementView>*>(column)->getRaw();
    distinctAppendElementBytes(key, raw[row]);
}

// Serialize one row of a list column into the row key, so two rows key alike when their
// lists hold equal elements - the equality Cypher gives two lists.
void distinctKeyAppendListColumn(const Column* column, size_t row, std::string& key) {
    const auto& raw = static_cast<const ColumnVector<ListView>*>(column)->getRaw();
    distinctAppendListBytes(key, raw[row]);
}

// Count the non-null cells of a type-erased column of tagged scalars, so count(x) over a
// heterogeneous unwind charges the same rows a nullable value column would.
size_t countNonNullElementsColumn(const Column* column) {
    const auto& raw = static_cast<const ColumnVector<ListElementView>*>(column)->getRaw();
    return std::count_if(raw.begin(), raw.end(), [](const ListElementView element) {
        return element.getTag() != ListBufferTypeTag::Null;
    });
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

// The number a tagged cell holds, whatever numeric type its tag names. A reduction over
// a type-erased column is defined over numbers only, so any other tag is a query error -
// the check the static column types make for a typed column, made per row here.
double taggedNumericValue(const ListElementView element) {
    switch (element.getTag()) {
        case ListBufferTypeTag::Int:
            return static_cast<double>(element.getAs<types::Int64::Primitive>());
        break;

        case ListBufferTypeTag::UInt:
            return static_cast<double>(element.getAs<types::UInt64::Primitive>());
        break;

        case ListBufferTypeTag::Double:
            return element.getAs<types::Double::Primitive>();
        break;

        default:
            throw IRException("sum/avg over type-erased cells requires a numeric column");
        break;
    }
}

// Fold a chunk of tagged cells into a running f64 sum, counting the ones folded so avg
// reads the same accumulator. A cell tagged null is skipped, as a null value column row
// is. Mixed numeric tags are what makes a column type-erased, and Cypher sums those to a
// float, so the accumulator is an f64 whichever tags turn up.
template <bool CountsRows>
void aggregateUpdateNumericTagged(NLAggregateState* state, const Column* input) {
    auto* accumulator = static_cast<ColumnOptVector<double>*>(state->getAccumulator());
    std::optional<double>& current = accumulator->getRaw().front();
    const auto& inputRaw = static_cast<const ColumnVector<ListElementView>*>(input)->getRaw();

    double running = current.value();
    size_t seen = 0;

    for (const ListElementView element : inputRaw) {
        if (element.getTag() == ListBufferTypeTag::Null) {
            continue;
        }

        running += taggedNumericValue(element);
        seen++;
    }

    current = running;

    if constexpr (CountsRows) {
        state->addCount(seen);
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
                  const std::vector<size_t>& groups,
                  NLGroupDistinctTally& distinct) {
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

// Fold a chunk's distinct present values into per-group sum accumulators
// (sum(DISTINCT x)): a value its group has already been charged is skipped, so each
// distinct value is added once however many rows carry it.
template <typename Primitive>
void groupFoldSumDistinct(Column* accumulator,
                          std::vector<uint64_t>& counts,
                          const Column* input,
                          const std::vector<size_t>& groups,
                          NLGroupDistinctTally& distinct) {
    auto& raw = static_cast<ColumnOptVector<Primitive>*>(accumulator)->getRaw();
    const auto& inputRaw = static_cast<const ColumnOptVector<Primitive>*>(input)->getRaw();

    for (size_t row = 0; row < inputRaw.size(); row++) {
        const std::optional<Primitive>& value = inputRaw[row];
        if (!value.has_value()) {
            continue;
        }

        const size_t group = groups[row];

        distinct.beginKey(group);
        distinctAppendValueBytes(distinct.getKey(), *value);

        if (!distinct.insertIfNew()) {
            continue;
        }

        std::optional<Primitive>& running = raw[group];
        running = numericAdd(running.value(), *value);
    }
}

// Fold a group's tagged cells into its running f64 sum, tallying the ones folded so avg
// divides by the same count. The type-erased sibling of groupFoldSum / groupFoldAvg: the
// cells carry a type each, so the number each holds is read by its tag.
template <bool CountsRows, bool Distinct>
void groupFoldNumericTagged(Column* accumulator,
                            std::vector<uint64_t>& counts,
                            const Column* input,
                            const std::vector<size_t>& groups,
                            NLGroupDistinctTally& distinct) {
    auto& raw = static_cast<ColumnOptVector<double>*>(accumulator)->getRaw();
    const auto& inputRaw = static_cast<const ColumnVector<ListElementView>*>(input)->getRaw();

    for (size_t row = 0; row < inputRaw.size(); row++) {
        const ListElementView element = inputRaw[row];
        if (element.getTag() == ListBufferTypeTag::Null) {
            continue;
        }

        const size_t group = groups[row];

        if constexpr (Distinct) {
            distinct.beginKey(group);
            distinctAppendElementBytes(distinct.getKey(), element);

            if (!distinct.insertIfNew()) {
                continue;
            }
        }

        std::optional<double>& running = raw[group];
        running = running.value() + taggedNumericValue(element);

        if constexpr (CountsRows) {
            counts[group]++;
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
                     const std::vector<size_t>& groups,
                     NLGroupDistinctTally& distinct) {
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
                  const std::vector<size_t>& groups,
                  NLGroupDistinctTally& distinct) {
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

// Fold a chunk's distinct present values into per-group avg accumulators
// (avg(DISTINCT x)): a value its group has already been charged moves neither the
// running sum nor the count, so the mean is taken over the distinct values.
template <typename Primitive>
void groupFoldAvgDistinct(Column* accumulator,
                          std::vector<uint64_t>& counts,
                          const Column* input,
                          const std::vector<size_t>& groups,
                          NLGroupDistinctTally& distinct) {
    auto& raw = static_cast<ColumnOptVector<double>*>(accumulator)->getRaw();
    const auto& inputRaw = static_cast<const ColumnOptVector<Primitive>*>(input)->getRaw();

    for (size_t row = 0; row < inputRaw.size(); row++) {
        const std::optional<Primitive>& value = inputRaw[row];
        if (!value.has_value()) {
            continue;
        }

        const size_t group = groups[row];

        distinct.beginKey(group);
        distinctAppendValueBytes(distinct.getKey(), *value);

        if (!distinct.insertIfNew()) {
            continue;
        }

        std::optional<double>& running = raw[group];
        running = running.value() + static_cast<double>(*value);
        counts[group]++;
    }
}

// Tally every row into its group (count(*) over a never-null column): each row
// charges its group regardless of value, so the input values are never read.
void groupFoldCountAll(Column* accumulator,
                       std::vector<uint64_t>& counts,
                       const Column* input,
                       const std::vector<size_t>& groups,
                       NLGroupDistinctTally& distinct) {
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
                           const std::vector<size_t>& groups,
                           NLGroupDistinctTally& distinct) {
    const auto& inputRaw = static_cast<const ColumnOptVector<Primitive>*>(input)->getRaw();

    for (size_t row = 0; row < inputRaw.size(); row++) {
        if (inputRaw[row].has_value()) {
            counts[groups[row]]++;
        }
    }
}

// Tally each group's distinct IDs (count(DISTINCT n) over a node/edge column): an ID
// is never null, so every row is charged the first time its ID is seen in its group.
template <typename ElementType>
void groupFoldCountDistinctID(Column* accumulator,
                              std::vector<uint64_t>& counts,
                              const Column* input,
                              const std::vector<size_t>& groups,
                              NLGroupDistinctTally& distinct) {
    const auto& inputRaw = static_cast<const ColumnVector<ElementType>*>(input)->getRaw();

    for (size_t row = 0; row < inputRaw.size(); row++) {
        const size_t group = groups[row];

        distinct.beginKey(group);
        distinctAppendValueBytes(distinct.getKey(), inputRaw[row].getValue());

        if (distinct.insertIfNew()) {
            counts[group]++;
        }
    }
}

// Tally each group's distinct values over a chunk holding them plainly - a column a
// procedure yielded. No row of such a chunk is null, so every row is charged the first
// time its value is seen in its group; the ID sibling keys on the ID's integer instead.
template <typename ElementType>
void groupFoldCountDistinctValue(Column* accumulator,
                                 std::vector<uint64_t>& counts,
                                 const Column* input,
                                 const std::vector<size_t>& groups,
                                 NLGroupDistinctTally& distinct) {
    const auto& inputRaw = static_cast<const ColumnVector<ElementType>*>(input)->getRaw();

    for (size_t row = 0; row < inputRaw.size(); row++) {
        const size_t group = groups[row];

        distinct.beginKey(group);
        distinctAppendValueBytes(distinct.getKey(), inputRaw[row]);

        if (distinct.insertIfNew()) {
            counts[group]++;
        }
    }
}

// Tally each group's distinct present values (count(DISTINCT x)): a null row is not
// charged, and a value repeated within its group is charged once. The value bytes are
// the DISTINCT serializer's, so two rows count as one exactly when a whole-row DISTINCT
// would fold them together.
template <typename Primitive>
void groupFoldCountDistinctPresent(Column* accumulator,
                                   std::vector<uint64_t>& counts,
                                   const Column* input,
                                   const std::vector<size_t>& groups,
                                   NLGroupDistinctTally& distinct) {
    const auto& inputRaw = static_cast<const ColumnOptVector<Primitive>*>(input)->getRaw();

    for (size_t row = 0; row < inputRaw.size(); row++) {
        const std::optional<Primitive>& value = inputRaw[row];
        if (!value.has_value()) {
            continue;
        }

        const size_t group = groups[row];

        distinct.beginKey(group);
        distinctAppendValueBytes(distinct.getKey(), *value);

        if (distinct.insertIfNew()) {
            counts[group]++;
        }
    }
}

// Tally each group's present cells of a type-erased column of tagged scalars, so a
// grouped count(x) over a heterogeneous unwind charges the same rows a nullable value
// column would.
void groupFoldCountPresentListElement(Column* accumulator,
                                     std::vector<uint64_t>& counts,
                                     const Column* input,
                                     const std::vector<size_t>& groups,
                                     NLGroupDistinctTally& distinct) {
    const auto& inputRaw = static_cast<const ColumnVector<ListElementView>*>(input)->getRaw();

    for (size_t row = 0; row < inputRaw.size(); row++) {
        if (inputRaw[row].getTag() != ListBufferTypeTag::Null) {
            counts[groups[row]]++;
        }
    }
}

// Tally each group's distinct present cells of a type-erased column of tagged scalars.
// The key carries the tag as well as the value, so cells of different types are told
// apart the way a whole-row DISTINCT tells them apart.
void groupFoldCountDistinctListElement(Column* accumulator,
                                       std::vector<uint64_t>& counts,
                                       const Column* input,
                                       const std::vector<size_t>& groups,
                                       NLGroupDistinctTally& distinct) {
    const auto& inputRaw = static_cast<const ColumnVector<ListElementView>*>(input)->getRaw();

    for (size_t row = 0; row < inputRaw.size(); row++) {
        const ListElementView element = inputRaw[row];
        if (element.getTag() == ListBufferTypeTag::Null) {
            continue;
        }

        const size_t group = groups[row];

        distinct.beginKey(group);
        distinctAppendElementBytes(distinct.getKey(), element);

        if (distinct.insertIfNew()) {
            counts[group]++;
        }
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
                 NLGroupDistinctTally& distinct) {
    auto& valuesRaw = static_cast<ColumnVector<Primitive>*>(values)->getRaw();
    const auto& inputRaw = static_cast<const ColumnOptVector<Primitive>*>(input)->getRaw();

    for (size_t row = 0; row < inputRaw.size(); row++) {
        const std::optional<Primitive>& value = inputRaw[row];
        if (value.has_value()) {
            const size_t position = valuesRaw.size();
            valuesRaw.push_back(*value);
            groupPositions[groups[row]].push_back(position);
        }
    }
}

// Append this step's values a group has not collected yet - collect(DISTINCT x). A value
// repeated within its group is dropped rather than buffered a second time, so the list is
// the group's distinct values in first-seen order.
template <typename Primitive>
void collectFoldDistinct(Column* values,
                         const Column* input,
                         const std::vector<size_t>& groups,
                         std::vector<std::vector<size_t>>& groupPositions,
                         NLGroupDistinctTally& distinct) {
    auto& valuesRaw = static_cast<ColumnVector<Primitive>*>(values)->getRaw();
    const auto& inputRaw = static_cast<const ColumnOptVector<Primitive>*>(input)->getRaw();

    for (size_t row = 0; row < inputRaw.size(); row++) {
        const std::optional<Primitive>& value = inputRaw[row];
        if (!value.has_value()) {
            continue;
        }

        const size_t group = groups[row];

        distinct.beginKey(group);
        distinctAppendValueBytes(distinct.getKey(), *value);

        if (!distinct.insertIfNew()) {
            continue;
        }

        const size_t position = valuesRaw.size();
        valuesRaw.push_back(*value);
        groupPositions[group].push_back(position);
    }
}

// The entity sibling of collectFold: an ID chunk has no null rows, so every row of the
// input joins its group's list.
template <typename IDType>
void collectEntityFold(Column* values,
                       const Column* input,
                       const std::vector<size_t>& groups,
                       std::vector<std::vector<size_t>>& groupPositions,
                       NLGroupDistinctTally& distinct) {
    auto& valuesRaw = static_cast<ColumnVector<IDType>*>(values)->getRaw();
    const auto& inputRaw = static_cast<const ColumnVector<IDType>*>(input)->getRaw();

    const size_t base = valuesRaw.size();
    valuesRaw.insert(valuesRaw.end(), inputRaw.begin(), inputRaw.end());

    for (size_t row = 0; row < inputRaw.size(); row++) {
        groupPositions[groups[row]].push_back(base + row);
    }
}

// The entity sibling of collectFoldDistinct: an entity repeated within its group joins
// the list once, keyed by the ID's underlying integer.
template <typename IDType>
void collectEntityFoldDistinct(Column* values,
                               const Column* input,
                               const std::vector<size_t>& groups,
                               std::vector<std::vector<size_t>>& groupPositions,
                               NLGroupDistinctTally& distinct) {
    auto& valuesRaw = static_cast<ColumnVector<IDType>*>(values)->getRaw();
    const auto& inputRaw = static_cast<const ColumnVector<IDType>*>(input)->getRaw();

    for (size_t row = 0; row < inputRaw.size(); row++) {
        const size_t group = groups[row];

        distinct.beginKey(group);
        distinctAppendValueBytes(distinct.getKey(), inputRaw[row].getValue());

        if (!distinct.insertIfNew()) {
            continue;
        }

        const size_t position = valuesRaw.size();
        valuesRaw.push_back(inputRaw[row]);
        groupPositions[group].push_back(position);
    }
}

// The list-buffer value a tagged cell holds, read back as the type its tag names, so a
// collect of type-erased cells buffers each one under the type it came in with.
ListBuffer<>::ListItemVariant taggedListItem(const ListElementView element) {
    const ListBufferTypeTag tag = element.getTag();

    switch (tag) {
        case ListBufferTypeTag::Int:
            return ListBuffer<>::ListItemVariant {element.getAs<types::Int64::Primitive>()};
        break;

        case ListBufferTypeTag::UInt:
            return ListBuffer<>::ListItemVariant {element.getAs<types::UInt64::Primitive>()};
        break;

        case ListBufferTypeTag::Double:
            return ListBuffer<>::ListItemVariant {element.getAs<types::Double::Primitive>()};
        break;

        case ListBufferTypeTag::Bool:
            return ListBuffer<>::ListItemVariant {element.getAs<types::Bool::Primitive>()};
        break;

        case ListBufferTypeTag::String:
            return ListBuffer<>::ListItemVariant {element.getAs<types::String::Primitive>()};
        break;

        case ListBufferTypeTag::Embedding:
            return ListBuffer<>::ListItemVariant {element.getAs<types::Embedding::Primitive>()};
        break;

        case ListBufferTypeTag::ListView:
            return ListBuffer<>::ListItemVariant {element.getAs<ListView>()};
        break;

        case ListBufferTypeTag::Null:
            return ListBuffer<>::ListItemVariant {element.getAs<PropertyNull>()};
        break;

        case ListBufferTypeTag::NodeID:
            return ListBuffer<>::ListItemVariant {element.getAs<NodeID>()};
        break;

        case ListBufferTypeTag::EdgeID:
            return ListBuffer<>::ListItemVariant {element.getAs<EdgeID>()};
        break;

        case ListBufferTypeTag::INVALID:
            throw IRException("cannot collect an untagged element");
        break;
    }

    throw IRException("Unknown ListBufferTypeTag");
}

// The type-erased sibling of collectFold: a tagged cell is there in every row, but one
// tagged null is the null Cypher's collect drops, so only the rest join the group's list.
void collectTaggedFold(Column* values,
                       const Column* input,
                       const std::vector<size_t>& groups,
                       std::vector<std::vector<size_t>>& groupPositions,
                       NLGroupDistinctTally& distinct) {
    auto& valuesRaw = static_cast<ColumnVector<ListElementView>*>(values)->getRaw();
    const auto& inputRaw = static_cast<const ColumnVector<ListElementView>*>(input)->getRaw();

    for (size_t row = 0; row < inputRaw.size(); row++) {
        if (inputRaw[row].getTag() == ListBufferTypeTag::Null) {
            continue;
        }

        const size_t position = valuesRaw.size();
        valuesRaw.push_back(inputRaw[row]);
        groupPositions[groups[row]].push_back(position);
    }
}

// The dedup sibling: a tagged cell keys by the value its tag names, so the same number
// reached under two tags keys once - the ordering rules read these cells the same way.
void collectTaggedFoldDistinct(Column* values,
                               const Column* input,
                               const std::vector<size_t>& groups,
                               std::vector<std::vector<size_t>>& groupPositions,
                               NLGroupDistinctTally& distinct) {
    auto& valuesRaw = static_cast<ColumnVector<ListElementView>*>(values)->getRaw();
    const auto& inputRaw = static_cast<const ColumnVector<ListElementView>*>(input)->getRaw();

    for (size_t row = 0; row < inputRaw.size(); row++) {
        if (inputRaw[row].getTag() == ListBufferTypeTag::Null) {
            continue;
        }

        const size_t group = groups[row];

        distinct.beginKey(group);
        distinctAppendElementBytes(distinct.getKey(), inputRaw[row]);

        if (!distinct.insertIfNew()) {
            continue;
        }

        const size_t position = valuesRaw.size();
        valuesRaw.push_back(inputRaw[row]);
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
    const auto& valuesRaw = static_cast<const ColumnVector<ListElementView>*>(values)->getRaw();
    auto& outputRaw = static_cast<ColumnVector<ListView>*>(output)->getRaw();

    outputRaw.clear();
    outputRaw.reserve(count);

    std::vector<ListBuffer<>::ListItemVariant> elements;
    for (size_t index = 0; index < count; index++) {
        const std::vector<size_t>& positions = groupPositions[begin + index];

        elements.clear();
        elements.reserve(positions.size());
        for (const size_t position : positions) {
            elements.push_back(taggedListItem(valuesRaw[position]));
        }

        outputRaw.push_back(listBuffer.insert(elements));
    }
}

// The list sibling of collectEntityFoldDistinct: two cells are the same value when their
// elements are, so a cell keys by the list serialized element by element rather than by
// the ListView's own span, which two equal lists never share.
void collectListFoldDistinct(Column* values,
                             const Column* input,
                             const std::vector<size_t>& groups,
                             std::vector<std::vector<size_t>>& groupPositions,
                             NLGroupDistinctTally& distinct) {
    auto& valuesRaw = static_cast<ColumnVector<ListView>*>(values)->getRaw();
    const auto& inputRaw = static_cast<const ColumnVector<ListView>*>(input)->getRaw();

    for (size_t row = 0; row < inputRaw.size(); row++) {
        const size_t group = groups[row];

        distinct.beginKey(group);
        distinctAppendListBytes(distinct.getKey(), inputRaw[row]);

        if (!distinct.insertIfNew()) {
            continue;
        }

        const size_t position = valuesRaw.size();
        valuesRaw.push_back(inputRaw[row]);
        groupPositions[group].push_back(position);
    }
}

// Emit a chunk of unwound values (nl.unwind_collect): for each flat-buffer position this chunk
// covers, write the present value into the nullable value output. collect dropped
// nulls, so every emitted element is present.
template <typename Primitive>
void unwindCollectValueEmit(const Column* values,
                     const ColumnVector<size_t>* positions,
                     Column* output) {
    const auto& valuesRaw = static_cast<const ColumnVector<Primitive>*>(values)->getRaw();
    auto& outputRaw = static_cast<ColumnOptVector<Primitive>*>(output)->getRaw();
    const auto& positionsRaw = positions->getRaw();

    outputRaw.clear();
    outputRaw.reserve(positionsRaw.size());

    for (const size_t position : positionsRaw) {
        outputRaw.push_back(std::optional<Primitive>(valuesRaw[position]));
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

// The fold and the list emit an entity collect of this ID reads. The list emits through
// the value path's template - its elements are the IDs the fold appended - so there is
// no entity emit of its own.
template <typename IDType>
void selectCollectIDHandlers(bool distinctValues,
                             NLCollectFoldFunction& fold,
                             NLCollectListEmitFunction& listEmit) {
    fold = distinctValues ? &collectEntityFoldDistinct<IDType> : &collectEntityFold<IDType>;
    listEmit = &collectListEmit<IDType>;
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

// The grouped sum(DISTINCT x) fold for a column of this value type: sum's numeric
// domain, reduced over each group's distinct values.
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

// The grouped avg(DISTINCT x) fold for a column of this value type: avg's numeric
// domain, averaged over each group's distinct values.
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

        // Column many not have been allocated (=nullptr) if its var wasn't used
        if (gatheredNodeIDs) {
            gatherColumn<NodeID>(loopData->getInput(), indices, gatheredNodeIDs);
        }

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

class ConstPropertyExtractor {
public:
    ConstPropertyExtractor(CommitWriteBuffer::UntypedProperties& buf,
                           PropertyTypeID propID,
                           size_t rowCount)
        : _buf(buf),
        _propID(propID),
        _rowCount(rowCount)
    {
    }

    template <typename T>
    void operator()(const ColumnConst<T>* typed) {
        _buf.clear();
        _buf.reserve(_rowCount);
        for (size_t i = 0; i < _rowCount; i++) {
            _buf.emplace_back(_propID, typed->getRaw());
        }
    }

    void operator()(const ColumnConst<types::String::Primitive>* typed) {
        _buf.clear();
        _buf.reserve(_rowCount);
        for (size_t i = 0; i < _rowCount; i++) {
            _buf.emplace_back(_propID, std::string(typed->getRaw()));
        }
    }

    void operator()(const ColumnConst<types::Embedding::Primitive>* typed) {
        _buf.clear();
        _buf.reserve(_rowCount);
        const types::Embedding::Primitive span = typed->getRaw();
        for (size_t i = 0; i < _rowCount; i++) {
            _buf.emplace_back(_propID, types::Embedding::OwningPrimitive(span.begin(), span.end()));
        }
    }

private:
    CommitWriteBuffer::UntypedProperties& _buf;
    PropertyTypeID _propID;
    size_t _rowCount;
};

class VectorPropertyExtractor {
public:
    VectorPropertyExtractor(CommitWriteBuffer::UntypedProperties& buf,
                            PropertyTypeID propID)
        : _buf(buf),
        _propID(propID)
    {
    }

    template <typename T>
    void operator()(const ColumnVector<T>* typed) {
        _buf.clear();
        _buf.reserve(typed->size());
        for (const T& val : *typed) {
            _buf.emplace_back(_propID, val);
        }
    }

    void operator()(const ColumnVector<types::String::Primitive>* typed) {
        _buf.clear();
        _buf.reserve(typed->size());
        for (const types::String::Primitive val : *typed) {
            _buf.emplace_back(_propID, std::string(val));
        }
    }

    void operator()(const ColumnVector<types::Embedding::Primitive>* typed) {
        _buf.clear();
        _buf.reserve(typed->size());
        for (const types::Embedding::Primitive val : *typed) {
            _buf.emplace_back(_propID, types::Embedding::OwningPrimitive(val.begin(), val.end()));
        }
    }

    template <typename T>
    void operator()(const ColumnVector<std::optional<T>>* typed) {
        _buf.clear();
        _buf.reserve(typed->size());
        for (const std::optional<T>& val : *typed) {
            if (!val) {
                throw IRException("Cannot set a property to NULL in CREATE.");
            }
            _buf.emplace_back(_propID, *val);
        }
    }

    void operator()(const ColumnVector<std::optional<types::String::Primitive>>* typed) {
        _buf.clear();
        _buf.reserve(typed->size());
        for (const std::optional<types::String::Primitive>& val : *typed) {
            if (!val) {
                throw IRException("Cannot set a property to NULL in CREATE.");
            }
            _buf.emplace_back(_propID, std::string(*val));
        }
    }

    void operator()(const ColumnVector<std::optional<types::Embedding::Primitive>>* typed) {
        _buf.clear();
        _buf.reserve(typed->size());
        for (const std::optional<types::Embedding::Primitive>& val : *typed) {
            if (!val) {
                throw IRException("Cannot set a property to NULL in CREATE.");
            }
            _buf.emplace_back(_propID, types::Embedding::OwningPrimitive(val->begin(), val->end()));
        }
    }

private:
    CommitWriteBuffer::UntypedProperties& _buf;
    PropertyTypeID _propID;
};

void extractColumnProperties(const Column* column,
                             size_t rowCount,
                             PropertyTypeID propID,
                             CommitWriteBuffer::UntypedProperties& buf) {
    using Types = WriteProcessorPropertyTypes;

    const ContainerKind::Code containerKind = ColumnKind::extractContainerKind(column->getKind());

    if (containerKind == ContainerKind::code<ColumnConst>()) {
        ConstPropertyExtractor extractor(buf, propID, rowCount);
        ColumnSingleDispatcher<Types::AllowedConst,
                               ConstPropertyExtractor,
                               Types::ExcludedConst>::dispatch(column, extractor);
    } else {
        VectorPropertyExtractor extractor(buf, propID);
        ColumnSingleDispatcher<Types::AllowedVector,
                               VectorPropertyExtractor,
                               Types::ExcludedVector>::dispatch(column, extractor);
    }
}

size_t committedNodeCount(const GraphView* view) {
    if (!view || !view->isValid()) {
        return 0;
    }

    const GraphReader reader = view->read();
    return reader.getTotalNodesAllocated();
}

size_t committedEdgeCount(const GraphView* view) {
    if (!view || !view->isValid()) {
        return 0;
    }

    const GraphReader reader = view->read();
    return reader.getTotalEdgesAllocated();
}

CommitWriteBuffer::ExistingOrPendingNode resolveNode(const ColumnNodeIDs* column,
                                                     size_t row,
                                                     bool isPending,
                                                     size_t firstPendingNodeID) {
    const NodeID nodeID = (*column)[row];
    if (isPending) {
        return CommitWriteBuffer::PendingNodeOffset(nodeID.getValue() - firstPendingNodeID);
    } else {
        return nodeID;
    }
}

void throwIfNodesHaveEdges(const GraphView& view, const ColumnNodeIDs* nodes) {
    const Tombstones& tombstones = view.tombstones();

    const GetOutEdgesRange outEdges(view, nodes);
    for (const EdgeRecord& record : outEdges) {
        if (!tombstones.containsEdge(record._edgeID)) {
            throw IRException("Cannot delete a node with relationships; use DETACH DELETE");
        }
    }

    const GetInEdgesRange inEdges(view, nodes);
    for (const EdgeRecord& record : inEdges) {
        if (!tombstones.containsEdge(record._edgeID)) {
            throw IRException("Cannot delete a node with relationships; use DETACH DELETE");
        }
    }
}

// Rebuild every column carried past a call from the input rows the procedure reported
// for the rows it just emitted: an input row it emitted several rows for is repeated,
// one it emitted none for is dropped - the gather an edge hop replicates its carry set
// with, over a row map a callback filled rather than a chunk writer.
//
// Throws when the procedure reported no row for every row it emitted, or one outside
// the chunk it was handed: either would leave the carried columns misaligned, silently
// pairing the wrong rows in the projection, so it is caught at the step that did it.
void gatherProcedureCarriedColumns(NLProcedureLoopData* loopData) {
    NLProcedureState* state = loopData->getState();
    ColumnIndices* indices = loopData->getIndices();

    const size_t emittedRows = state->getRowCount();
    const std::vector<size_t>& indicesRaw = indices->getRaw();
    if (indicesRaw.size() != emittedRows) {
        throw IRException(fmt::format("Procedure '{}' emitted {} rows but reported the input row of "
                                      "{} of them, so the columns carried past the call cannot be "
                                      "aligned with its result",
                                      state->getProcedure()->getFullName(),
                                      emittedRows,
                                      indicesRaw.size()));
    }

    // Each index selects the input row a carried value is replicated from, so one out
    // of range would read past the chunk the procedure was handed.
    const size_t inputRows = state->getInputRowCount();
    for (const size_t inputRow : indicesRaw) {
        if (inputRow >= inputRows) {
            throw IRException(fmt::format("Procedure '{}' reported input row {} for a chunk of {} "
                                          "rows",
                                          state->getProcedure()->getFullName(),
                                          inputRow,
                                          inputRows));
        }
    }

    for (const NLCarriedColumn& carriedColumn : loopData->carriedColumns()) {
        const NLGatherFunction gather = carriedColumn.getGatherFunc();
        gather(carriedColumn.getInput(), indices, carriedColumn.getOutput());
    }
}

// Drive a procedure through one loop: each step runs its execute callback once through
// runStep, refilling the loop variables in place, then rebuilds any carried column and
// runs the body. The loop ends when the procedure declares itself finished, so one entry
// may cover as many chunks as it needs.
//
// The body sees only the steps that produced rows. A procedure declares itself finished
// on the step that exhausts it - which may still carry rows - so the flag is read after
// the call, not tested before it.
template <typename StepFunction>
void runProcedureDrive(NLExecutionContext* context,
                       NLProcedureLoopData* loopData,
                       StepFunction runStep) {
    NLProcedureState* state = loopData->getState();
    const NLStmtContainer* loopBody = loopData->getStmts();
    const NLProcedureLoopData::CarriedColumns& carriedColumns = loopData->carriedColumns();

    // A null limit leaves the drive unbounded; otherwise it stops once the budget is
    // spent, so a LIMIT ends the drive rather than running the procedure out. The limit
    // is fixed for the whole loop, so the null check is hoisted out of the per-iteration
    // condition, as the scan loops do.
    const NLLimitState* limit = loopData->getLimit();

    bool finished = false;
    const auto runIteration = [&]() {
        // The procedure reports the input row behind each row it emits, so clear the
        // map before the call: what it appends is this step's mapping alone, as its
        // result columns are.
        if (!carriedColumns.empty()) {
            loopData->getIndices()->clear();
        }

        runStep();
        finished = state->isFinished();

        if (state->getRowCount() == 0) {
            return;
        }

        if (!carriedColumns.empty()) {
            gatherProcedureCarriedColumns(loopData);
        }

        runBody(context, loopBody);
    };

    if (limit) {
        while (!finished && limit->getRemaining() > 0) {
            runIteration();
        }
    } else {
        while (!finished) {
            runIteration();
        }
    }
}

}

vec::VectorDatabase* NLExecutionContext::getVectorDatabase() const {
    SystemAccessor* const accessor = _system ? _system->getAccessor() : nullptr;

    return accessor ? accessor->getVectorDatabase() : nullptr;
}

const fs::Path* NLExecutionContext::getDataDir() const {
    const SystemManager* const manager = _system ? _system->getSystemManager() : nullptr;
    if (!manager) {
        return nullptr;
    }

    return &manager->getConfig()->getDataDir();
}

NLExecutor::NLExecutor(const GraphView* view,
                       const NLProgram* prog,
                       NLOutputSink* sink,
                       CommitWriteBuffer* writeBuffer,
                       const NLSystemContext* system)
    : _ctxt(view, sink, prog->getChunkSize(), writeBuffer, system),
    _prog(prog)
{
}

NLExecutor::~NLExecutor() {
}

void NLExecutor::run() {
    NLOutputSink* const sink = _ctxt.getSink();
    const std::span<const std::string_view> columnNames = _prog->columnNames();

    const bool hasNamesToPublish = sink && !columnNames.empty();
    if (hasNamesToPublish) {
        sink->setColumnNames(columnNames);
    }

    runBody(&_ctxt, _prog->getStmts());
}

void NLExecutor::runCreateNode(NLExecutionContext* context, NLFunctionData* data) {
    NLCreateNodeData* createData = static_cast<NLCreateNodeData*>(data);
    CommitWriteBuffer* writeBuffer = context->getWriteBuffer();
    bioassert(writeBuffer, "nl.create_node requires an active write transaction");

    const size_t rowCount = createData->getRowCount();
    const LabelSetHandle labelsetHandle = createData->getLabelSetHandle();

    // Must extract this value before adding in the loop
    const size_t numPendingNodes = writeBuffer->numPendingNodes();

    for (size_t row = 0; row < rowCount; row++) {
        CommitWriteBuffer::PendingNode& node = writeBuffer->newPendingNode();
        node.labelsetHandle = labelsetHandle;
    }

    CommitWriteBuffer::UntypedProperties propsBuffer;

    for (const NLCreateNodeData::Property& prop : createData->properties()) {
        extractColumnProperties(prop._values, rowCount, prop._propertyTypeID, propsBuffer);

        for (size_t row = 0; row < rowCount; row++) {
            CommitWriteBuffer::PendingNode& pendingNode =
                writeBuffer->getPendingNode(numPendingNodes + row);
            CommitWriteBuffer::UntypedProperties& properties = pendingNode.properties;
            properties.push_back(propsBuffer[row]);
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

    const GraphView* view = context->getView();
    const size_t firstPendingNodeID = committedNodeCount(view);

    // Must extract this value before adding in the loop
    const size_t numPendingEdges = writeBuffer->numPendingEdges();

    for (size_t row = 0; row < rowCount; row++) {
        const CommitWriteBuffer::ExistingOrPendingNode srcNode =
            resolveNode(src, row, srcIsPending, firstPendingNodeID);
        const CommitWriteBuffer::ExistingOrPendingNode tgtNode =
            resolveNode(tgt, row, tgtIsPending, firstPendingNodeID);

        CommitWriteBuffer::PendingEdge& edge = writeBuffer->newPendingEdge(srcNode, tgtNode);
        edge.edgeType = edgeTypeID;
    }

    CommitWriteBuffer::UntypedProperties propsBuffer;

    for (const NLCreateEdgeData::Property& prop : createData->properties()) {
        extractColumnProperties(prop._values, rowCount, prop._propertyTypeID, propsBuffer);

        for (size_t row = 0; row < rowCount; row++) {
            writeBuffer->getPendingEdge(numPendingEdges + row).properties.push_back(propsBuffer[row]);
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
    const PropertyTypeID propID = setData->getPropertyTypeID();
    const Column* nodeCol = setData->getValue();
    extractColumnProperties(nodeCol, rowCount, propID, propsBuffer);

    const auto& raw = nodes->getRaw();
    for (size_t row = 0; row < rowCount; row++) {
        writeBuffer->addNodeUpdate(raw[row], propsBuffer[row]);
    }
}

void NLExecutor::runSetEdgeProperty(NLExecutionContext* context, NLFunctionData* data) {
    NLSetEdgePropertyData* setData = static_cast<NLSetEdgePropertyData*>(data);
    CommitWriteBuffer* writeBuffer = context->getWriteBuffer();
    bioassert(writeBuffer, "nl.set_edge_property requires an active write transaction");

    const ColumnEdgeIDs* edges = setData->getInput();
    const size_t rowCount = edges->size();
    const PropertyTypeID propID = setData->getPropertyTypeID();
    const Column* edgeCol = setData->getValue();

    CommitWriteBuffer::UntypedProperties propsBuffer;
    extractColumnProperties(edgeCol, rowCount, propID, propsBuffer);

    const auto& raw = edges->getRaw();
    for (size_t row = 0; row < rowCount; row++) {
        writeBuffer->addEdgeUpdate(raw[row], propsBuffer[row]);
    }
}

void NLExecutor::runDeleteNode(NLExecutionContext* context, NLFunctionData* data) {
    NLDeleteNodeData* deleteData = static_cast<NLDeleteNodeData*>(data);
    CommitWriteBuffer* writeBuffer = context->getWriteBuffer();
    bioassert(writeBuffer, "nl.delete_node requires an active write transaction");

    const ColumnNodeIDs* nodes = deleteData->getInput();
    const GraphView* view = context->getView();

    if (deleteData->isDetaching()) {
        writeBuffer->addDeletedNodes(nodes->getRaw());
        writeBuffer->addHangingEdges(*view);
    } else {
        throwIfNodesHaveEdges(*view, nodes);
        writeBuffer->addDeletedNodes(nodes->getRaw());
    }
}

void NLExecutor::runDeleteEdge(NLExecutionContext* context, NLFunctionData* data) {
    NLDeleteEdgeData* deleteData = static_cast<NLDeleteEdgeData*>(data);
    CommitWriteBuffer* writeBuffer = context->getWriteBuffer();
    bioassert(writeBuffer, "nl.delete_edge requires an active write transaction");

    const ColumnEdgeIDs* edges = deleteData->getInput();
    writeBuffer->addDeletedEdges(edges->getRaw());
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

void NLExecutor::runConstScanNodesLoop(NLExecutionContext* context, NLFunctionData* data) {
    NLConstScanLoopData* loopData = static_cast<NLConstScanLoopData*>(data);
    const NLStmtContainer* loopBody = loopData->getStmts();
    ColumnNodeIDs* nodeIDs = loopData->getNodeIDs();
    const std::span<const NodeID> constNodeIDs = loopData->getConstNodeIDs();
    const size_t chunkSize = context->getChunkSize();
    const size_t totalCount = constNodeIDs.size();

    // A null limit leaves the loop unbounded, exactly as in runScanNodesLoop.
    const NLLimitState* limit = loopData->getLimit();

    // Emit the fixed node ID list one chunk at a time: each step copies the next
    // slice into the loop's node chunk and runs the body over it. The cursor is
    // local to this call, so a const scan nested in a cross product restarts from
    // the first ID on every outer step, the same way runScanNodesLoop opens a
    // fresh chunk writer each call.
    size_t cursor = 0;

    const auto runIteration = [&]() {
        const size_t remaining = totalCount - cursor;
        const size_t rows = std::min(chunkSize, remaining);

        std::vector<NodeID>& raw = nodeIDs->getRaw();
        raw.assign(constNodeIDs.begin() + cursor, constNodeIDs.begin() + cursor + rows);

        cursor += rows;

        runBody(context, loopBody);
    };

    if (limit) {
        while (cursor < totalCount && limit->getRemaining() > 0) {
            runIteration();
        }
    } else {
        while (cursor < totalCount) {
            runIteration();
        }
    }
}

void NLExecutor::runUnwindConstLoop(NLExecutionContext* context, NLFunctionData* data) {
    NLUnwindConstLoopData* loopData = static_cast<NLUnwindConstLoopData*>(data);
    const NLStmtContainer* loopBody = loopData->getStmts();
    const ListView list = loopData->getList();
    Column* output = loopData->getOutput();
    const bool heterogeneous = loopData->isHeterogeneous();
    const ValueType valueType = loopData->getValueType();
    const size_t chunkSize = context->getChunkSize();
    const size_t totalCount = list.size();

    // A null limit leaves the loop unbounded, exactly as in runConstScanNodesLoop.
    const NLLimitState* limit = loopData->getLimit();

    // Emit the fixed list one chunk at a time: each step fills the value chunk with the
    // next slice and runs the body over it. The cursor is local to this call, so an
    // unwind nested in a cross product restarts from the first element on every outer
    // step - the same way runConstScanNodesLoop reopens its slice each call.
    size_t cursor = 0;

    const auto runIteration = [&]() {
        const size_t remaining = totalCount - cursor;
        const size_t rows = std::min(chunkSize, remaining);

        if (heterogeneous) {
            fillListElementChunk(output, list, cursor, rows);
        } else {
            fillHomogeneousChunk(output, valueType, list, cursor, rows);
        }

        cursor += rows;

        runBody(context, loopBody);
    };

    if (limit) {
        while (cursor < totalCount && limit->getRemaining() > 0) {
            runIteration();
        }
    } else {
        while (cursor < totalCount) {
            runIteration();
        }
    }
}

void NLExecutor::runLoadCSVLoop(NLExecutionContext* context, NLFunctionData* data) {
    NLLoadCSVLoopData* loopData = static_cast<NLLoadCSVLoopData*>(data);
    const NLStmtContainer* loopBody = loopData->getStmts();
    ColumnStringTable* row = loopData->getRow();
    const size_t chunkSize = context->getChunkSize();

    // A null limit leaves the loop unbounded, exactly as in runConstScanNodesLoop.
    const NLLimitState* limit = loopData->getLimit();

    const fs::Path* const dataDir = context->getDataDir();
    if (!dataDir) {
        throw IRException("Reading a CSV file needs a data directory, which this session has not opened");
    }

    fs::Path path;
    NLSystemContext::resolveInDataDir(path, *dataDir, loopData->getPath());

    // The file's shape decides what the fields the query named resolve to: a header line
    // names them, and the first record fixes how many a record carries - which is also
    // the count a malformed record is caught against.
    CSVFileInfo fileInfo;
    CSVParser::peekFileStructure(path, loopData->hasHeaders(), fileInfo);

    // A file holding no record names no field, so there is nothing to resolve a position
    // or a header against - and no row to run the body over either
    if (fileInfo._fieldCount == 0) {
        return;
    }

    std::vector<size_t> fieldIndices;
    resolveCSVFieldIndices(*loopData, fileInfo, fieldIndices);

    const CSVErrorMode errorMode = loopData->skipOnError() ? CSVErrorMode::Skip : CSVErrorMode::Fail;
    CSVParser parser(path, loopData->hasHeaders(), errorMode, fileInfo._fieldCount);

    // Parse the file one chunk of records at a time: each step fills the field columns
    // with the next records and runs the body over them. The parser is local to this
    // call, so a load nested in a cross product reopens the file on every outer step -
    // the same way runScanNodesLoop opens a fresh chunk writer each call.
    bool exhausted = false;

    const auto runIteration = [&]() {
        const size_t rows = parser.readChunk(chunkSize, fieldIndices, row);

        if (rows == 0) {
            exhausted = true;
            return;
        }

        runBody(context, loopBody);
    };

    if (limit) {
        while (!exhausted && limit->getRemaining() > 0) {
            runIteration();
        }
    } else {
        while (!exhausted) {
            runIteration();
        }
    }
}

void NLExecutor::searchVectorIndex(NLExecutionContext* context, NLVectorSearchLoopData* loopData) {
    const std::string_view indexName = loopData->getIndexName();

    vec::VectorDatabase* const vectorDatabase = context->getVectorDatabase();
    if (!vectorDatabase) {
        throw IRException("A vector search needs a vector database, which this session has not opened");
    }

    vec::VecLibAccessor accessor = vectorDatabase->getLibrary(indexName);
    if (!accessor.isValid()) {
        throw IRException(fmt::format("Vector index '{}' not found", indexName));
    }

    // The index reads the query vector as a flat span of exactly its dimension, so a
    // shorter one would be read past its end rather than rejected further down.
    const vec::Dimension dimension = accessor.metadata()->_dimension;
    const std::span<const float> queryVector = loopData->getQueryVector();
    if (queryVector.size() != dimension) {
        throw IRException(fmt::format("Vector index '{}' holds vectors of dimension {}, but the "
                                      "query searches for one of dimension {}",
                                      indexName,
                                      dimension,
                                      queryVector.size()));
    }

    vec::VectorSearchQuery query(dimension);
    query.setVector(queryVector);
    query.setMaxResultCount(loopData->getNeighbourCount());

    vec::VectorSearchResult result;
    const vec::VectorResult<void> searched = accessor.search(&query, &result);
    if (!searched.has_value()) {
        throw IRException(fmt::format("Vector search in '{}' failed: {}",
                                      indexName,
                                      searched.error().fmtMessage()));
    }

    // The neighbours come back nearest first, the two spans row-aligned, so either one
    // measures the result.
    const std::span<const int64_t> ids = result.ids();
    const std::span<const float> distances = result.distances();

    // An ID naming no node is kept rather than dropped the way a const scan drops one: it
    // matches no edge and holds no property, so an index whose IDs are not node IDs still
    // reports every neighbour it found.
    std::vector<NodeID>& neighbourIDs = loopData->neighbourIDs();
    neighbourIDs.resize(ids.size());
    std::transform(ids.begin(),
                   ids.end(),
                   neighbourIDs.begin(),
                   [](int64_t id) { return NodeID {static_cast<uint64_t>(id)}; });

    std::vector<std::optional<types::Double::Primitive>>& neighbourScores = loopData->neighbourScores();
    neighbourScores.resize(distances.size());
    std::copy(distances.begin(), distances.end(), neighbourScores.begin());

    loopData->markSearched();
}

void NLExecutor::runVectorSearchLoop(NLExecutionContext* context, NLFunctionData* data) {
    NLVectorSearchLoopData* loopData = static_cast<NLVectorSearchLoopData*>(data);
    const NLStmtContainer* loopBody = loopData->getStmts();

    // A null limit leaves the loop unbounded, exactly as in runUnwindConstLoop. A spent
    // one emits nothing, so the neighbours it would report are never read: the index is
    // asked for them only once the budget can take a row.
    const NLLimitState* limit = loopData->getLimit();
    if (limit && limit->getRemaining() == 0) {
        return;
    }

    // The reader lock the accessor holds lives no longer than the search itself, so a
    // concurrent LOAD VECTOR into this index waits for the search rather than for the
    // whole query the neighbours feed.
    if (!loopData->hasSearched()) {
        searchVectorIndex(context, loopData);
    }

    const std::span<const NodeID> ids = loopData->neighbourIDs();
    const std::span<const std::optional<types::Double::Primitive>> scores = loopData->neighbourScores();

    using ScoreColumn = ColumnOptVector<types::Double::Primitive>;

    ColumnNodeIDs* const idColumn = loopData->getIDs();
    ScoreColumn* const scoreColumn = static_cast<ScoreColumn*>(loopData->getScores());

    const size_t chunkSize = context->getChunkSize();
    const size_t totalCount = ids.size();

    // Emit the neighbours one chunk at a time: each step fills the two chunks with the
    // next slice and runs the body over them. The cursor is local to this call, so a
    // search nested in a cross product walks the neighbours again on every outer step -
    // the way runScanNodesLoop reopens its chunk writer each call.
    size_t cursor = 0;

    const auto runIteration = [&]() {
        const size_t remaining = totalCount - cursor;
        const size_t rows = std::min(chunkSize, remaining);

        std::vector<NodeID>& rawIDs = idColumn->getRaw();
        rawIDs.resize(rows);
        std::copy(ids.begin() + cursor, ids.begin() + cursor + rows, rawIDs.begin());

        std::vector<std::optional<types::Double::Primitive>>& rawScores = scoreColumn->getRaw();
        rawScores.resize(rows);
        std::copy(scores.begin() + cursor, scores.begin() + cursor + rows, rawScores.begin());

        cursor += rows;

        runBody(context, loopBody);
    };

    if (limit) {
        while (cursor < totalCount && limit->getRemaining() > 0) {
            runIteration();
        }
    } else {
        while (cursor < totalCount) {
            runIteration();
        }
    }
}

void NLExecutor::runUnwindLoop(NLExecutionContext* context, NLFunctionData* data) {
    NLUnwindLoopData* loopData = static_cast<NLUnwindLoopData*>(data);
    const Column* source = loopData->getSource();
    const size_t sourceRows = source->size();

    const NLStmtContainer* loopBody = loopData->getStmts();
    const NLUnwindElementCountFunction elementCount = loopData->getElementCountFunc();
    const NLUnwindElementEmitFunction elementEmit = loopData->getElementEmitFunc();
    const size_t chunkSize = context->getChunkSize();

    // A null limit leaves the loop unbounded, exactly as in runUnwindConstLoop.
    const NLLimitState* limit = loopData->getLimit();

    ColumnVector<size_t>* rows = loopData->getRows();
    ColumnVector<size_t>* positions = loopData->getPositions();

    // Walk every (row, element) pair in row order. sourceRow / elementIndex is the cursor
    // into that flattened sequence; a cell contributing nothing - a null, an empty list -
    // is skipped, so it emits no row. The cursor is local to this call, so an unwind
    // nested in an outer loop restarts on every one of its steps.
    size_t sourceRow = 0;
    size_t elementIndex = 0;
    size_t rowElements = 0;

    const auto openNextRow = [&]() {
        while (sourceRow < sourceRows) {
            rowElements = elementCount(source, sourceRow);
            if (rowElements > 0) {
                return;
            }

            sourceRow++;
        }
    };

    openNextRow();

    const auto runIteration = [&]() {
        std::vector<size_t>& rowsRaw = rows->getRaw();
        std::vector<size_t>& positionsRaw = positions->getRaw();
        rowsRaw.clear();
        positionsRaw.clear();

        // Fill up to chunkSize rows, each the next (row, element) pair. Which element of
        // its cell a row took is the emit handler's to read, so a source without one -
        // whose cells are the elements already - has no position to record.
        while (rowsRaw.size() < chunkSize && sourceRow < sourceRows) {
            rowsRaw.push_back(sourceRow);

            if (elementEmit) {
                positionsRaw.push_back(elementIndex);
            }

            elementIndex++;

            if (elementIndex == rowElements) {
                elementIndex = 0;
                sourceRow++;
                openNextRow();
            }
        }

        // A source whose cells hold more than the element drains through its own emit;
        // any other holds the elements already and rides the carry set, gathered by the
        // source row like everything else in flight.
        if (elementEmit) {
            elementEmit(source, rows, positions, loopData->getElementOutput());
        }

        for (const NLCarriedColumn& carriedColumn : loopData->carriedColumns()) {
            const auto gatherFunc = carriedColumn.getGatherFunc();
            gatherFunc(carriedColumn.getInput(), rows, carriedColumn.getOutput());
        }

        runBody(context, loopBody);
    };

    if (limit) {
        while (sourceRow < sourceRows && limit->getRemaining() > 0) {
            runIteration();
        }
    } else {
        while (sourceRow < sourceRows) {
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

void NLExecutor::runGetEdgesLoop(NLExecutionContext* context, NLFunctionData* data) {
    NLEdgeLoopData* loopData = static_cast<NLEdgeLoopData*>(data);
    const ColumnNodeIDs* inputNodeIDs = loopData->getInput();

    if (inputNodeIDs->empty()) {
        return;
    }

    GetEdgesChunkWriter chunkWriter(*context->getView(), inputNodeIDs);
    chunkWriter.setIndices(loopData->getIndices());
    chunkWriter.setEdgeIDs(loopData->getEdgeIDs());
    chunkWriter.setEdgeTypes(loopData->getEdgeTypes());
    chunkWriter.setOtherIDs(loopData->getTargets());

    runEdgeLoopSteps(context, loopData, &chunkWriter, loopData->getSources());
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
        // Logical row count, assumes the columns that carry rows share a dimension. A
        // constant is not one of them: it holds a single value standing for every row of
        // the step, so a step that kept no row would emit it as a row of its own
        for (const Column* column : output->rowCountColumns()) {
            rowCount = std::max(rowCount, column->size());
        }
    }

    context->getSink()->appendChunks(cols, offset, rowCount);
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
    binary->getFn()(binary->getResult(), binary->getLhs(), binary->getRhs(), binary->getMemory());
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

NLUnaryFn NLExecutor::selectToNullable(ValueType valueType, const Column* operand, LocalMemory* memory, Column*& result) {
    switch (valueType) {
        case ValueType::Int64:
            return selectToNullableOf<types::Int64::Primitive>(operand, memory, result);
        break;

        case ValueType::UInt64:
            return selectToNullableOf<types::UInt64::Primitive>(operand, memory, result);
        break;

        case ValueType::Double:
            return selectToNullableOf<types::Double::Primitive>(operand, memory, result);
        break;

        case ValueType::Bool:
            return selectToNullableOf<types::Bool::Primitive>(operand, memory, result);
        break;

        case ValueType::String:
            return selectToNullableOf<types::String::Primitive>(operand, memory, result);
        break;

        default:
            throw IRException("Only a scalar value column can be read as a nullable value column");
        break;
    }

    return nullptr;
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

void NLExecutor::runUnaryFunction(NLExecutionContext* context, NLFunctionData* data) {
    const NLUnaryFunctionData* funcData = static_cast<NLUnaryFunctionData*>(data);
    funcData->getKernel()(context, funcData->getResult(), funcData->getInput());
}

template <typename Functor>
NLUnaryFunctionKernel NLExecutor::selectFunction(const Column* input, bool inputNullable, LocalMemory* memory, Column*& result) {
    using Res = typename Functor::ResultType;
    using JustRes = TypeUtils::unwrap_optional_t<Res>;

    // Early exit noop for NULL literals
    if (dynamic_cast<const ColumnConst<PropertyNull>*>(input)) {
        result = memory->alloc<ColumnConst<PropertyNull>>();
        return &functionNullKernel;
    }

    if (input->getContainerKind() == ContainerKind::code<ColumnConst>()) {
        result = memory->alloc<ColumnConst<Res>>();
        return &functionConstKernel<Functor>;
    }

    if (inputNullable) {
        result = memory->alloc<ColumnOptVector<JustRes>>();
        return &functionOptKernel<Functor>;
    }

    result = memory->alloc<ColumnVector<Res>>();
    return &functionVectorKernel<Functor>;
}

template NLUnaryFunctionKernel NLExecutor::selectFunction<LabelsFunction>(const Column* input, bool inputNullable, LocalMemory* memory, Column*& result);
template NLUnaryFunctionKernel NLExecutor::selectFunction<EdgeTypesFunction>(const Column* input, bool inputNullable, LocalMemory* memory, Column*& result);
template NLUnaryFunctionKernel NLExecutor::selectFunction<toIntegerFunction>(const Column* input, bool inputNullable, LocalMemory* memory, Column*& result);
template NLUnaryFunctionKernel NLExecutor::selectFunction<toFloatFunction>(const Column* input, bool inputNullable, LocalMemory* memory, Column*& result);
template NLUnaryFunctionKernel NLExecutor::selectFunction<toBoolFunction>(const Column* input, bool inputNullable, LocalMemory* memory, Column*& result);

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

    const NLLimitState* limit = loopData->getLimit();

    // Re-chunk the sorted rows: each step gathers the next chunkSize rows, in
    // permutation order, into the loop variables, then runs the body (nl.output).
    // The last chunk may be partial; an empty result runs the body zero times.
    const auto runIteration = [&](size_t offset) {
        const size_t stepRows = std::min(chunkSize, totalRows - offset);

        std::vector<size_t>& indicesRaw = indices->getRaw();
        indicesRaw.assign(permutation.begin() + offset, permutation.begin() + offset + stepRows);

        for (const NLCarriedColumn& column : loopData->columns()) {
            const NLGatherFunction gather = column.getGatherFunc();
            gather(column.getInput(), indices, column.getOutput());
        }

        runBody(context, loopBody);
    };

    if (limit) {
        for (size_t offset = 0; offset < totalRows && limit->getRemaining() > 0; offset += chunkSize) {
            runIteration(offset);
        }
    } else {
        for (size_t offset = 0; offset < totalRows; offset += chunkSize) {
            runIteration(offset);
        }
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

    // That alignment is this step's precondition, not a property of the op's types:
    // the group assignment is computed once per key row, then indexed by each fold as
    // it walks its own input, so a column of another length folds rows into the wrong
    // group - or, when it is the longer one, reads past the assignments entirely.
    // Generated IR places the update where every column is bound together, so a
    // mismatch here means malformed IR.
    for (const NLGroupAggregateState::KeyColumn& keyColumn : keyColumns) {
        bioassert(keyColumn._input->size() == rowCount,
                  "nl.group_aggregate_update grouping keys must be row-aligned");
    }

    for (const NLGroupAggregateState::Aggregate& aggregate : aggregates) {
        bioassert(aggregate._input->size() == rowCount,
                  "nl.group_aggregate_update aggregate inputs must be row-aligned with the grouping keys");
    }

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
        aggregate._fold(aggregate._accumulator,
                        aggregate._counts,
                        aggregate._input,
                        groupIndices,
                        aggregate._distinct);
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

    const NLLimitState* limit = loopData->getLimit();

    // Re-chunk the accumulated groups: each step materializes the next chunk of
    // group rows - the key values sliced from the buffers and each aggregate
    // finalized from the per-group state - into the loop variables, then runs the
    // body (the nl.output) per chunk. An empty result (no group) runs the body zero
    // times, so a grouped aggregate over no row emits nothing.
    const auto runIteration = [&](size_t offset) {
        const size_t stepGroups = std::min(chunkSize, totalGroups - offset);

        for (const NLGroupAggregateState::KeyColumn& keyColumn : keyColumns) {
            keyColumn._emitCopy(keyColumn._buffer, offset, stepGroups, keyColumn._output);
        }

        for (const NLGroupAggregateState::Aggregate& aggregate : aggregates) {
            aggregate._emit(aggregate._accumulator, aggregate._counts, offset, stepGroups, aggregate._output);
        }

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

void NLExecutor::runCollectReset(NLExecutionContext* context, NLFunctionData* data) {
    const NLCollectResetData* reset = static_cast<NLCollectResetData*>(data);
    reset->getState()->reset();
}

void NLExecutor::runCollectUpdate(NLExecutionContext* context, NLFunctionData* data) {
    NLCollectUpdateData* update = static_cast<NLCollectUpdateData*>(data);
    NLCollectState* state = update->getState();

    std::vector<NLCollectState::KeyColumn>& keyColumns = state->keyColumns();
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
    for (const NLCollectState::KeyColumn& keyColumn : keyColumns) {
        bioassert(keyColumn._input->size() == rowCount,
                  "nl.collect_update grouping keys must be row-aligned");
    }

    for (const NLCollectState::ValueColumn& valueColumn : valueColumns) {
        bioassert(valueColumn._input->size() == rowCount,
                  "nl.collect_update value columns must be row-aligned with the grouping keys");
    }

    std::vector<NLGroupAggregateState::Aggregate>& aggregates = state->aggregates();
    for (const NLGroupAggregateState::Aggregate& aggregate : aggregates) {
        bioassert(aggregate._input->size() == rowCount,
                  "nl.collect_update aggregate inputs must be row-aligned with the grouping keys");
    }

    if (rowCount == 0) {
        return;
    }

    // Assign each row to its group: serialize the grouping-key tuple, look it up, and
    // on first sight create a new group (its index is the next group count) and record
    // the row that created it, so the key buffers can take that row's values. With no
    // grouping key the tuple is empty, so every row falls into the single group 0.
    NLGroupTable& groupTable = state->groupTable();
    std::vector<size_t>& groupIndices = state->groupIndicesScratch();
    std::vector<size_t>& newGroupRows = state->newGroupRowsScratch();
    std::string& key = state->keyScratch();

    groupIndices.resize(rowCount);
    newGroupRows.clear();

    for (size_t row = 0; row < rowCount; row++) {
        key.clear();
        for (const NLCollectState::KeyColumn& keyColumn : keyColumns) {
            keyColumn._keyAppend(keyColumn._input, row, key);
        }

        const NLGroupTable::Assignment assignment = groupTable.assign(key);
        if (assignment._created) {
            newGroupRows.push_back(row);
        }

        groupIndices[row] = assignment._index;
    }

    const size_t groupCount = groupTable.getGroupCount();

    // Grow the key buffers with the new groups' key values, and the per-group position
    // lists to the new group count, then append this step's present values to their
    // groups' lists.
    for (NLCollectState::KeyColumn& keyColumn : keyColumns) {
        keyColumn._gatherAppend(keyColumn._input, newGroupRows, keyColumn._buffer);
    }

    for (NLCollectState::ValueColumn& valueColumn : valueColumns) {
        valueColumn._groupPositions.resize(groupCount);

        valueColumn._fold(valueColumn._buffer,
                          valueColumn._input,
                          groupIndices,
                          valueColumn._groupPositions,
                          valueColumn._distinct);
    }

    // The reductions taken over the same groups fold beside the list, off the group
    // assignment computed once above, exactly as nl.group_aggregate_update folds them.
    for (NLGroupAggregateState::Aggregate& aggregate : aggregates) {
        aggregate._grow(aggregate._accumulator, aggregate._counts, groupCount);
    }

    for (NLGroupAggregateState::Aggregate& aggregate : aggregates) {
        aggregate._fold(aggregate._accumulator,
                        aggregate._counts,
                        aggregate._input,
                        groupIndices,
                        aggregate._distinct);
    }
}

// Only the scalar value types are collectable for now; an embedding column (a span of
// floats per row) has no owned primitive to buffer, so it is rejected here rather than
// silently mishandled.
NLCollectFoldFunction NLExecutor::selectCollectFold(ValueType valueType) {
    switch (valueType) {
        case ValueType::Int64:
            return &collectFold<types::Int64::Primitive>;
        break;

        case ValueType::UInt64:
            return &collectFold<types::UInt64::Primitive>;
        break;

        case ValueType::Double:
            return &collectFold<types::Double::Primitive>;
        break;

        case ValueType::Bool:
            return &collectFold<types::Bool::Primitive>;
        break;

        case ValueType::String:
            return &collectFold<types::String::Primitive>;
        break;

        default:
            throw IRException("collect does not support this value type");
        break;
    }

    return nullptr;
}

// The collect(DISTINCT x) fold for a column of this value type: collect's own domain,
// with each of a group's values buffered once. An embedding has no bytes to key on, and
// no primitive to buffer either, so it is rejected for both reasons.
NLCollectFoldFunction NLExecutor::selectCollectDistinctFold(ValueType valueType) {
    switch (valueType) {
        case ValueType::Int64:
            return &collectFoldDistinct<types::Int64::Primitive>;
        break;

        case ValueType::UInt64:
            return &collectFoldDistinct<types::UInt64::Primitive>;
        break;

        case ValueType::Double:
            return &collectFoldDistinct<types::Double::Primitive>;
        break;

        case ValueType::Bool:
            return &collectFoldDistinct<types::Bool::Primitive>;
        break;

        case ValueType::String:
            return &collectFoldDistinct<types::String::Primitive>;
        break;

        default:
            throw IRException("collect does not support this value type");
        break;
    }

    return nullptr;
}

NLUnwindElementCountFunction NLExecutor::selectListUnwindElementCount() {
    return &unwindListElementCount;
}

NLUnwindElementCountFunction NLExecutor::selectOptUnwindElementCount(ValueType valueType) {
    NLUnwindElementCountFunction selected = nullptr;

    const auto select = [&]<SupportedType T>() {
        selected = &unwindOptElementCount<typename T::Primitive>;
    };
    ValueTypeDispatcher(valueType).execute(select);

    return selected;
}

NLUnwindElementCountFunction NLExecutor::selectValueUnwindElementCount() {
    return &unwindValueElementCount;
}

NLUnwindElementEmitFunction NLExecutor::selectListUnwindElementEmit() {
    return &unwindListElementEmit;
}

NLUnwindElementEmitFunction NLExecutor::selectListUnwindValueEmit(ValueType valueType) {
    NLUnwindElementEmitFunction selected = nullptr;

    const auto select = [&]<SupportedType T>() {
        selected = &unwindListValueEmit<typename T::Primitive>;
    };
    ValueTypeDispatcher(valueType).execute(select);

    return selected;
}

NLUnwindElementEmitFunction NLExecutor::selectListUnwindNodeEmit() {
    return &unwindListPlainEmit<NodeID>;
}

NLUnwindElementEmitFunction NLExecutor::selectListUnwindEdgeEmit() {
    return &unwindListPlainEmit<EdgeID>;
}

NLUnwindElementEmitFunction NLExecutor::selectListUnwindListEmit() {
    return &unwindListPlainEmit<ListView>;
}

NLUnwindElementCountFunction NLExecutor::selectTaggedUnwindElementCount() {
    return &unwindTaggedElementCount;
}

NLUnwindElementEmitFunction NLExecutor::selectTaggedUnwindElementEmit() {
    return &unwindTaggedElementEmit;
}

NLUnwindCollectValueEmitFunction NLExecutor::selectUnwindCollectValueEmit(ValueType valueType) {
    switch (valueType) {
        case ValueType::Int64:
            return &unwindCollectValueEmit<types::Int64::Primitive>;
        break;

        case ValueType::UInt64:
            return &unwindCollectValueEmit<types::UInt64::Primitive>;
        break;

        case ValueType::Double:
            return &unwindCollectValueEmit<types::Double::Primitive>;
        break;

        case ValueType::Bool:
            return &unwindCollectValueEmit<types::Bool::Primitive>;
        break;

        case ValueType::String:
            return &unwindCollectValueEmit<types::String::Primitive>;
        break;

        default:
            throw IRException("unwind does not support this value type");
        break;
    }

    return nullptr;
}

NLCollectListEmitFunction NLExecutor::selectCollectListEmit(ValueType valueType) {
    switch (valueType) {
        case ValueType::Int64:
            return &collectListEmit<types::Int64::Primitive>;
        break;

        case ValueType::UInt64:
            return &collectListEmit<types::UInt64::Primitive>;
        break;

        case ValueType::Double:
            return &collectListEmit<types::Double::Primitive>;
        break;

        case ValueType::Bool:
            return &collectListEmit<types::Bool::Primitive>;
        break;

        case ValueType::String:
            return &collectListEmit<types::String::Primitive>;
        break;

        default:
            throw IRException("collect does not support this value type");
        break;
    }

    return nullptr;
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

// A list cell is present in every row, so it folds the way an entity ID does; only the
// dedup differs, keying on the elements rather than on the cell.
void NLExecutor::selectCollectListHandlers(bool distinctValues,
                                           NLCollectFoldFunction& fold,
                                           NLCollectListEmitFunction& listEmit) {
    fold = distinctValues ? &collectListFoldDistinct : &collectEntityFold<ListView>;
    listEmit = &collectListEmit<ListView>;
}

// A type-erased cell carries its own type, so the fold drops the ones tagged null and the
// emit writes each survivor back under the type its tag names.
void NLExecutor::selectCollectTaggedHandlers(bool distinctValues,
                                             NLCollectFoldFunction& fold,
                                             NLCollectListEmitFunction& listEmit) {
    fold = distinctValues ? &collectTaggedFoldDistinct : &collectTaggedFold;
    listEmit = &collectTaggedListEmit;
}

void NLExecutor::runUnwindCollectLoop(NLExecutionContext* context, NLFunctionData* data) {
    NLUnwindCollectLoopData* loopData = static_cast<NLUnwindCollectLoopData*>(data);
    NLCollectState* state = loopData->getState();

    const size_t chunkSize = context->getChunkSize();
    const NLStmtContainer* loopBody = loopData->getStmts();

    std::vector<NLCollectState::KeyColumn>& keyColumns = state->keyColumns();
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
        for (const NLCollectState::KeyColumn& keyColumn : keyColumns) {
            keyColumn._gather(keyColumn._buffer, groupIndices, keyColumn._output);
        }

        unwound._unwindCollectEmit(unwound._buffer, positions, unwound._output);

        runBody(context, loopBody);
    }
}

void NLExecutor::runCollectLoop(NLExecutionContext* context, NLFunctionData* data) {
    NLCollectLoopData* loopData = static_cast<NLCollectLoopData*>(data);
    NLCollectState* state = loopData->getState();

    const size_t totalGroups = state->groupTable().getGroupCount();
    const size_t chunkSize = context->getChunkSize();
    const NLStmtContainer* loopBody = loopData->getStmts();

    std::vector<NLCollectState::KeyColumn>& keyColumns = state->keyColumns();

    // Re-chunk the groups: each step slices the next chunk of key values from the key
    // buffers and materializes one list cell per group (a ListView over that group's
    // run in the list buffer), then runs the body. A grouped collect over no row has no
    // group, so the body runs zero times and it emits nothing; an ungrouped collect
    // always carries the single group the reset created, so it emits exactly one row -
    // holding the empty list when no value was folded.
    for (size_t offset = 0; offset < totalGroups; offset += chunkSize) {
        const size_t stepGroups = std::min(chunkSize, totalGroups - offset);

        for (const NLCollectState::KeyColumn& keyColumn : keyColumns) {
            keyColumn._emitCopy(keyColumn._buffer, offset, stepGroups, keyColumn._output);
        }

        for (NLCollectState::ValueColumn& valueColumn : state->valueColumns()) {
            valueColumn._listEmit(valueColumn._buffer,
                                  valueColumn._groupPositions,
                                  offset,
                                  stepGroups,
                                  state->listBuffer(),
                                  valueColumn._output);
        }

        for (const NLGroupAggregateState::Aggregate& aggregate : state->aggregates()) {
            aggregate._emit(aggregate._accumulator, aggregate._counts, offset, stepGroups, aggregate._output);
        }

        runBody(context, loopBody);
    }
}

void NLExecutor::runProcedureInitLoop(NLExecutionContext* context, NLFunctionData* data) {
    NLProcedureLoopData* loopData = static_cast<NLProcedureLoopData*>(data);
    NLProcedureState* state = loopData->getState();

    // This loop drives the procedure over one chunk of its arguments and is re-entered
    // for the next chunk, so rewind it here: a procedure that finished the previous
    // chunk starts afresh on this one, and its own per-drive state goes with it.
    state->prepareOrResetForNewDrive();

    runProcedureDrive(context, loopData, [state]() { state->execute(); });
}

NLGatherFunction NLExecutor::selectGatherFunction(NLChunkKind kind) {
    NLGatherFunction selected = nullptr;
    dispatchChunkKind(kind, [&]<typename ElementType>() { selected = &gatherColumn<ElementType>; });

    return selected;
}

NLGatherFunction NLExecutor::selectCountGatherFunction() {
    return &gatherColumn<uint64_t>;
}

NLMaskSurvivorFunction NLExecutor::selectMaskSurvivorFunction(bool nullable) {
    if (nullable) {
        return &collectOptMaskSurvivors;
    }

    return &collectMaskSurvivors;
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

NLBroadcastConstantFunction NLExecutor::selectNullConstantBroadcast() {
    return &broadcastNullColumn;
}

NLBroadcastConstantFunction NLExecutor::selectConstantListBroadcast() {
    return &broadcastConstantListColumn;
}

NLBroadcastConstantFunction NLExecutor::selectConstantBroadcast(ValueType valueType) {
    NLBroadcastConstantFunction fill = nullptr;
    const auto select = [&]<SupportedType T>() {
        fill = &broadcastConstantColumn<typename T::Primitive>;
    };
    ValueTypeDispatcher(valueType).execute(select);

    return fill;
}

// A list_element chunk is a ColumnVector<ListElementView> of fixed-width tagged
// scalars, so the same broadcast templates carry the tag along with the value.
NLBroadcastFunction NLExecutor::selectListElementBlockRepeatFunction() {
    return &blockRepeatColumn<ListElementView>;
}

NLBroadcastFunction NLExecutor::selectListElementTileFunction() {
    return &tileColumn<ListElementView>;
}

NLAppendFunction NLExecutor::selectListElementAppendFunction() {
    return &appendColumn<ListElementView>;
}

NLGatherFunction NLExecutor::selectListElementGatherFunction() {
    return &gatherColumn<ListElementView>;
}

NLCompareFunction NLExecutor::selectListElementCompareFunction() {
    return &compareListElementColumn;
}

NLKeyAppendFunction NLExecutor::selectListElementKeyAppendFunction() {
    return &distinctKeyAppendListElementColumn;
}

NLCountFunction NLExecutor::selectListElementCountFunction() {
    return &countNonNullElementsColumn;
}

NLGroupKeyGatherFunction NLExecutor::selectListElementGroupKeyGatherFunction() {
    return &groupGatherAppendColumn<ListElementView>;
}

NLCopyFunction NLExecutor::selectListElementCopyFunction() {
    return &copyRangeColumn<ListElementView>;
}

NLBroadcastFunction NLExecutor::selectListBlockRepeatFunction() {
    return &blockRepeatColumn<ListView>;
}

NLCompareFunction NLExecutor::selectListCompareFunction() {
    return &compareListColumn;
}

NLCopyFunction NLExecutor::selectListCopyFunction() {
    return &copyRangeColumn<ListView>;
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
    NLAppendFunction selected = nullptr;
    dispatchChunkKind(kind, [&]<typename ElementType>() { selected = &appendColumn<ElementType>; });

    return selected;
}

NLAppendFunction NLExecutor::selectCountAppendFunction() {
    return &appendColumn<uint64_t>;
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

        case NLChunkKind::OwnedString:
            return &distinctKeyAppendPlainColumn<std::string>;
        break;

        case NLChunkKind::List:
            return &distinctKeyAppendListColumn;
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

// The reduction of a type-erased column: only sum and avg are defined over one, both
// into the f64 accumulator mixed numeric tags reduce to. min/max would have to hand back
// the winning cell in its own type, which no static result type names.
NLAggregateUpdateFunction NLExecutor::selectTaggedAggregateUpdate(AggregateKind kind) {
    switch (kind) {
        case AggregateKind::Sum:
            return &aggregateUpdateNumericTagged</*CountsRows=*/false>;
        break;

        case AggregateKind::Avg:
            return &aggregateUpdateNumericTagged</*CountsRows=*/true>;
        break;

        case AggregateKind::Min:
        case AggregateKind::Max:
            throw IRException("min/max over type-erased cells is not supported");
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
        case GroupAggregateKind::CountDistinct:
        case GroupAggregateKind::CountRows:
            return &groupGrowCount;
        break;

        case GroupAggregateKind::Avg:
        case GroupAggregateKind::AvgDistinct:
            return &groupGrowAvg;
        break;

        case GroupAggregateKind::Sum:
        case GroupAggregateKind::SumDistinct:
        case GroupAggregateKind::Min:
        case GroupAggregateKind::Max: {
            // sum grows each new group to a present zero (its additive identity),
            // min/max to null (no extreme seen yet); neither carries a per-group
            // tally. Both compile for any value type, and lowering has already
            // validated the kind / type pairing (and the fold selector re-checks it),
            // so one dispatch over the accumulator type suffices - the grouped sibling
            // of selectAggregateReset.
            const bool growsToZero = (kind == GroupAggregateKind::Sum) || (kind == GroupAggregateKind::SumDistinct);

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

// The grouped reduction of a type-erased column: sum and avg only, both into the f64
// accumulator mixed numeric tags reduce to - the grouped sibling of
// selectTaggedAggregateUpdate. A switch (not a default) over every kind so a new one is a
// compile error here rather than being reported as an unsupported min/max.
NLGroupAggregateFoldFunction NLExecutor::selectTaggedGroupAggregateFold(GroupAggregateKind kind) {
    switch (kind) {
        case GroupAggregateKind::Sum:
            return &groupFoldNumericTagged</*CountsRows=*/false, /*Distinct=*/false>;
        break;

        case GroupAggregateKind::SumDistinct:
            return &groupFoldNumericTagged</*CountsRows=*/false, /*Distinct=*/true>;
        break;

        case GroupAggregateKind::Avg:
            return &groupFoldNumericTagged</*CountsRows=*/true, /*Distinct=*/false>;
        break;

        case GroupAggregateKind::AvgDistinct:
            return &groupFoldNumericTagged</*CountsRows=*/true, /*Distinct=*/true>;
        break;

        case GroupAggregateKind::Min:
        case GroupAggregateKind::Max:
            throw IRException("min/max over type-erased cells is not supported");
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

NLGroupAggregateFoldFunction NLExecutor::selectGroupAggregateFold(GroupAggregateKind kind, ValueType inputType) {
    switch (kind) {
        case GroupAggregateKind::CountRows:
            return selectGroupCountAllFold();
        break;

        case GroupAggregateKind::CountDistinct:
            return selectGroupCountDistinctFold(inputType);
        break;

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

        case GroupAggregateKind::SumDistinct:
            return selectGroupSumDistinctFold(inputType);
        break;

        case GroupAggregateKind::Avg:
            return selectGroupAvgFold(inputType);
        break;

        case GroupAggregateKind::AvgDistinct:
            return selectGroupAvgDistinctFold(inputType);
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

NLGroupAggregateFoldFunction NLExecutor::selectGroupCountListElementFold() {
    return &groupFoldCountPresentListElement;
}

NLGroupAggregateFoldFunction NLExecutor::selectGroupCountDistinctListElementFold() {
    return &groupFoldCountDistinctListElement;
}

NLGroupAggregateFoldFunction NLExecutor::selectGroupCountAllFold() {
    // count(*) over an ID chunk (never null): every row charges its group.
    return &groupFoldCountAll;
}

// Selected per column from its value type. A manual switch, not ValueTypeDispatcher,
// because a distinct tally keys on the value's bytes and an embedding has none to key
// on - the same reason selectOptKeyAppendFunction is written out by hand.
NLGroupAggregateFoldFunction NLExecutor::selectGroupCountDistinctFold(ValueType inputType) {
    switch (inputType) {
        case ValueType::Int64:
            return &groupFoldCountDistinctPresent<types::Int64::Primitive>;
        break;

        case ValueType::UInt64:
            return &groupFoldCountDistinctPresent<types::UInt64::Primitive>;
        break;

        case ValueType::Double:
            return &groupFoldCountDistinctPresent<types::Double::Primitive>;
        break;

        case ValueType::Bool:
            return &groupFoldCountDistinctPresent<types::Bool::Primitive>;
        break;

        case ValueType::String:
            return &groupFoldCountDistinctPresent<types::String::Primitive>;
        break;

        case ValueType::Embedding:
            throw IRException("cannot count the distinct values of an embedding column");
        break;

        case ValueType::Invalid:
        case ValueType::_SIZE:
            throw IRException("invalid count(DISTINCT) value type");
        break;
    }

    bioassert(false, "Unhandled value type");
    return nullptr;
}

// count(DISTINCT x) over a chunk that is never null - an ID column, or one a procedure
// yielded: each group is charged once per distinct value it sees. The chunk kind picks
// the column layout the key is read from, the way selectKeyAppendFunction does for a
// DISTINCT row key. An ID keys on its integer, everything else on the value itself.
NLGroupAggregateFoldFunction NLExecutor::selectGroupCountDistinctChunkFold(NLChunkKind kind) {
    switch (kind) {
        case NLChunkKind::NodeID:
            return &groupFoldCountDistinctID<NodeID>;
        break;

        case NLChunkKind::EdgeID:
            return &groupFoldCountDistinctID<EdgeID>;
        break;

        case NLChunkKind::EdgeTypeID:
            return &groupFoldCountDistinctID<EdgeTypeID>;
        break;

        case NLChunkKind::LabelID:
            return &groupFoldCountDistinctID<LabelID>;
        break;

        case NLChunkKind::PropertyTypeID:
            return &groupFoldCountDistinctID<PropertyTypeID>;
        break;

        case NLChunkKind::ValueTypeCode:
            return &groupFoldCountDistinctValue<ValueType>;
        break;

        case NLChunkKind::UInt64:
            return &groupFoldCountDistinctValue<types::UInt64::Primitive>;
        break;

        case NLChunkKind::Int64:
            return &groupFoldCountDistinctValue<types::Int64::Primitive>;
        break;

        case NLChunkKind::Double:
            return &groupFoldCountDistinctValue<types::Double::Primitive>;
        break;

        case NLChunkKind::Bool:
            return &groupFoldCountDistinctValue<types::Bool::Primitive>;
        break;

        case NLChunkKind::String:
            return &groupFoldCountDistinctValue<types::String::Primitive>;
        break;

        case NLChunkKind::OwnedString:
            return &groupFoldCountDistinctValue<std::string>;
        break;

        case NLChunkKind::List:
            throw IRException("count(DISTINCT) cannot key on a list column: a list has no scalar value to count distinct");
        break;
    }

    bioassert(false, "Unknown NLChunkKind");
    return nullptr;
}

NLGroupAggregateEmitFunction NLExecutor::selectGroupAggregateEmit(GroupAggregateKind kind, ValueType resultType) {
    // A switch (not an if/else) over every kind so a new one is a compile error here
    // rather than silently taking the sum/min/max path.
    switch (kind) {
        case GroupAggregateKind::Count:
        case GroupAggregateKind::CountDistinct:
        case GroupAggregateKind::CountRows:
            // count emits its per-group tally as an unsigned i64, whatever the input was.
            return &groupEmitCount;
        break;

        case GroupAggregateKind::Avg:
        case GroupAggregateKind::AvgDistinct:
            // avg emits the running f64 sum divided by the count, per group.
            return &groupEmitAvg;
        break;

        case GroupAggregateKind::Sum:
        case GroupAggregateKind::SumDistinct:
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
    NLGroupKeyGatherFunction selected = nullptr;
    dispatchChunkKind(kind, [&]<typename ElementType>() { selected = &groupGatherAppendColumn<ElementType>; });

    return selected;
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

NLAppendFunction NLExecutor::selectPlainAppendFunction(ValueType valueType) {
    switch (valueType) {
        case ValueType::Int64:
            return &appendColumn<types::Int64::Primitive>;
        break;

        case ValueType::UInt64:
            return &appendColumn<types::UInt64::Primitive>;
        break;

        case ValueType::Double:
            return &appendColumn<types::Double::Primitive>;
        break;

        default:
            throw IRException("a plain value column must be numeric");
        break;
    }
}

NLGatherFunction NLExecutor::selectPlainGatherFunction(ValueType valueType) {
    switch (valueType) {
        case ValueType::Int64:
            return &gatherColumn<types::Int64::Primitive>;
        break;

        case ValueType::UInt64:
            return &gatherColumn<types::UInt64::Primitive>;
        break;

        case ValueType::Double:
            return &gatherColumn<types::Double::Primitive>;
        break;

        default:
            throw IRException("a plain value column must be numeric");
        break;
    }
}

NLCompareFunction NLExecutor::selectPlainCompareFunction(ValueType valueType) {
    switch (valueType) {
        case ValueType::Int64:
            return &compareColumn<types::Int64::Primitive>;
        break;

        case ValueType::UInt64:
            return &compareColumn<types::UInt64::Primitive>;
        break;

        case ValueType::Double:
            return &compareColumn<types::Double::Primitive>;
        break;

        default:
            throw IRException("a plain value column must be numeric");
        break;
    }
}

NLCopyFunction NLExecutor::selectPlainCopyFunction(ValueType valueType) {
    switch (valueType) {
        case ValueType::Int64:
            return &copyRangeColumn<types::Int64::Primitive>;
        break;

        case ValueType::UInt64:
            return &copyRangeColumn<types::UInt64::Primitive>;
        break;

        case ValueType::Double:
            return &copyRangeColumn<types::Double::Primitive>;
        break;

        default:
            throw IRException("a plain value column must be numeric");
        break;
    }
}

NLKeyAppendFunction NLExecutor::selectPlainKeyAppendFunction(ValueType valueType) {
    switch (valueType) {
        case ValueType::Int64:
            return &distinctKeyAppendPlainColumn<types::Int64::Primitive>;
        break;

        case ValueType::UInt64:
            return &distinctKeyAppendPlainColumn<types::UInt64::Primitive>;
        break;

        case ValueType::Double:
            return &distinctKeyAppendPlainColumn<types::Double::Primitive>;
        break;

        default:
            throw IRException("a plain value column must be numeric");
        break;
    }
}

NLGroupKeyGatherFunction NLExecutor::selectPlainGroupKeyGather(ValueType valueType) {
    switch (valueType) {
        case ValueType::Int64:
            return &groupGatherAppendColumn<types::Int64::Primitive>;
        break;

        case ValueType::UInt64:
            return &groupGatherAppendColumn<types::UInt64::Primitive>;
        break;

        case ValueType::Double:
            return &groupGatherAppendColumn<types::Double::Primitive>;
        break;

        default:
            throw IRException("a plain value column must be numeric");
        break;
    }
}

NLBroadcastFunction NLExecutor::selectPlainBlockRepeatFunction(ValueType valueType) {
    switch (valueType) {
        case ValueType::Int64:
            return &blockRepeatColumn<types::Int64::Primitive>;
        break;

        case ValueType::UInt64:
            return &blockRepeatColumn<types::UInt64::Primitive>;
        break;

        case ValueType::Double:
            return &blockRepeatColumn<types::Double::Primitive>;
        break;

        default:
            throw IRException("a plain value column must be numeric");
        break;
    }
}

NLBroadcastFunction NLExecutor::selectPlainTileFunction(ValueType valueType) {
    switch (valueType) {
        case ValueType::Int64:
            return &tileColumn<types::Int64::Primitive>;
        break;

        case ValueType::UInt64:
            return &tileColumn<types::UInt64::Primitive>;
        break;

        case ValueType::Double:
            return &tileColumn<types::Double::Primitive>;
        break;

        default:
            throw IRException("a plain value column must be numeric");
        break;
    }
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

        case NLChunkKind::OwnedString:
            return &compareColumn<std::string>;
        break;

        case NLChunkKind::List:
            throw IRException("A list column cannot be a sort key: a list has no order here");
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

void NLExecutor::runGetNodeLabelSet(NLExecutionContext* context, NLFunctionData* data) {
    NLGetNodeLabelSetData* fetchData = static_cast<NLGetNodeLabelSetData*>(data);

    const GraphView& view = *context->getView();
    const ColumnNodeIDs* input = fetchData->getInput();
    ColumnLabelSetIDs* output = fetchData->getOutput();

    GetNodeLabelSetChunkWriter writer(view, input);
    writer.setLabelSetIDs(output);
    writer.fill(input->size());
}

void NLExecutor::runCheckLabelConstraint(NLExecutionContext* context, NLFunctionData* data) {
    NLCheckLabelConstraintData* checkData = static_cast<NLCheckLabelConstraintData*>(data);

    const ColumnLabelSetIDs* input = checkData->getInput();
    ColumnMask* output = checkData->getOutput();

    const size_t rowCount = input->size();
    output->resize(rowCount);

    for (size_t rowIndex = 0; rowIndex < rowCount; rowIndex++) {
        const LabelSetID id = (*input)[rowIndex];
        (*output)[rowIndex] = checkData->isMatching(id);
    }
}

void NLExecutor::runCheckEdgeTypeConstraint(NLExecutionContext* context, NLFunctionData* data) {
    NLCheckEdgeTypeConstraintData* checkData = static_cast<NLCheckEdgeTypeConstraintData*>(data);

    const ColumnEdgeTypes* input = checkData->getInput();
    ColumnMask* output = checkData->getOutput();

    const size_t rowCount = input->size();
    output->resize(rowCount);

    for (size_t rowIndex = 0; rowIndex < rowCount; rowIndex++) {
        const EdgeTypeID id = (*input)[rowIndex];
        (*output)[rowIndex] = checkData->isMatching(id);
    }
}

template NLBinaryFn NLExecutor::selectBinary<OP_ADD>(const Column* lhs, const Column* rhs, LocalMemory* memory, Column*& result);
template NLBinaryFn NLExecutor::selectBinary<OP_CONCAT>(const Column* lhs, const Column* rhs, LocalMemory* memory, Column*& result);
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
template NLBinaryFn NLExecutor::selectBinary<OP_FUNC_COSINE_SIMILARITY>(const Column* lhs, const Column* rhs, LocalMemory* memory, Column*& result);
template NLBinaryFn NLExecutor::selectBinary<OP_FUNC_EUCLIDEAN_DISTANCE>(const Column* lhs, const Column* rhs, LocalMemory* memory, Column*& result);

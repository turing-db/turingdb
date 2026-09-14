#pragma once

#include <math.h>

#include <limits>
#include <optional>
#include <string>
#include <system_error>
#include <type_traits>
#include <vector>

#include "columns/ColumnConst.h"
#include "columns/ColumnIDs.h"
#include "columns/ColumnVector.h"
#include "TypeUtils.h"
#include "list/ListElementView.h"

#include "list/ListBuffer.h"

#include "metadata/LabelSetHandle.h"
#include "metadata/PropertyType.h"

#include "views/GraphView.h"

#include "ID.h"

#include "TypeUtils.h"

#include "BioAssert.h"

namespace db {

class CommitWriteBuffer;

/**
 * @brief Generic function to apply a generic invokable to two possibly-optional
 * operands, where either operand being nullopt results in the final result being
 * nullopt, and the result of applying the invokable otherwise.
 */
template <typename Func, typename T, typename U>
    requires OptionallyInvokable<Func, T, U>
auto optionalGenericFunc(T&& a, U&& b) -> TypeUtils::optional_invoke_result<Func, T, U> {
    if constexpr (TypeUtils::is_optional_v<T>) {
        if (!a.has_value()) {
            return std::nullopt;
        }
    }

    if constexpr (TypeUtils::is_optional_v<U>) {
        if (!b.has_value()) {
            return std::nullopt;
        }
    }

    // a and b are both either engaged optionals or values, so safe to unwrap

    auto&& av = TypeUtils::unwrap(a);
    auto&& bv = TypeUtils::unwrap(b);

    return Func {}(av, bv);
}

class LabelsFunction {
public:
    using ArgType = NodeID;
    using ResultType = ListView;

    LabelsFunction(GraphView view, QueryListBuffer* listBuffer);

    // The graph holds none of a change's writes until they commit, so the labels of a
    // node this one wrote are read out of @param writeBuffer
    LabelsFunction(GraphView view,
                   QueryListBuffer* listBuffer,
                   const CommitWriteBuffer* writeBuffer);

    ResultType operator()(NodeID node);

private:
    GraphView _view;
    QueryListBuffer* _listBuffer {nullptr};
    const CommitWriteBuffer* _writeBuffer {nullptr};
    size_t _firstPendingNodeID {0};
    std::vector<LabelID> _labels;
    std::vector<QueryListBuffer::ListItemVariant> _elements;

    bool isPendingNode(NodeID node) const;
    LabelSetHandle readLabelSet(NodeID node) const;
};

class EdgeTypesFunction {
public:
    using ArgType = EdgeID;
    using ResultType = std::string;

    explicit EdgeTypesFunction(GraphView view);
    EdgeTypesFunction(GraphView view, const CommitWriteBuffer* writeBuffer);

    ResultType operator()(const EdgeID edge) {
        getEdgeTypeString(_tmp, edge);
        return _tmp;
    }

private:
    GraphView _view;
    const CommitWriteBuffer* _writeBuffer {nullptr};
    size_t _firstPendingEdgeID {0};
    std::string _tmp;

    void getEdgeTypeString(std::string& out, EdgeID edge);
    EdgeTypeID readEdgeType(EdgeID edge) const;
};

// The ends of an edge, read as the graph stores it: the start is the node the edge leaves
// and the end the node it reaches, whichever way the pattern walked it. An invalid edge -
// what an OPTIONAL MATCH leaves where it matched nothing - has no end, so the answer is the
// invalid node ID a node column carries its null as.
class EdgeEndsFunction {
public:
    using ArgType = EdgeID;
    using ResultType = NodeID;

    explicit EdgeEndsFunction(GraphView view);

    // The graph holds none of a change's writes until they commit, so the ends of an edge
    // this one wrote are read out of @param writeBuffer
    EdgeEndsFunction(GraphView view, const CommitWriteBuffer* writeBuffer);

protected:
    NodeID getStartNode(EdgeID edge) const;
    NodeID getEndNode(EdgeID edge) const;

private:
    GraphView _view;
    const CommitWriteBuffer* _writeBuffer {nullptr};
    size_t _firstPendingNodeID {0};
    size_t _firstPendingEdgeID {0};

    void readEnds(EdgeID edge, NodeID& start, NodeID& end) const;
    void readPendingEnds(EdgeID edge, NodeID& start, NodeID& end) const;
};

// The same two over a type-erased cell, which is what an UNWIND or an index of a stored
// list binds: a stored list names no element type, so what its elements are is known per
// row rather than in the plan. A cell holding a null answers the invalid node such a
// column carries; one holding no edge at all is the type error the row raises.
class TaggedStartNodeFunction : public EdgeEndsFunction {
public:
    using EdgeEndsFunction::EdgeEndsFunction;
    using ArgType = ListElementView;

    ResultType operator()(ArgType cell) const;
    ResultType operator()(const std::optional<ArgType>& cell) const;
};

class TaggedEndNodeFunction : public EdgeEndsFunction {
public:
    using EdgeEndsFunction::EdgeEndsFunction;
    using ArgType = ListElementView;

    ResultType operator()(ArgType cell) const;
    ResultType operator()(const std::optional<ArgType>& cell) const;
};

class StartNodeFunction : public EdgeEndsFunction {
public:
    using EdgeEndsFunction::EdgeEndsFunction;
    using TaggedCounterpart = TaggedStartNodeFunction;

    ResultType operator()(const EdgeID edge) const { return getStartNode(edge); }
};

class EndNodeFunction : public EdgeEndsFunction {
public:
    using EdgeEndsFunction::EdgeEndsFunction;
    using TaggedCounterpart = TaggedEndNodeFunction;

    ResultType operator()(const EdgeID edge) const { return getEndNode(edge); }
};

// The ID a type-erased cell's entity is named by. id() over a node or an edge is the
// column it was handed, but a cell holds its entity behind a tag, so the number has to be
// read out of it before anything can be compared against it.
class TaggedIdFunction {
public:
    using ArgType = ListElementView;
    using ResultType = std::optional<types::Int64::Primitive>;

    ResultType operator()(ArgType cell) const;
};

class toIntegerFunction {
public:
    using ArgType = types::String::Primitive;
    using ResultType = std::optional<types::Int64::Primitive>;

    ResultType operator()(std::string_view sv) {
        types::Int64::Primitive result = 0;

        const auto* start = sv.data();
        const auto* end = sv.data() + sv.size();

        auto [ptr, ec] = std::from_chars(start, end, result);

        const bool success = ec == std::errc {};
        const bool parsedAll = ptr == end;

        if (!success || !parsedAll) {
            return std::nullopt;
        }

        return result;
    }
};

// NOTE: macOS wheel build libc++ doesn't support from_chars on double: use strtod instead
class toFloatFunction {
public:
    using ArgType = types::String::Primitive;
    using ResultType = std::optional<types::Double::Primitive>;

    ResultType operator()(std::string_view sv) {
        // @ref std::strtod requires a null-terminated std::string: convert the string_view
        _buf.assign(sv);
        char* end = nullptr;
        errno = 0;

        const types::Double::Primitive result = std::strtod(_buf.data(), &end);

        // Either empty string, or could not parse all of the string as double
        const bool couldNotConvert = end == _buf.data() || end != _buf.data() + _buf.size();
        if (couldNotConvert) {
            return std::nullopt;
        }

        const bool outOfRange = errno == ERANGE;
        if (outOfRange) {
            return std::nullopt;
        }

        return result;
    }

private:
    std::string _buf; // Temporary buffer to null-terminate string view
};

class toBoolFunction {
public:
    using ArgType = types::String::Primitive;
    using ResultType = std::optional<types::Bool::Primitive>;

    ResultType operator()(std::string_view sv) {
        strToLower(_buf, sv);

        if (_buf == "true") {
            return true;
        }
        if (_buf == "false") {
            return false;
        }

        return std::nullopt;
    }

private:
    std::string _buf; // Preallocated buffer to reuse over iterations

    static void strToLower(std::string& lower, std::string_view src);
};

// toInteger() and toFloat() over a number rather than a string. Every conversion keeps an
// optional result so one column type carries it whatever the argument was, and so a double
// that no integer can represent reads as null.
template <typename Number>
class toIntegerFromNumberFunction {
public:
    using ArgType = Number;
    using ResultType = std::optional<types::Int64::Primitive>;

    ResultType operator()(const Number value) {
        if constexpr (std::is_floating_point_v<Number>) {
            const types::Double::Primitive truncated = trunc(static_cast<types::Double::Primitive>(value));

            // The smallest int64 converts to a double exactly; the largest does not, so the
            // upper bound is the power of two just past it and the comparison excludes it
            const types::Double::Primitive lowest
                = static_cast<types::Double::Primitive>(std::numeric_limits<types::Int64::Primitive>::min());
            const types::Double::Primitive pastHighest = -lowest;

            const bool representable = truncated >= lowest && truncated < pastHighest;
            if (!representable) {
                return std::nullopt;
            }

            return static_cast<types::Int64::Primitive>(truncated);
        } else {
            return static_cast<types::Int64::Primitive>(value);
        }
    }
};

template <typename Number>
class toFloatFromNumberFunction {
public:
    using ArgType = Number;
    using ResultType = std::optional<types::Double::Primitive>;

    ResultType operator()(const Number value) {
        return static_cast<types::Double::Primitive>(value);
    }
};

template <typename Argument>
concept ConvertibleNumber = std::is_arithmetic_v<Argument>
                            && !std::is_same_v<Argument, types::Bool::Primitive>;

// The element a column of the given type converts, with any optional unwrapped: what a
// conversion is applied to row by row.
template <typename ColumnType>
using ConversionArgument = TypeUtils::unwrap_inner_t<TypeUtils::decay_col_t<ColumnType>>;

// The conversion a column of the given element type goes through. A conversion names its
// string form, which stays the one every non-numeric element converts through.
template <typename StringFunctor, typename Argument>
struct ConversionFunctorFor {
    using Type = StringFunctor;
};

template <ConvertibleNumber Argument>
struct ConversionFunctorFor<toIntegerFunction, Argument> {
    using Type = toIntegerFromNumberFunction<Argument>;
};

template <ConvertibleNumber Argument>
struct ConversionFunctorFor<toFloatFunction, Argument> {
    using Type = toFloatFromNumberFunction<Argument>;
};

// The list family over a type-erased cell, which is what a list looks like wherever its
// type is known only per row - an element an UNWIND of a list of lists hands on. A cell
// holding a null answers null, as Cypher answers a list function over one; a cell holding
// no list at all is a type error only the row it is in can find out about, so each throws.

class TaggedListSizeFunction {
public:
    using ArgType = ListElementView;
    using ResultType = std::optional<types::Int64::Primitive>;

    ResultType operator()(ArgType cell) const;
    ResultType operator()(const std::optional<ArgType>& cell) const;
};

class TaggedListHeadFunction {
public:
    using ArgType = ListElementView;
    using ResultType = ListElementView;

    ResultType operator()(ArgType cell) const;
    ResultType operator()(const std::optional<ArgType>& cell) const;
};

class TaggedListLastFunction {
public:
    using ArgType = ListElementView;
    using ResultType = ListElementView;

    ResultType operator()(ArgType cell) const;
    ResultType operator()(const std::optional<ArgType>& cell) const;
};

class TaggedListTailFunction {
public:
    using ArgType = ListElementView;
    using ResultType = std::optional<types::List::Primitive>;

    ResultType operator()(ArgType cell) const;
    ResultType operator()(const std::optional<ArgType>& cell) const;
};

class ListSizeFunction {
public:
    using ArgType = types::List::Primitive;
    using ResultType = types::Int64::Primitive;
    using TaggedCounterpart = TaggedListSizeFunction;

    ResultType operator()(const ArgType list) const {
        return static_cast<ResultType>(list.size());
    }
};

class ListHeadFunction {
public:
    using ArgType = types::List::Primitive;
    using ResultType = ListElementView;
    using TaggedCounterpart = TaggedListHeadFunction;

    // An empty list has no first element and an absent one has no element at all, so both
    // head into the null a tagged cell carries itself: the result needs no nullable column
    static constexpr bool ReadsNullsItself = true;

    ResultType operator()(const ArgType list) const {
        return list.empty() ? ListElementView::nullElement() : list.front();
    }

    ResultType operator()(const std::optional<ArgType>& list) const {
        return list.has_value() ? (*this)(*list) : ListElementView::nullElement();
    }
};

class ListLastFunction {
public:
    using ArgType = types::List::Primitive;
    using ResultType = ListElementView;
    using TaggedCounterpart = TaggedListLastFunction;

    static constexpr bool ReadsNullsItself = true;

    ResultType operator()(const ArgType list) const {
        return list.empty() ? ListElementView::nullElement() : list.back();
    }

    ResultType operator()(const std::optional<ArgType>& list) const {
        return list.has_value() ? (*this)(*list) : ListElementView::nullElement();
    }
};

class ListTailFunction {
public:
    using ArgType = types::List::Primitive;
    using ResultType = types::List::Primitive;
    using TaggedCounterpart = TaggedListTailFunction;

    ResultType operator()(const ArgType list) const {
        return list.tail();
    }
};

// A function that reads its own nulls is handed the absent value itself and answers with a
// value of its result type, so its result rides a plain column rather than a nullable one.
template <typename Functor>
concept ReadsItsNulls = requires { requires Functor::ReadsNullsItself; };

// A function naming a counterpart reads the same argument out of a type-erased cell, so a
// column of cells is served by that one rather than by the function itself.
template <typename Functor>
concept HasTaggedCounterpart = requires { typename Functor::TaggedCounterpart; };

class CosineSimilarityFunction {
public:
    using ResultType = types::Double::Primitive;

    ResultType operator()(const types::Embedding::Primitive& a,
                          const types::Embedding::Primitive& b);
};

class EuclideanDistanceFunction {
public:
    using ResultType = types::Double::Primitive;

    ResultType operator()(const types::Embedding::Primitive& a,
                          const types::Embedding::Primitive& b);
};

/// Generic function executor; default constructible operator
template <typename Op, typename Res, typename Arg>
struct FunctionExecutor {
    static void apply(ColumnVector<Res>* res,
                      const ColumnVector<Arg>* arg) {
        const size_t size = arg->size();
        res->resize(size);

        const auto& argd = arg->getRaw();
        auto& resd = res->getRaw();

        auto op = Op {};
        for (size_t i = 0; i < size; i++) {
            resd[i] = op(argd[i]);
        }
    }

    static void apply(ColumnConst<Res>* res,
                      const ColumnConst<Arg>* arg) {
        const auto& argd = arg->getRaw();

        auto op = Op {};
        auto&& result = op(argd);
        res->set(result);
    }
};

/// Specialisation for labels()
template <typename Res, typename Arg>
struct FunctionExecutor<LabelsFunction, Res, Arg> {
    static void apply(ColumnVector<ListView>* res,
                      const ColumnNodeIDs* arg,
                      GraphView view,
                      QueryListBuffer* listBuffer) {
        const size_t size = arg->size();
        res->resize(size);

        const auto& argd = arg->getRaw();
        auto& resd = res->getRaw();

        LabelsFunction labels(view, listBuffer);
        for (size_t i = 0; i < size ; i ++) {
            resd[i] = labels(argd[i]);
        }
    }
};

/// Specialisation for type()
template <typename Res, typename Arg>
struct FunctionExecutor<EdgeTypesFunction, Res, Arg> {
    static void apply(ColumnVector<std::string>* res,
                      const ColumnEdgeIDs* arg,
                      GraphView view) {
        const size_t size = arg->size();
        res->resize(size);

        const auto& argd = arg->getRaw();
        auto& resd = res->getRaw();

        EdgeTypesFunction edgeType(view);
        for (size_t i = 0; i < size ; i ++) {
            resd[i] = edgeType(argd[i]);
        }
    }
};

template <typename Op, typename Res, typename ArgA, typename ArgB>
struct BinaryFunctionExecutor {
    static void apply(ColumnVector<Res>* res,
                      const ColumnVector<ArgA>* lhs,
                      const ColumnVector<ArgB>* rhs) {
        bioassert(lhs->size() == rhs->size(), "Misshapen ColumnVectors.");
        const size_t size = lhs->size();

        res->resize(size);

        const auto& lhsRaw = lhs->getRaw();
        const auto& rhsRaw = rhs->getRaw();
        auto& resRaw = res->getRaw();

        auto op = Op {};
        for (size_t i = 0; i < size; i++) {
            resRaw[i] = op(lhsRaw[i], rhsRaw[i]);
        }
    }

    static void apply(ColumnVector<Res>* res,
                      const ColumnVector<ArgA>* lhs,
                      const ColumnConst<ArgB>* rhs) {
        const size_t size = lhs->size();
        res->resize(size);

        const auto& lhsRaw = lhs->getRaw();
        const auto& val = rhs->getRaw();
        auto& resRaw = res->getRaw();

        auto op = Op {};
        for (size_t i = 0; i < size; i++) {
            resRaw[i] = op(TypeUtils::unwrap(lhsRaw[i]), TypeUtils::unwrap(val));
        }
    }

    static void apply(ColumnVector<Res>* res,
                      const ColumnConst<ArgA>* lhs,
                      const ColumnVector<ArgB>* rhs) {
        const size_t size = rhs->size();
        res->resize(size);


        const auto& val = lhs->getRaw();
        const auto& rhsRaw = rhs->getRaw();
        auto& resRaw = res->getRaw();

        auto op = Op {};
        for (size_t i = 0; i < size; i++) {
            resRaw[i] = op(TypeUtils::unwrap(val), TypeUtils::unwrap(rhsRaw[i]));
        }
    }

    static void apply(ColumnConst<Res>* res,
                      const ColumnConst<ArgA>* lhs,
                      const ColumnConst<ArgB>* rhs) {
        auto op = Op {};
        const Res& result = op(lhs->getRaw(), rhs->getRaw());
        res->set(result);
    }
};

template <typename F>
struct BinaryFunc {
    template <typename T, typename U>
        requires TypeUtils::is_optional_v<T> || TypeUtils::is_optional_v<U>
    inline decltype(auto) operator()(T&& a, U&& b) {
        return optionalGeneric<F>(std::forward<T>(a), std::forward<U>(b));
    }

    template <typename T, typename U>
    inline decltype(auto) operator()(T&& a, U&& b) {
        return F {}(std::forward<T>(a), std::forward<U>(b));
    }
};

using CosineSimilarity = BinaryFunc<CosineSimilarityFunction>;
using EuclideanDistance = BinaryFunc<EuclideanDistanceFunction>;

}

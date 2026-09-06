#pragma once

#include <math.h>

#include <limits>
#include <optional>
#include <string>
#include <system_error>
#include <type_traits>

#include "columns/ColumnConst.h"
#include "columns/ColumnIDs.h"
#include "columns/ColumnVector.h"
#include "TypeUtils.h"
#include "metadata/PropertyType.h"
#include "views/GraphView.h"

#include "ID.h"

#include "BioAssert.h"
#include "TuringException.h"

namespace db {

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
    using ResultType = std::string;

    explicit LabelsFunction(GraphView view)
        : _view(view)
    {
    }

    ResultType operator()(const NodeID n) {
        getLabelString(_tmp, _view, n);
        return _tmp;
    }

private:
    GraphView _view;
    std::string _tmp;

    static void getLabelString(std::string& out, GraphView view, NodeID n);
};

class EdgeTypesFunction {
public:
    using ArgType = EdgeID;
    using ResultType = std::string;

    explicit EdgeTypesFunction(GraphView view)
        : _view(view)
    {
    }

    ResultType operator()(const EdgeID e) {
        getEdgeTypeString(_tmp, _view, e);
        return _tmp;
    }

private:
    GraphView _view;
    std::string _tmp;

    static void getEdgeTypeString(std::string& out, GraphView view, EdgeID e);
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
    static void apply(ColumnVector<std::string>* res,
                      const ColumnNodeIDs* arg,
                      GraphView view) {
        const size_t size = arg->size();
        res->resize(size);

        const auto& argd = arg->getRaw();
        auto& resd = res->getRaw();

        LabelsFunction labels(view);
        for (size_t i = 0; i < size ; i ++) {
            resd[i] = labels(argd[i]);
        }
    }
};

/// Specialisation for edgeType()
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

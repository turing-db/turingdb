#pragma once

#include <limits>
#include <string>
#include <system_error>

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

class LabelsFunction {
public:
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
    using ResultType = types::Int64::Primitive;

    ResultType operator()(std::string_view sv) {
        ResultType result = std::numeric_limits<ResultType>::max();

        const auto* start = sv.data();
        const auto* end = sv.data() + sv.size();

        auto [ptr, ec] = std::from_chars(start, end, result);

        const bool success = ec == std::errc {};
        const bool parsedAll = ptr == end;

        if (!success || !parsedAll) {
            throw TuringException(fmt::format("toInteger: cannot convert '{}' to integer.", sv));
        }

        return result;
    }
};

// NOTE: macOS wheel build libc++ doesn't support from_chars on double: use strtod instead
class toFloatFunction {
public:
    using ResultType = types::Double::Primitive;

    ResultType operator()(std::string_view sv) {
        // @ref std::strtod requires a null-terminated std::string: convert the string_view
        _buf.assign(sv);
        char* end = nullptr;
        errno = 0;

        const ResultType result = std::strtod(_buf.data(), &end);

        // Either empty string, or could not parse all of the string as double
        const bool couldNotConvert = end == _buf.data() || end != _buf.data() + _buf.size();
        if (couldNotConvert) {
            throw TuringException(fmt::format("toFloat: cannot convert '{}' to float.", sv));
        }

        const bool outOfRange = errno == ERANGE;
        if (outOfRange) {
            throw TuringException(fmt::format("toFloat: cannot convert '{}' to float: out of range.", sv));
        }

        return result;
    }

private:
    std::string _buf; // Temporary buffer to null-terminate string view
};

class toBoolFunction {
public:
    using ResultType = types::Bool::Primitive;

    ResultType operator()(std::string_view sv) {
        strToLower(_buf, sv);

        if (_buf == "true") {
            return true;
        }
        if (_buf == "false") {
            return false;
        }

        throw TuringException(fmt::format("toBoolean: cannot convert '{}' to boolean.", sv));
    }

private:
    std::string _buf; // Preallocated buffer to reuse over iterations

    static void strToLower(std::string& lower, std::string_view src);
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

/// Binary function executor for two-argument functions
template <typename Op, typename Res, typename ArgA, typename ArgB>
struct BinaryFunctionExecutor {
    static void apply(ColumnVector<Res>* res,
                      const ColumnVector<ArgA>* lhs,
                      const ColumnVector<ArgB>* rhs) {
        bioassert(lhs->size() == rhs->size(), "Misshapen ColumnVectors.");

        const size_t size = lhs->size();

        res->resize(size);
        auto op = Op {};

        const auto& lhsRaw = lhs->getRaw();
        const auto& rhsRaw = rhs->getRaw();
        auto& resRaw = res->getRaw();
        for (size_t i = 0; i < size; i++) {
            resRaw[i] = op(TypeUtils::unwrap(lhsRaw[i]), TypeUtils::unwrap(rhsRaw[i]));
        }
    }

    static void apply(ColumnVector<Res>* res,
                      const ColumnVector<ArgA>* lhs,
                      const ColumnConst<ArgB>* rhs) {
        const size_t size = lhs->size();
        res->resize(size);
        auto op = Op {};

        const auto& lhsRaw = lhs->getRaw();
        const auto& val = rhs->getRaw();
        auto& resRaw = res->getRaw();
        for (size_t i = 0; i < size; i++) {
            resRaw[i] = op(TypeUtils::unwrap(lhsRaw[i]), TypeUtils::unwrap(val));
        }
    }

    static void apply(ColumnVector<Res>* res,
                      const ColumnConst<ArgA>* lhs,
                      const ColumnVector<ArgB>* rhs) {
        const size_t size = rhs->size();
        res->resize(size);
        auto op = Op {};

        const auto& val = lhs->getRaw();
        const auto& rhsRaw = rhs->getRaw();
        auto& resRaw = res->getRaw();
        for (size_t i = 0; i < size; i++) {
            resRaw[i] = op(TypeUtils::unwrap(val), TypeUtils::unwrap(rhsRaw[i]));
        }
    }

    static void apply(ColumnConst<Res>* res,
                      const ColumnConst<ArgA>* lhs,
                      const ColumnConst<ArgB>* rhs) {
        auto op = Op {};
        res->set(op(TypeUtils::unwrap(lhs->getRaw()), TypeUtils::unwrap(rhs->getRaw())));
    }
};

}

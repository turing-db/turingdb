#pragma once

#include <limits>
#include <optional>
#include <string>
#include <system_error>

#include <range/v3/view/drop.hpp>

#include "columns/ColumnVector.h"
#include "metadata/LabelMap.h"
#include "metadata/PropertyType.h"
#include "reader/GraphReader.h"
#include "views/GraphView.h"

#include "ID.h"

#include "PipelineException.h"

namespace db {

namespace rg = ranges;
namespace rv = rg::views;

static void strToLower(std::string& lower, std::string_view src) {
    lower.clear();
    lower.reserve();
    for (const auto c : src) {
        lower += static_cast<char>(std::tolower(static_cast<unsigned char>(c)));
    }
}

static void getLabelString(std::string& out, const GraphView view, NodeID n) {
    out.clear();
    const LabelSetHandle lblset = view.read().getNodeLabelSet(n);

    std::vector<LabelID> labels;
    lblset.decompose(labels);

    bioassert(!labels.empty(), "Could not retrieve labels for node {}.", n.getValue());

    const LabelMap& lblMap = view.metadata().labels();

    {
        const LabelID fstLbl = labels.front();
        const std::optional<std::string_view> fstName = lblMap.getName(fstLbl);
        bioassert(fstName, "Could not get name of LabelID {}.", fstLbl.getValue());
        const std::string_view fstNameUnwrapped = *fstName;

        out = std::string {fstNameUnwrapped};
    }

    for (const LabelID label : labels | rv::drop(1)) {
        out += ", ";

        const std::optional<std::string_view> name = lblMap.getName(label);
        bioassert(name, "Could not get name of LabelID {}.", label.getValue());

        out += *name;
    }
}

static void getEdgeTypeString(std::string& out, const GraphView view, EdgeID e) {
    out.clear();
    const EdgeTypeID et = view.read().getEdgeTypeID(e);

    const EdgeTypeMap& etMap = view.metadata().edgeTypes();
    const std::optional<std::string_view> name = etMap.getName(et);
    bioassert(name, "Could not get name of EdgeTypeID {}.", et.getValue());

    out = *name;
}

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
};

struct toIntegerFunction {
    using ResultType = types::Int64::Primitive;

    ResultType operator()(std::string_view sv) {
        ResultType result {std::numeric_limits<ResultType>::max()};

        const auto* start = sv.data();
        const auto* end = sv.data() + sv.size();

        auto [ptr, ec] = std::from_chars(start, end, result);

        const bool success = ec == std::errc {};
        const bool parsedAll = ptr == end;

        if (!success || !parsedAll) {
            throw PipelineException(
                fmt::format("toInteger: cannot convert '{}' to integer.", sv));
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
        char* end {nullptr};
        errno = 0;

        const ResultType result = std::strtod(_buf.data(), &end);

        // Either empty string, or could not parse all of the string as double
        const bool couldNotConvert = end == _buf.data() || end != _buf.data() + _buf.size();
        if (couldNotConvert) {
            throw PipelineException(
                fmt::format("toFloat: cannot convert '{}' to float.", sv));
        }

        const bool outOfRange = errno == ERANGE;
        if (outOfRange) {
            throw PipelineException(
                fmt::format("toFloat: cannot convert '{}' to float: out of range.", sv));
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

        throw PipelineException(
            fmt::format("toBoolean: cannot convert '{}' to boolean", sv));
    }

private:
    std::string _buf; // Preallocated buffer to reuse over iterations
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

}

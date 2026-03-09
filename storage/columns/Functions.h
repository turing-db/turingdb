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

struct LabelsFunction {
    using ResultType = std::string;

    ResultType operator()(const NodeID n) {
        getLabelString(_tmp, _view, n);
        return _tmp;
    }

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

struct toFloatFunction {
    using ResultType = types::Double::Primitive;

    ResultType operator()(std::string_view sv) {
        ResultType result {std::numeric_limits<ResultType>::max()};

        const auto* start = sv.data();
        const auto* end = sv.data() + sv.size();

        auto [ptr, ec] = std::from_chars(start, end, result);

        const bool success = ec == std::errc {};
        const bool parsedAll = ptr == end;

        if (!success || !parsedAll) {
            throw PipelineException(
                fmt::format("toFloat: cannot convert '{}' to float.", sv));
        }

        return result;
    }
};

// NOTE: Define this stub for uniformity when using @ref FunctionColumnResult.
// However, this function is never used, because we specialise
// @ref EvalFunction::eval<OP_TO_BOOLEAN> to allow for reuse of the string buffer.
struct toBoolFunction {
    using ResultType = types::Bool::Primitive;
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

        auto op = LabelsFunction{._view = view, ._tmp = ""};
        for (size_t i = 0; i < size ; i ++) {
            resd[i] = op(argd[i]);
        }
    }
};

}

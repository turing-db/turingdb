#pragma once

#include <optional>
#include <string>

#include <range/v3/view/drop.hpp>

#include "columns/ColumnOperator.h"
#include "views/GraphView.h"
#include "reader/GraphReader.h"
#include "metadata/LabelMap.h"

#include "ID.h"

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
    std::string operator()(const NodeID n) {
        getLabelString(_tmp, _view, n);
        return _tmp;
    }

    GraphView _view;
    std::string _tmp;
};

template <typename Op, typename Res, typename Arg>
struct FunctionExecutor {
    static void apply(ColumnVector<Res>* res,
                      const ColumnVector<Arg>* arg,
                      GraphView view) {
        const size_t size = arg->size();
        res->resize(size);

        const auto& argd = arg->getRaw();
        auto& resd = res->getRaw();

        auto op = Op {._view = view, ._tmp = ""};
        for (size_t i = 0; i < size ; i ++) {
            resd[i] = op(argd[i]);
        }
    }
};

}

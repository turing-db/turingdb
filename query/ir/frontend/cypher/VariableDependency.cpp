#include "VariableDependency.h"

#include <algorithm>
#include <variant>

#include "BioAssert.h"

using namespace db;

namespace {

void appendUnique(std::span<const std::string_view> names, std::vector<std::string_view>& out) {
    for (const std::string_view name : names) {
        const bool alreadyPresent = std::ranges::find(out, name) != out.end();
        if (alreadyPresent) {
            continue;
        }

        out.push_back(name);
    }
}

}

void VariableDependency::addIncoming(DependencyEdge* newEdge) {
    _incoming.push_back(newEdge);
}

void VariableDependency::addOutgoing(DependencyEdge* newEdge) {
    _outgoing.push_back(newEdge);
}

void VariableDependency::addLabelConstraints(std::span<const std::string_view> labels) {
    if (labels.empty()) {
        return;
    }

    if (_constraints) {
        bioassert(!std::holds_alternative<EdgeTypeNames>(*_constraints), "Have edge type");
    }

    if (!_constraints) {
        _constraints = LabelNames {};
    }

    LabelNames& labelNames = std::get<LabelNames>(*_constraints);

    appendUnique(labels, labelNames);
}

void VariableDependency::setEdgeTypeConstraint(std::span<const std::string_view> types) {
    if (types.empty()) {
        return;
    }

    bioassert(!_constraints.has_value(), "Edge already constrained.");

    _constraints = EdgeTypeNames {};

    EdgeTypeNames& edgeTypeNames = std::get<EdgeTypeNames>(*_constraints);

    appendUnique(types, edgeTypeNames._names);
}

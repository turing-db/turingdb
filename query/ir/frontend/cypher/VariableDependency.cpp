#include "VariableDependency.h"

#include <algorithm>
#include <variant>

#include "BioAssert.h"

using namespace db;

namespace {

template <typename T>
void appendUnique(std::span<const T> names, std::vector<T>& out) {
    for (const T& name : names) {
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

void VariableDependency::addLabelConstraints(std::span<const LabelRef> labels) {
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

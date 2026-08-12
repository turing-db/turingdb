#include "VariableDependency.h"

#include <algorithm>
#include <variant>

#include "BioAssert.h"

using namespace db;

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
        bioassert(!std::holds_alternative<EdgeType>(*_constraints), "Have edge type");
    }

    if (!_constraints) {
        _constraints = LabelNames {};
    }

    LabelNames& labelNames = std::get<LabelNames>(*_constraints);

    for (const std::string_view label : labels) {
        const bool alreadyPresent =
            std::ranges::find(labelNames, label) != labelNames.end();
        if (alreadyPresent) {
            continue;
        }

        labelNames.push_back(label);
    }
}

void VariableDependency::setEdgeTypeConstraint(std::string_view type) {
    if (type.empty()) {
        return;
    }

    bioassert(!_constraints.has_value(), "Multiple edge types.");

    _constraints = EdgeType {type};
}

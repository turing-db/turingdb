#pragma once

#include <deque>

#include "EntityPattern.h"

#include "SmallVector.h"

namespace db {

class VariableDependency;

class VariableDependencyGraph {
public:
    const VariableDependency* getOrCreateVariable(const EntityPattern* entity);

private:
    std::deque<VariableDependency> _vars;

    const VariableDependency* newVariable(const EntityPattern* entity);
};

class VariableDependency {
public:
    using Deps = SmallVector<const VariableDependency*, 4>;

    VariableDependency(const EntityPattern* entity)
        : _entity(entity)
    {
    }

    void addIncoming(const VariableDependency* dep) { _incoming.push_back(dep); }
    void addOutgoing(const VariableDependency* dep) { _outgoing.push_back(dep); }

    const EntityPattern* entity() const { return _entity; }

    bool isRoot() const { return _incoming.empty(); }

private:
    const EntityPattern* _entity {nullptr};
    Deps _incoming;
    Deps _outgoing;
};

}

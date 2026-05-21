#pragma once

#include <deque>

#include "SmallVector.h"

namespace db {

class EntityPattern;
class PatternElement;

class VariableDependency;

class VariableDependencyGraph {
public:
    void registerPatternElement(const PatternElement* ptn);

    const std::deque<VariableDependency>& vars() const { return _vars; }

private:
    std::deque<VariableDependency> _vars;

    VariableDependency* getOrCreateVariable(const EntityPattern* entity);
    VariableDependency* newVariable(const EntityPattern* entity);
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
    const Deps& getOutgoing() const { return _outgoing; }

    bool isRoot() const { return _incoming.empty(); }
    bool isSink() const { return _outgoing.empty(); }
    bool isIsolated() const { return isRoot() && isSink(); }

private:
    const EntityPattern* _entity {nullptr};
    Deps _incoming;
    Deps _outgoing;
};

}

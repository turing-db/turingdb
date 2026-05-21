#pragma once

#include <deque>
#include <vector>

namespace db {

class EntityPattern;
class PatternElement;

class VariableDependency;

/**
 * @brief Graph representation of the dependencies among variables, generated from
 * an analyzed AST.
 * @warn Ensure any AST elements passed to this class have been appropriately analysed, to
 * populate @ref VarDecl s.
 */
class VariableDependencyGraph {
public:
    /// Given a pattern (e.g. (n)-[e]->(m)), inserts all vars into the dependency graph
    void registerPatternElement(const PatternElement* ptn);

    /// Iteration order has no semantic meaning
    auto begin() const { return std::cbegin(_vars); }
    auto end() const { return std::cend(_vars); }

private:
    /// Variables whose dependencies are tracked this class
    std::deque<VariableDependency> _vars;

    VariableDependency* getOrCreateVariable(const EntityPattern* entity);
    VariableDependency* newVariable(const EntityPattern* entity);
};

/**
 * @brief A representation of a variable and its dependencies.
 * @detail Used in @ref VariableDependencyGraph
 */
class VariableDependency {
public:
    using Deps = std::vector<const VariableDependency*>;

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

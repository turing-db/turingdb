#pragma once

#include <deque>
#include <vector>

namespace db {

class EntityPattern;
class PatternElement;
class VarDecl;

/**
 * @brief A representation of a variable and its dependencies.
 * @detail Used in @ref VariableDependencyGraph
 */
class VariableDependency {
public:
    using Deps = std::vector<const VariableDependency*>;

    VariableDependency(VarDecl* decl)
        : _decl(decl)
    {
    }

    void dependsOn(VariableDependency* dep);
    void requiredFor(VariableDependency* dep);

    VarDecl* getDecl() const { return _decl; }
    const Deps& getOutgoing() const { return _outgoing; }

    bool isRoot() const { return _incoming.empty(); }
    bool isSink() const { return _outgoing.empty(); }
    bool isIsolated() const { return isRoot() && isSink(); }

private:
    VarDecl* _decl {nullptr};

    Deps _incoming;
    Deps _outgoing;
};

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

}

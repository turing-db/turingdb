#pragma once

#include <deque>
#include <string>
#include <string_view>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "DependencyEdge.h"
#include "VariableDependency.h"

namespace db {

class EntityPattern;
class PatternElement;

/**
 * @brief Graph representation of the dependencies among variables, generated from
 * an analyzed AST.
 * @warn Ensure any AST elements passed to this class have been appropriately analysed, to
 * populate @ref VarDecl s.
 */
class VariableDependencyGraph {
public:
    using Cycle = std::vector<VariableDependency*>;

    /// Given a pattern (e.g. (n)-[e]->(m)), inserts all vars into the dependency graph
    void registerPatternElement(const PatternElement* ptn);

    /// Iteration order has no semantic meaning
    const auto& vars() const { return _vars; }
    const auto& edges() const { return _edges; }

    const DependencyEdge* addDirected(VariableDependency* src, VariableDependency* tgt, const EdgeMetadata& data);

    std::vector<Cycle> cycleBasis();

    void detachCycle(const Cycle& cyc);

private:
    /// Variables whose dependencies are tracked by this class
    std::deque<VariableDependency> _vars;
    std::deque<DependencyEdge> _edges;

    std::unordered_set<VariableDependency*> _seenInCycle;

    std::unordered_map<VariableDependency*, int> _anonymised;

    VariableDependency* getOrCreateVariable(const EntityPattern* entity);
    VariableDependency* newVariable(const EntityPattern* entity);
    VariableDependency* newVariable(std::string_view name);

    std::string getNextAnonymisation(VariableDependency* v);

    void subdivideWithMerge(VariableDependency* s, VariableDependency* mid, VariableDependency* t);

    void subdivideWithMergeOutImpl(VariableDependency* s, VariableDependency* mid, VariableDependency* t, DependencyEdge* e);
    void subdivideWithMergeIncImpl(VariableDependency* s, VariableDependency* mid, VariableDependency* t, DependencyEdge* e);
};

}

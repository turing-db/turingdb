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

    /**
     * @brief Add a directed edge with provided metadata @param data between @param src
     * and @param tgt, if it does not already exist, otherwise, is noop.
     * @detail Appends to @ref src->_incoming and @ref tgt->_outgoing.
     */
    const DependencyEdge* addDirected(VariableDependency* src, VariableDependency* tgt, const EdgeMetadata& data);

    /**
    * @brief Get the cycle basis of this graph.
    */
    std::vector<Cycle> cycleBasis();

    /**
    */
    void detachCycle(const Cycle& cyc);

    /**
     * @brief Rotates provided @cyc such that the element with the highest in-degree is
     * first.
     */
    void canonicaliseCycle(Cycle& cyc) const;

private:
    /// Variables whose dependencies are tracked by this class
    std::deque<VariableDependency> _vars;
    std::deque<DependencyEdge> _edges;

    // Tracks whether a node has been seen in a cycle
    std::unordered_set<VariableDependency*> _seenInCycle;

    // Tracks how many times a variable has been anonimised
    std::unordered_map<VariableDependency*, int> _anonymised;

    VariableDependency* getOrCreateVariable(const EntityPattern* entity);
    VariableDependency* newVariable(const EntityPattern* entity);
    VariableDependency* newVariable(std::string_view name);

    std::string getNextAnonymisation(VariableDependency* v);

    /**
     * @brief For nodes @param s, @param t connected via an arbitrarily-directed edge, e,
     * and an arbitrary node @param mid, replaces s-[e]->t with s-[e]->mid-[merge]->t
     * @detail Original edge is removed from variable adjacency lists, and two new edges
     * are created. Removed edges are not removed from @ref _edges.
     */
    void subdivideWithMerge(VariableDependency* s, VariableDependency* mid, VariableDependency* t);

    void subdivideWithMergeOutImpl(VariableDependency* s, VariableDependency* mid, VariableDependency* t, DependencyEdge* e);
    void subdivideWithMergeIncImpl(VariableDependency* s, VariableDependency* mid, VariableDependency* t, DependencyEdge* e);
};

}

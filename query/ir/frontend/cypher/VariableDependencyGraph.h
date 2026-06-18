#pragma once

#include <deque>
#include <string>
#include <string_view>
#include <unordered_map>
#include <vector>

#include "DependencyEdge.h"
#include "VariableDependency.h"

namespace db {

class CypherAST;
class EntityPattern;
class PatternElement;

/**
 * @brief Graph representation of the dependencies among variables, generated from
 * an analyzed AST.
 *
 * @detail The graph holds the following invariants:
 * - for nodes u and v such that u::_outgoing contains an edge, e,  with @ref
 *   DependencyEdge::_tgt = v, then v::_incoming contains e
 * - any node has precisely zero or two incoming meta edges
 *   (see: DependencyEdge::isMetaEdge)
 *
 * A @ref DependencyEdge, e, in @ref _edges may become disconnected in the sense that no
 * @ref VariableDependency, v, in @ref _vars may have e in v::_incoming or v::_outgoing.
 *
 * @warn Ensure any AST elements passed to this class have been appropriately analysed, to
 * populate @ref VarDecl s.
 */
class VariableDependencyGraph {
public:
    using Cycle = std::vector<VariableDependency*>;

    VariableDependencyGraph();
    ~VariableDependencyGraph();

    void buildFromAST(const CypherAST* ast);

    /// Given a pattern (e.g. (n)-[e]->(m)), inserts all vars into the dependency graph
    void registerPatternElement(const PatternElement* ptn);

    /// Iteration order has no semantic meaning
    const auto& vars() const { return _vars; }
    const auto& edges() const { return _edges; }

    /**
     * @brief Removes all cycles in this graph by replacing the cyclic edges with meta
     * edges.
     */
    void eliminateCycles();

private:
    /// Variables whose dependencies are tracked by this class
    std::deque<VariableDependency> _vars;
    std::deque<DependencyEdge> _edges;

    // Tracks how many times a variable has been anonimised
    std::unordered_map<VariableDependency*, int> _anonymised;

    VariableDependency* getOrCreateVariable(const EntityPattern* entity);
    VariableDependency* newVariable(const EntityPattern* entity);
    VariableDependency* newVariable(std::string_view name);

    void getNextAnonymisation(VariableDependency* v, std::string& buf);

    /**
     * @brief Add a directed edge with provided metadata @param data between @param src
     * and @param tgt, if it does not already exist, otherwise, is noop.
     * @detail Appends to @ref src->_incoming and @ref tgt->_outgoing.
     */
    const DependencyEdge* addDirected(VariableDependency* src, VariableDependency* tgt, const EdgeMetadata& data);

    /**
     * @brief For nodes @param s, @param t possibly connected via an arbitrarily-directed
     * edge, e, replaces s-[e]-t with s-[e]-mid-[merge]->t if s and t are connected, and
     * otherwise is a no op.
     * @detail Original edge is removed from variable adjacency lists, and two new edges
     * are created. Removed edges are not removed from @ref _edges.
     * @returns The newly created midpoint node between s and t, or nullptr if s and t are
     * not connected
     */
    VariableDependency* subdivideWithMerge(VariableDependency* s, VariableDependency* t);

    void subdivideWithMergeOutImpl(VariableDependency* s, VariableDependency* mid, VariableDependency* t, DependencyEdge* e);
    void subdivideWithMergeIncImpl(VariableDependency* s, VariableDependency* mid, VariableDependency* t, DependencyEdge* e);

    /**
    * @brief Get the cycle basis of this graph.
    */
    void computeCycleBasis(std::vector<Cycle>& cycles);

    /**
     * @brief Removes a non-meta cycle by detaching the cycle and replacing the critical
     * edges via meta edges.
     */
    void detachCycle(const Cycle& cyc);

    /**
     * @brief Rotates provided @cyc such that the element with the highest in-degree is
     * first.
     * @detail Tends to make the resultant graph more human-readeable
     */
    static void canonicaliseCycle(Cycle& cyc);

    /**
     * @brief Ensures that no node in the graph has more than 2 incoming meta edges
     * @detail For each v in @ref _vars such that v has more than 2 incoming meta edges,
     * moves pairs of those meta edges to point to a newly created intermediary node, v',
     * and a meta edge from v' to v, repeating until v contains at most 2 incoming meta
     * edges
     */
    void cascadeMerges();
};

}

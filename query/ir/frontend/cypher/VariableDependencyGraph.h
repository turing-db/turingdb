#pragma once

#include <deque>
#include <span>
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
class Stmt;
class UnwindStmt;
class VarDecl;

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
    using EdgeIdentityMap = std::unordered_map<const VarDecl*, std::vector<VariableDependency*>>;
    using UnwindSourceMap = std::unordered_map<const VariableDependency*, const UnwindStmt*>;
    using BoundVars = std::vector<VariableDependency*>;

    VariableDependencyGraph();
    ~VariableDependencyGraph();

    /// Inserts the variables of one query part, keeping the ones @ref
    /// registerBoundVariable already declared so a pattern naming one depends on it
    void build(std::span<Stmt* const> stmts);

    /// Declares a variable a preceding WITH bound, whose column the code generator holds:
    /// a pattern naming it is a join onto that column, not a scan, so it enters isolated
    VariableDependency* registerBoundVariable(std::string_view name, const VarDecl* decl);

    /// Drops every variable and edge, so the graph can be rebuilt for the next query part
    void clear();

    /// Given a pattern (e.g. (n)-[e]->(m)), inserts all vars into the dependency graph
    void registerPatternElement(const PatternElement* ptn);

    /// Given an UNWIND (e.g. UNWIND [1, 2, 3] AS x), inserts its variable into the
    /// dependency graph
    void registerUnwindStmt(const UnwindStmt* stmt);

    /// Iteration order has no semantic meaning
    const auto& vars() const { return _vars; }
    const auto& edges() const { return _edges; }
    const EdgeIdentityMap& edgeIdentities() const { return _edgeIdentities; }
    const UnwindSourceMap& unwindSources() const { return _unwindSources; }
    const BoundVars& boundVars() const { return _boundVars; }

    bool empty() const { return _vars.empty() && _edges.empty(); }

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

    // Maps each Cypher edge variable to all anonymous VDG variables created for it
    EdgeIdentityMap _edgeIdentities;

    // Maps each UNWIND variable to the statement whose list it is bound to
    UnwindSourceMap _unwindSources;

    // The variables a preceding WITH bound, in the order that WITH projects them
    BoundVars _boundVars;

    VariableDependency* getOrCreateVariable(const EntityPattern* entity);
    VariableDependency* newVariable(const VarDecl* decl);
    VariableDependency* newVariable(std::string_view name);

    /// Under a name no Cypher identifier can be, so a user alias never resolves to it
    VariableDependency* newAnonymousVariable(const VarDecl* decl);

    VariableDependency* findVariable(const VarDecl* decl);

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

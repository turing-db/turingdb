#include "VariableDependencyGraph.h"

#include <algorithm>
#include <ranges>
#include <set>
#include <string_view>
#include <unordered_map>
#include <unordered_set>
#include <utility>

#include <spdlog/spdlog.h>

#include "EdgePattern.h"
#include "EntityPattern.h"
#include "VariableDependencyGraphDumper.h"
#include "CypherAST.h"
#include "SinglePartQuery.h"
#include "Pattern.h"
#include "PatternElement.h"
#include "stmt/MatchStmt.h"
#include "stmt/StmtContainer.h"
#include "decl/VarDecl.h"

#include "DependencyEdge.h"
#include "EdgeMetadata.h"
#include "VariableDependency.h"

#include "BioAssert.h"
#include "FatalException.h"

using namespace db;

static EdgeMetadata::EdgeType directionToType(EdgePattern::Direction dir) {
    switch (dir) {
        case EdgePattern::Direction::Undirected:
            return EdgeMetadata::EdgeType::GET_EDGES;
        break;
        case EdgePattern::Direction::Backward:
            return EdgeMetadata::EdgeType::GET_IN_EDGES;
        break;
        case EdgePattern::Direction::Forward:
            return EdgeMetadata::EdgeType::GET_OUT_EDGES;
        break;
    }
    throw FatalException("Invalid edge pattern direction");
}

static EdgeMetadata::EdgeType edgeTypeToNodeType(EdgeMetadata::EdgeType t) {
    if (t == EdgeMetadata::EdgeType::GET_OUT_EDGES) {
        return EdgeMetadata::EdgeType::GET_EDGE_TGT;
    }
    if (t == EdgeMetadata::EdgeType::GET_IN_EDGES) {
        return EdgeMetadata::EdgeType::GET_EDGE_SRC;
    }
    throw FatalException(
        fmt::format("Unsure how to get node type for {}", EdgeTypeName::value(t)));
}

VariableDependencyGraph::VariableDependencyGraph()
{
}

VariableDependencyGraph::~VariableDependencyGraph() {
}

void VariableDependencyGraph::buildFromAST(const CypherAST* ast) {
    bioassert(ast->queries().size() == 1, "Single queries only.");

    const QueryCommand* query = ast->queries().front();
    const auto* spq = dynamic_cast<const SinglePartQuery*>(query);
    bioassert(spq, "Non-SinglePartQueries are not yet supported.");

    const StmtContainer* stmtsContainer = spq->getReadStmts();
    const StmtContainer::Stmts& stmts = stmtsContainer->stmts();

    for (Stmt* stmt : stmts) {
        const auto* match = dynamic_cast<const MatchStmt*>(stmt);
        if (!match) {
            spdlog::warn("Non-match statement: skipped");
            continue;
        }

        const Pattern* pattern = match->getPattern();
        const Pattern::PatternElements& elements = pattern->elements();
        for (const PatternElement* element : elements) {
            registerPatternElement(element);
        }
    }

    eliminateCycles();
}

const DependencyEdge* VariableDependencyGraph::addDirected(VariableDependency* src,
                                                           VariableDependency* tgt,
                                                           const EdgeMetadata& data) {
    DependencyEdge newEdge(src, tgt, data);

    // Exercises invariant that src outgoing and tgt incoming are synced
    const auto same = [&newEdge](DependencyEdge* other) {
        return newEdge == *other;
    };

    // If an edge between @ref src and @ref tgt with equivalent @ref data, return that
    // edge, and do not add any edges. This canonicalises patterns such as
    // `MATCH (n)-->(a), (n)-->(a)`, avoiding adding duplicate edges.
    const auto findIt = std::ranges::find_if(src->_outgoing, same);
    const bool exists = findIt != end(src->_outgoing);
    if (exists) {
        return *findIt;
    }

    DependencyEdge& placedEdge = _edges.emplace_back(std::move(newEdge));

    src->addOutgoing(&placedEdge);
    tgt->addIncoming(&placedEdge);

    return &placedEdge;
}

void VariableDependencyGraph::registerPatternElement(const PatternElement* ptn) {
    const EntityPattern* origin = ptn->getRootEntity();

    VariableDependency* originVar = getOrCreateVariable(origin);

    const auto& chain = ptn->getElementChain();

    VariableDependency* prev = originVar;
    for (const auto& [edge, tgtPtn] : chain) {
        VariableDependency* tgtVar = getOrCreateVariable(tgtPtn);

        const EdgePattern::Direction direction = edge->getDirection();
        const EdgeMetadata::EdgeType edgeType = directionToType(direction);
        const EdgeMetadata::EdgeType otherType = edgeTypeToNodeType(edgeType);

        VariableDependency* src {nullptr};
        VariableDependency* tgt {nullptr};

        src = prev;
        tgt = tgtVar;

        VariableDependency* edgeVar = getOrCreateVariable(edge);
        addDirected(src, edgeVar, EdgeMetadata {edgeType});
        addDirected(edgeVar, tgt, EdgeMetadata {otherType});

        prev = tgt;
    }
}

VariableDependency* VariableDependencyGraph::newVariable(const EntityPattern* entity) {
    bioassert(entity->getDecl(), "Variable without declaration.");
    return &_vars.emplace_back(std::string(entity->getDecl()->getName()));
}

VariableDependency* VariableDependencyGraph::newVariable(std::string_view name) {
    return &_vars.emplace_back(name);
}

VariableDependency* VariableDependencyGraph::getOrCreateVariable(const EntityPattern* entity) {
    bioassert(entity->getDecl(), "Variable with null declaration.");
    const auto match = [entity](const VariableDependency& dep) {
        return entity->getDecl()->getName() == dep.getName();
    };
    const auto foundIt = std::ranges::find_if(_vars, match);
    const bool exists  = foundIt != _vars.end();

    return exists ? &*foundIt : newVariable(entity);
}

void VariableDependencyGraph::computeCycleBasis(std::vector<Cycle>& cycles) {
    using VarSet = std::unordered_set<VariableDependency*>;
    using PredMap = std::unordered_map<VariableDependency*, VariableDependency*>;
    using VarToVarSet = std::unordered_map<VariableDependency*, VarSet>;

    cycles.clear();

    if (_vars.empty()) {
        return;
    }

    // Persistent across iterations of outer loop
    VarSet visited;

    // @ref {pred, discovered, stack} are unique to each outer iteration

    PredMap pred; // Records spanning tree from its key
    // If v is present in this map, then v is discovered.
    // For a pair [v, set] in this map, the set used as value contains 2 types of vars:
    // 1. The variable that "discovered" v
    // 2. For a cycle v, ..., w, the set contains the other end of the
    // cycle, w, stored to prevent reporting the same cycle twice at both ends
    VarToVarSet discovered;
    std::vector<VariableDependency*> stack;

    Cycle cycle;

    // Outer loop ensures all connected components are traversed
    for (VariableDependency& v : _vars) {
        VariableDependency* root = &v;
        if (visited.contains(root)) {
            continue;
        }

        pred.clear();
        discovered.clear();
        stack.clear();

        // Register this node as the root of the spanning tree
        pred[root] = root;
        // Register this node as being discovered, but by nothing since it is root
        discovered[root] = {};

        stack.push_back(root);

        // DFS from the root of this connected component
        while (!stack.empty()) {
            VariableDependency* u = stack.back();
            stack.pop_back();

            for (DependencyEdge* edge : u->edges()) {
                VariableDependency* adj = edge->_src == u ? edge->_tgt : edge->_src;
                bioassert(adj != u, "Invalid self loop.");

                const bool encountered = discovered.contains(adj);
                if (!encountered) {
                    stack.push_back(adj);
                    pred[adj] = u;
                    discovered[adj] = {u};
                    continue;
                }

                // Otherwise, already encountered: found a cycle.

                const bool cycleAlreadyLogged = discovered[u].contains(adj);
                if (cycleAlreadyLogged) {
                    continue;
                }

                // Check for edge case for 2-element cycle, e.g. in (x)-->(x)
                const bool parallelEdge = pred[adj] == u && pred.contains(adj);
                if (parallelEdge) {
                    cycles.push_back({u, adj});
                    discovered[u].insert(adj);
                    continue;
                }

                cycle.clear();

                // We have an edge (u, adj) such that adj was already
                // discovered. discovered[adj] contains the node which first
                // discovered adj.
                const VarSet& adjDiscoverers = discovered[adj];
                cycle.push_back(adj);
                cycle.push_back(u);

                // Trace back the predecessors in the spanning tree from the parent of u.
                // Any node not in adjDiscoverers is part of the cycle. First node
                // encountered in adjDiscoverers is the end of the cycle, as it is a
                // node which leads to adj, just as u does.
                VariableDependency* p = pred[u];
                while (!adjDiscoverers.contains(p)) {
                    cycle.push_back(p); // Element in the cycle
                    p = pred[p];
                }
                // Add the element included in adjDiscoverers, the end of the cycle
                cycle.push_back(p);

                cycles.push_back(cycle);
                // Record u as the cycle partner of adj to prevent reporting this
                // cycle again
                discovered[adj].insert(u);
            }
        }

        for (const auto& entry : pred) {
            visited.insert(entry.first);
        }
    }
}

void VariableDependencyGraph::detachCycle(const Cycle& cyc) {
    if (cyc.empty()) {
        return;
    }

    bioassert(cyc.size() >= 2, "Invalid cycle.");

    // For a cycle (head, u, ..., v) with [v,head] in E :
    VariableDependency* head = cyc.front();
    VariableDependency* u = *next(begin(cyc));
    VariableDependency* v = *prev(end(cyc));

    // Break the cycle by subdividing with a merge edge
    subdivideWithMerge(u, head);
    subdivideWithMerge(v, head);
}

void VariableDependencyGraph::subdivideWithMergeOutImpl(VariableDependency* s,
                                                        VariableDependency* mid,
                                                        VariableDependency* t,
                                                        DependencyEdge* e) {
    std::erase_if(s->_outgoing, [e](DependencyEdge* f) { return f == e; });
    std::erase_if(t->_incoming, [e](DependencyEdge* f) { return f == e; });

    const EdgeMetadata& data = e->data();
    addDirected(s, mid, data);
    addDirected(mid, t, EdgeMetadata(EdgeMetadata::EdgeType::MERGE));
}

void VariableDependencyGraph::subdivideWithMergeIncImpl(VariableDependency* s,
                                                        VariableDependency* mid,
                                                        VariableDependency* t,
                                                        DependencyEdge* e) {
    std::erase_if(s->_incoming, [e](DependencyEdge* f) { return f == e; });
    std::erase_if(t->_outgoing, [e](DependencyEdge* f) { return f == e; });

    const EdgeMetadata& data = e->data();
    addDirected(mid, s, data);
    addDirected(mid, t, EdgeMetadata(EdgeMetadata::EdgeType::MERGE));
}

VariableDependency* VariableDependencyGraph::subdivideWithMerge(VariableDependency* s,
                                                                VariableDependency* t) {
    std::string buf;
    { // Search out edges of @param s for an edge to @param t
        const auto findOut = std::ranges::find_if(
            s->_outgoing, [t](DependencyEdge* e) { return e->_tgt == t; });
        if (findOut != end(s->_outgoing)) {
            getNextAnonymisation(t, buf);
            VariableDependency* mid = newVariable(buf);
            subdivideWithMergeOutImpl(s, mid, t, *findOut);
            return mid;
        }
    }

    { // Search in edges of @param s for an edge from @param t
        const auto findIn = std::ranges::find_if(
            s->_incoming, [t](DependencyEdge* e) { return e->_src == t; });
        if (findIn != end(s->_incoming)) {
            getNextAnonymisation(t, buf);
            VariableDependency* mid = newVariable(buf);
            subdivideWithMergeIncImpl(s, mid, t, *findIn);
            return mid;
        }
    }

    // @param s and @param t are not connected, nothing to do

    return nullptr;
}

void VariableDependencyGraph::getNextAnonymisation(VariableDependency* v, std::string& buf) {
    const std::string_view name = v->getName();
    const int count = _anonymised[v];

    _anonymised[v]++;

    buf.clear();
    buf += name;
    buf += '\'';
    buf += std::to_string(count);
}

void VariableDependencyGraph::canonicaliseCycle(Cycle& cyc) {
    const auto inDegree = [](const VariableDependency* v) {
        return v->incoming().size();
    };
    const auto pivot = std::ranges::max_element(
        cyc, [inDegree](auto&& a, auto&& b) { return inDegree(a) < inDegree(b); });

    std::ranges::rotate(cyc, pivot);
}

void VariableDependencyGraph::eliminateCycles() {
    std::vector<Cycle> cycles;
    computeCycleBasis(cycles);
    if (cycles.empty()) {
        return;
    }

    std::ranges::for_each(cycles, [](auto& c) { canonicaliseCycle(c); });
    std::ranges::for_each(cycles, [this](auto& c) { detachCycle(c); });

    cascadeMerges();
}

void VariableDependencyGraph::cascadeMerges() {
    DependencyEdge* meta1 = nullptr;
    DependencyEdge* meta2 = nullptr;
    const auto getMetaPair = [&](DependencyEdge* e) {
        if (!e->isMetaEdge()) {
            return false;
        }

        if (!meta1) {
            meta1 = e;
            return true;
        }

        if (!meta2) {
            meta2 = e;
            return true;
        }
        return false;
    };

    const auto eraseFromSrc = [](DependencyEdge* toDel) {
        VariableDependency* src = toDel->_src;
        std::erase_if(src->_outgoing, [toDel](DependencyEdge* e) { return e == toDel; });
    };

    const auto isMeta = [](const DependencyEdge* e) { return e->isMetaEdge(); };

    std::string nameBuf;
    for (VariableDependency& v : _vars) {
        // For a variable which has more than 2 meta-edges, merge pairs into intermediate
        // nodes until every node has at exactly 0 or 2 incoming merge edges.
        while (std::ranges::count_if(v._incoming, isMeta) > 2) {
            meta1 = nullptr;
            meta2 = nullptr;
            // Sets @ref meta1/2 with the meta edges to merge, and removes them from
            // @ref v._incoming
            std::erase_if(v._incoming, getMetaPair);
            bioassert(meta1 && meta2, "Failed to get meta edges.");
            eraseFromSrc(meta1);
            eraseFromSrc(meta2);

            VariableDependency* src1 = meta1->_src;
            VariableDependency* src2 = meta2->_src;
            getNextAnonymisation(&v, nameBuf);
            // The sources of these two merge edges will now each have a merge edge into
            // @ref parent instead of @ref v
            VariableDependency* parent = newVariable(nameBuf);

            EdgeMetadata data(EdgeMetadata::EdgeType::MERGE);
            addDirected(src1, parent, data);
            addDirected(src2, parent, data);
            // Final edge from cascaded merge into origin
            addDirected(parent, &v, data);
        }
    }
}

#include "VariableDependencyGraph.h"

#include <algorithm>
#include <ranges>
#include <set>
#include <span>
#include <string_view>
#include <unordered_map>
#include <unordered_set>
#include <utility>

#include "EdgePattern.h"
#include "EntityPattern.h"
#include "NodePattern.h"
#include "VariableDependencyGraphDumper.h"
#include "Pattern.h"
#include "PatternElement.h"
#include "stmt/MatchStmt.h"
#include "stmt/UnwindStmt.h"
#include "decl/PatternData.h"
#include "decl/VarDecl.h"

#include "DependencyEdge.h"
#include "EdgeMetadata.h"
#include "VariableDependency.h"

#include "BioAssert.h"
#include "FatalException.h"
#include "TuringException.h"

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
    } else if (t == EdgeMetadata::EdgeType::GET_IN_EDGES) {
        return EdgeMetadata::EdgeType::GET_EDGE_SRC;
    } else if (t == EdgeMetadata::EdgeType::GET_EDGES) {
        return EdgeMetadata::EdgeType::GET_EDGE_TGT;
    }
    throw FatalException(
        fmt::format("Unsure how to get node type for {}", EdgeTypeName::value(t)));
}

VariableDependencyGraph::VariableDependencyGraph()
{
}

VariableDependencyGraph::~VariableDependencyGraph() {
}

void VariableDependencyGraph::build(std::span<Stmt* const> stmts) {
    for (const Stmt* stmt : stmts) {
        if (const MatchStmt* match = dynamic_cast<const MatchStmt*>(stmt)) {
            const Pattern* pattern = match->getPattern();
            const Pattern::PatternElements& elements = pattern->elements();
            for (const PatternElement* element : elements) {
                registerPatternElement(element);
            }
        } else if (const UnwindStmt* unwind = dynamic_cast<const UnwindStmt*>(stmt)) {
            registerUnwindStmt(unwind);
        }
    }

    eliminateCycles();
}

VariableDependency* VariableDependencyGraph::registerBoundVariable(std::string_view name, const VarDecl* decl) {
    VariableDependency* boundVar = &_vars.emplace_back(name, decl);
    _boundVars.push_back(boundVar);

    return boundVar;
}

void VariableDependencyGraph::clear() {
    _vars.clear();
    _edges.clear();
    _anonymised.clear();
    _edgeIdentities.clear();
    _unwindSources.clear();
    _boundVars.clear();
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

    // Two patterns sharing an edge variable are joined on identity; one element naming it
    // twice is rejected instead, as no edge is two hops of one element.
    std::vector<const VarDecl*> edgesInElement;

    VariableDependency* prev = originVar;
    for (const auto& [edge, tgtPtn] : chain) {
        const VarDecl* edgeDecl = edge->getDecl();
        bioassert(edgeDecl, "Edge pattern without declaration.");

        const bool alreadyInElement =
            std::ranges::find(edgesInElement, edgeDecl) != edgesInElement.end();
        if (alreadyInElement) {
            throw TuringException("Re-using the same edge variable in a single pattern is not supported");
        }

        edgesInElement.push_back(edgeDecl);

        VariableDependency* tgtVar = getOrCreateVariable(tgtPtn);

        const EdgePattern::Direction direction = edge->getDirection();
        const EdgeMetadata::EdgeType edgeType = directionToType(direction);
        const EdgeMetadata::EdgeType otherType = edgeTypeToNodeType(edgeType);

        VariableDependency* src {nullptr};
        VariableDependency* tgt {nullptr};

        src = prev;
        tgt = tgtVar;

        const std::string_view cypherEdgeName = edgeDecl->getName();

        // An edge the pattern leaves anonymous is one occurrence of one variable, so it
        // joins nothing: only a variable the query named gets an identity, whose
        // occurrences the code generator equates
        VariableDependency* edgeVar = nullptr;
        if (edgeDecl->isUnnamed()) {
            edgeVar = newAnonymousVariable(edgeDecl);
        } else {
            std::vector<VariableDependency*>& edgeOccurrences = _edgeIdentities[edgeDecl];
            std::string occurrenceName;
            occurrenceName += cypherEdgeName;
            occurrenceName += '\'';
            occurrenceName += std::to_string(edgeOccurrences.size());
            edgeVar = newVariable(occurrenceName);
            edgeOccurrences.push_back(edgeVar);
        }

        const EdgePatternData* edgeData = edge->getData();
        std::string_view edgeTypeConstraint;
        if (edgeData) {
            const std::span<const std::string_view> types = edgeData->edgeTypeConstraints();
            bioassert(types.size() <= 1, "Edge pattern with more than one type; disjunction unsupported");
            if (!types.empty()) {
                edgeTypeConstraint = types.front();
            }
        }
        edgeVar->setEdgeTypeConstraint(edgeTypeConstraint);

        addDirected(src, edgeVar, EdgeMetadata {edgeType});
        addDirected(edgeVar, tgt, EdgeMetadata {otherType});

        prev = tgt;
    }
}

void VariableDependencyGraph::registerUnwindStmt(const UnwindStmt* stmt) {
    const VarDecl* decl = stmt->getDecl();
    bioassert(decl, "UNWIND without a declaration.");

    // An UNWIND variable is bound to a list rather than to a pattern, so it depends on
    // no other variable and enters the graph isolated - a root of its own connected
    // component, whose dataflow the code generator opens from the list. The lookup is what
    // registerPatternElement does: two variables of one declaration would leave every
    // resolver picking one of them by container order.
    VariableDependency* unwindVar = findVariable(decl);
    if (!unwindVar) {
        unwindVar = newVariable(decl);
    }

    _unwindSources[unwindVar] = stmt;
}

VariableDependency* VariableDependencyGraph::newVariable(const VarDecl* decl) {
    return &_vars.emplace_back(decl->getName(), decl);
}

VariableDependency* VariableDependencyGraph::newVariable(std::string_view name) {
    return &_vars.emplace_back(name);
}

VariableDependency* VariableDependencyGraph::newAnonymousVariable(const VarDecl* decl) {
    std::string name;
    name += decl->getName();
    name += '\'';

    return &_vars.emplace_back(name, decl);
}

VariableDependency* VariableDependencyGraph::findVariable(const VarDecl* decl) {
    const auto match = [decl](const VariableDependency& dep) {
        return dep.getDecl() == decl;
    };
    const auto foundIt = std::ranges::find_if(_vars, match);

    return foundIt != _vars.end() ? &*foundIt : nullptr;
}

VariableDependency* VariableDependencyGraph::getOrCreateVariable(const EntityPattern* entity) {
    const VarDecl* decl = entity->getDecl();
    bioassert(decl, "Variable with null declaration.");

    // The name of an entity the pattern leaves anonymous is generated, and a Cypher alias
    // can carry that spelling too - `WITH p AS v0` beside a `MATCH (v0)-->()` - so it is
    // given a name of the graph's own, which no Cypher identifier can be
    VariableDependency* var = findVariable(decl);
    if (!var) {
        var = decl->isUnnamed() ? newAnonymousVariable(decl) : newVariable(decl);
    }

    const NodePattern* node = dynamic_cast<const NodePattern*>(entity);
    if (node) {
        const NodePatternData* data = node->getData();
        if (data) {
            var->addLabelConstraints(data->labelConstraints());
        }
    }

    return var;
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
    const auto isEdgeVariable = [](const VariableDependency* v) {
        return std::ranges::any_of(v->incoming(), [](const DependencyEdge* e) {
            const EdgeMetadata::EdgeType type = e->data().type();
            return type == EdgeMetadata::EdgeType::GET_OUT_EDGES
                || type == EdgeMetadata::EdgeType::GET_IN_EDGES;
        });
    };
    const auto inDegree = [](const VariableDependency* v) {
        return v->incoming().size();
    };

    // Node variables must always rank above edge variables as the merge target.
    // Within the same category, prefer higher in-degree.
    const auto pivot = std::ranges::max_element(
        cyc,
        [&isEdgeVariable, &inDegree](auto&& a, auto&& b) {
            const bool aIsEdge = isEdgeVariable(a);
            const bool bIsEdge = isEdgeVariable(b);
            if (aIsEdge != bIsEdge) {
                return aIsEdge; // edge < node, so node wins max_element
            }
            return inDegree(a) < inDegree(b);
        });

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

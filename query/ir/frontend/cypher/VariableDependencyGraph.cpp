#include "VariableDependencyGraph.h"

#include <algorithm>
#include <iostream>
#include <set>
#include <string_view>
#include <unordered_map>
#include <unordered_set>
#include <utility>

#include "DependencyEdge.h"
#include "EdgeMetadata.h"
#include "EdgePattern.h"
#include "EntityPattern.h"
#include "IRDumper.h"
#include "PatternElement.h"

#include "BioAssert.h"
#include "VariableDependency.h"
#include "decl/VarDecl.h"
#include "spdlog/spdlog.h"

using namespace db;

static EdgeMetadata::EdgeType directionToType(EdgePattern::Direction dir) {
    switch (dir) {
        case EdgePattern::Direction::Undirected:
            return EdgeMetadata::EdgeType::BIDIRECTIONAL;
        break;
        case EdgePattern::Direction::Backward:
            return EdgeMetadata::EdgeType::INCOMING;
        break;
        case EdgePattern::Direction::Forward:
            return EdgeMetadata::EdgeType::OUTGOING;
        break;
    }
    std::unreachable();
    return EdgeMetadata::EdgeType::_SIZE;
}

const DependencyEdge* VariableDependencyGraph::addDirected(VariableDependency* src,
                                                           VariableDependency* tgt,
                                                           const EdgeMetadata& data) {
    DependencyEdge& newEdge = _edges.emplace_back(src, tgt, data);

    // Exercises invariant that src outgoing and tgt incoming are synced
    const auto same = [&newEdge](DependencyEdge* other) {
        return newEdge == *other;
    };
    const auto findIt = std::ranges::find_if(src->_outgoing, same);
    const bool exists = findIt != end(src->_outgoing);
    if (exists) {
        _edges.pop_back();
        return *findIt;
    }

    src->addOutgoing(&newEdge);
    tgt->addIncoming(&newEdge);

    return &newEdge;
}

void VariableDependencyGraph::registerPatternElement(const PatternElement* ptn) {
    const EntityPattern* origin = ptn->getRootEntity();

    VariableDependency* originVar = getOrCreateVariable(origin);

    const auto& chain = ptn->getElementChain();

    VariableDependency* prev = originVar;
    for (const auto& [edge, tgtPtn] : chain) {
        VariableDependency* tgtVar = getOrCreateVariable(tgtPtn);

        const EdgePattern::Direction direction = edge->getDirection();
        const EdgeMetadata::EdgeType type = directionToType(direction);

        VariableDependency* src {nullptr};
        VariableDependency* tgt {nullptr};

        if (direction == EdgePattern::Direction::Forward) {
            src = prev;
            tgt = tgtVar;
        } else {
            src = tgtVar;
            tgt = prev;
        }

        VariableDependency* edgeVar = getOrCreateVariable(edge);
        addDirected(src, edgeVar, EdgeMetadata {type});
        addDirected(edgeVar, tgt, EdgeMetadata {type});

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

std::vector<VariableDependencyGraph::Cycle> VariableDependencyGraph::cycleBasis() {
    using NodeSet = std::set<VariableDependency*>;
    using PredMap = std::unordered_map<VariableDependency*, VariableDependency*>;
    using UsedMap = std::unordered_map<VariableDependency*, NodeSet>;

    std::vector<Cycle> cycles;
    if (_vars.empty()) {
        return cycles;
    }

    NodeSet gnodes;
    for (VariableDependency& v : _vars) {
        gnodes.insert(&v);
    }

    VariableDependency* root = *begin(gnodes);

    // Records spanning tree from its key
    PredMap pred;
    UsedMap used;
    std::vector<VariableDependency*> stack;

    Cycle cycle;

    // Outer loop ensures all connected components are traversed
    while (!gnodes.empty()) {
        if (root == nullptr) {
            root = *begin(gnodes);
        }

        pred.clear();
        used.clear();
        stack.clear();

        pred[root] = root;
        used[root] = {};

        stack.push_back(root);

        // DFS from the root of this connected component
        while (!stack.empty()) {
            VariableDependency* u = stack.back();
            stack.pop_back();

            NodeSet& usedThisTraversal = used[u];

            for (DependencyEdge* edge : u->edges()) {
                VariableDependency* neighbour = edge->_src == u ? edge->_tgt : edge->_src;
                bioassert(neighbour != u, "Invalid self loop.");

                const bool encountered = used.contains(neighbour);
                if (!encountered) {
                    stack.push_back(neighbour);
                    pred[neighbour] = u;
                    used[neighbour] = {u};
                    continue;
                }

                // Otherwise, already encountered: found a cycle.

                const bool cycleAlreadyLogged = usedThisTraversal.contains(neighbour);
                if (cycleAlreadyLogged) {
                    continue;
                }

                // TODO: Remove, we should have a simple graph?
                // XXX: Consider (x)-->(x)
                // Parallel edge: u is neighbour's direct tree-parent, so pred walk would
                // start above u and never descend to neighbour. Emit 2-cycle directly.
                const bool isParallelEdge = pred.contains(neighbour) && pred[neighbour] == u;
                if (isParallelEdge) {
                    cycles.push_back({u, neighbour});
                    usedThisTraversal.insert(neighbour);
                    continue;
                }

                // Back-edge to ancestor: walk pred from u until hitting neighbour's parent.
                const NodeSet& pn = used[neighbour];
                cycle.clear();
                cycle.push_back(neighbour);
                cycle.push_back(u);

                VariableDependency* p = pred[u];
                while (!pn.contains(p)) {
                    cycle.push_back(p);
                    p = pred[p];
                }

                cycle.push_back(p);

                cycles.push_back(cycle);
                used[neighbour].insert(u);
            }
        }

        for (const auto& entry : pred) {
            gnodes.erase(entry.first);
        }
        root = nullptr;
    }

    return cycles;
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

    VariableDependency* newHead = nullptr;
    VariableDependency* newTail = nullptr;

    VariableDependency* merged1 = nullptr;
    VariableDependency* merged2 = nullptr;
    DependencyEdge* mergeToDelete1 = nullptr;
    DependencyEdge* mergeToDelete2 = nullptr;

    // If the current head has any incoming merge edges, it must have 2.
    // Find those two merge edges, delete them, but track the source nodes
    std::erase_if(head->_incoming, [&](DependencyEdge* e) {
        const bool meta = EdgeMetadata::isMetaEdge(e->data().type());
        if (!meta) {
            return false;
        }

        if (!merged1) {
            merged1 = e->_src;
            mergeToDelete1 = e;
            return true;
        }
        if (!merged2) {
            merged2 = e->_src;
            mergeToDelete2 = e;
            return true;
        }

        return false;
    });

    // Ensure we exactly 0 or 2 merge sources
    bioassert(!((bool)merged1 ^ (bool)merged2), "Invalid merge state");

    // If not cascading a merge, just merge the two nodes as normal
    if (!merged1 || !merged2) {
        newHead = newVariable(getNextAnonymisation(head));
        newTail = newVariable(getNextAnonymisation(head));

        spdlog::info("subdividing {}-{}->{}", u->getName(), newHead->getName(), head->getName());
        subdivideWithMerge(u, newHead, head);
        spdlog::info("subdividing {}-{}->{}", v->getName(), newTail->getName(), head->getName());
        subdivideWithMerge(v, newTail, head);

        // Register that we have seen these nodes in a cycle
        _seenInCycle.insert(begin(cyc), end(cyc));
        return;
    }

    // Otherwise, @ref head has already been a merge target, and we need to cascade the
    // merge by combining the existing two merge sources into a third, which is then
    // merged with the tail of the current cycle, into @ref head. This is to ensure that
    // any given merge has arity of at most 2 (to ensure it can be executed by a join).

    { // Erase the merge edges from the merge sources as well
        std::erase_if(merged1->_outgoing, [mergeToDelete1](DependencyEdge* e) {
            return e == mergeToDelete1;
        });
        std::erase_if(merged2->_outgoing, [mergeToDelete2](DependencyEdge* e) {
            return e == mergeToDelete2;
        });
    }

    // Create a parent of those two merge edges to cascade
    VariableDependency* mergeParent = newVariable(getNextAnonymisation(head));
    {
        EdgeMetadata data(EdgeMetadata::EdgeType::MERGE);
        spdlog::info("adding {}->{}", merged1->getName(), mergeParent->getName());
        addDirected(merged1, mergeParent, data);
        spdlog::info("adding {}->{}", merged2->getName(), mergeParent->getName());
        addDirected(merged2, mergeParent, data);
    }
    VariableDependency* otherInput = newVariable(getNextAnonymisation(head));

    // Only one of the ends of the cycle should be merged (property of cycle basis)
    // Employ a convention: make whatever side is already merged the "newHead"
    const bool startIsMerged = _seenInCycle.contains(u);
    newHead = startIsMerged ? mergeParent : otherInput;
    newTail = startIsMerged ? otherInput : mergeParent;

    // Parent of the previous two merges that were just cascaded, merged into current head
    spdlog::info("adding merge {}->{}", newHead->getName(), head->getName());
    addDirected(newHead, head, EdgeMetadata(EdgeMetadata::EdgeType::MERGE));
    // Add a temporary between the tail (unmerged end of cycle) and current head
    spdlog::info("subdividing {}-{}->{}", v->getName(), newTail->getName(), head->getName());
    subdivideWithMerge(v, newTail, head);

    _seenInCycle.insert(begin(cyc), end(cyc));
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
    addDirected(s, mid, data);
    addDirected(mid, t, EdgeMetadata(EdgeMetadata::EdgeType::MERGE));
}

void VariableDependencyGraph::subdivideWithMerge(VariableDependency* s,
                                                 VariableDependency* mid,
                                                 VariableDependency* t) {
    {
        const auto findOut = std::ranges::find_if(
            s->_outgoing, [t](DependencyEdge* e) { return e->_tgt == t; });
        // Case where the existing edge between s and t is outgoing s->t
        if (findOut != end(s->_outgoing)) {
            subdivideWithMergeOutImpl(s, mid, t, *findOut);
            return;
        }
    }

    {
        const auto findIn = std::ranges::find_if(
            s->_incoming, [t](DependencyEdge* e) { return e->_src == t; });
        // Case where the existing edge between s and t is incoming s<-t
        if (findIn != end(s->_incoming)) {
            subdivideWithMergeIncImpl(s, mid, t, *findIn);
            return;
        }
    }

    IRDumper::dumpMermaid(*this, std::cout);
    bioassert(false,
              "Attempted to subdivideWithMerge on two nodes that were not connected: {} and {}.",
              s->getName(), t->getName());
}

std::string VariableDependencyGraph::getNextAnonymisation(VariableDependency* v) {
    const std::string_view name = v->getName();
    const int count = _anonymised[v];

    _anonymised[v]++;

    std::string out;
    out += name;
    out += '\'';
    out += std::to_string(count);

    return out;
}

VariableDependencyGraph::Cycle VariableDependencyGraph::dfs(VariableDependency* cur,
                                                            VariableDependency* prev,
                                                            bool fromMeta) {
    const auto edges = cur->edges();
    _dfsVisited.emplace(cur);
    _dfsPath.push_back(cur);
    _dfsPathSet.emplace(cur);

    for (const DependencyEdge* e : edges) {
        VariableDependency* other = e->_src == cur ? e->_tgt : e->_src;

        const bool justVisited = other == prev;
        if (justVisited) {
            continue;
        }

        const bool seenOnPath = _dfsPathSet.contains(other);
        if (seenOnPath) {
            const auto start = std::ranges::find(_dfsPath, other);
            return {start, end(_dfsPath)};
        }

        const bool metaEdge = e->isMetaEdge();
        if (fromMeta && metaEdge) {
            continue;
        }

        const bool visited = _dfsVisited.contains(other);
        if (visited) {
            continue;
        }

        Cycle cyc = dfs(other, cur, metaEdge);
        if (!cyc.empty()) {
            return cyc;
        }

    }

    _dfsPath.pop_back();
    _dfsPathSet.erase(cur);
    return {};
}

VariableDependencyGraph::Cycle VariableDependencyGraph::getCycle() {
    Cycle cyc;

    if (_vars.empty()) {
        return cyc;
    }

    _dfsPath.clear();
    _dfsPathSet.clear();
    _dfsVisited.clear();

    for (VariableDependency& v : _vars) {
        if (_dfsVisited.contains(&v)) {
            continue;
        }

        cyc = dfs(&v, nullptr, false);
        if (!cyc.empty()) {
            const auto inDegree = [](const VariableDependency* v) {
                return v->incoming().size();
            };
            const auto sinkLike =
                std::ranges::max_element(cyc, [inDegree](auto&& a, auto&& b) {
                    return inDegree(a) < inDegree(b);
                });
            std::ranges::rotate(cyc, sinkLike);
            return cyc;
        }
    }

    return cyc;
}

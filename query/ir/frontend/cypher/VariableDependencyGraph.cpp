#include "VariableDependencyGraph.h"

#include <algorithm>
#include <cstddef>
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

    // If an edge between @ref src and @ref tgt with equivalent @ref data, return that
    // edge, and do not add any edges. This canonicalises patterns such as
    // `MATCH (n)-->(a), (n)-->(a)`, avoiding adding duplicate edges.
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
    using VarSet = std::unordered_set<VariableDependency*>;
    using PredMap = std::unordered_map<VariableDependency*, VariableDependency*>;
    using VarToVarSet = std::unordered_map<VariableDependency*, VarSet>;

    std::vector<Cycle> cycles;
    if (_vars.empty()) {
        return cycles;
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

void VariableDependencyGraph::canonicaliseCycle(Cycle& cyc) const {
    const auto inDegree = [](const VariableDependency* v) {
        return v->incoming().size();
    };
    const auto pivot = std::ranges::max_element(
        cyc, [inDegree](auto&& a, auto&& b) { return inDegree(a) < inDegree(b); });

    std::ranges::rotate(cyc, pivot);
}

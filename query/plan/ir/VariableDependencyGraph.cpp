#include "VariableDependencyGraph.h"

#include <algorithm>
#include <iostream>
#include <set>
#include <string_view>
#include <unordered_map>
#include <unordered_set>
#include <utility>

#include <range/v3/view/chunk.hpp>
#include <range/v3/view/concat.hpp>

#include "EdgePattern.h"
#include "EntityPattern.h"
#include "PatternElement.h"

#include "BioAssert.h"
#include "decl/VarDecl.h"
#include "ir/IRDumper.h"
#include "spdlog/spdlog.h"

using namespace db;

namespace rg = ranges;
namespace rv = rg::views;

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

auto VariableDependency::edges() const {
    return rv::concat(_incoming, _outgoing);
}

bool VariableDependency::isIsolated() const {
    return edges().empty();
}

void VariableDependency::addIncoming(DependencyEdge* newEdge) {
    _incoming.push_back(newEdge);
}

void VariableDependency::addOutgoing(DependencyEdge* newEdge) {
    _outgoing.push_back(newEdge);
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
        _edges.pop_back(); // Iterators is still valid
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
        VariableDependency* edgeVar = getOrCreateVariable(edge);
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

        // Below checks for duplication
        addDirected(src, edgeVar, EdgeMetadata {type});
        addDirected(edgeVar, tgt, EdgeMetadata{type});

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

// std::vector<VariableDependencyGraph::Cycle> VariableDependencyGraph::paton() {
//     std::vector<Cycle> cycles;
//     const size_t n = _vars.size();
//     if (n == 0) {
//         return cycles;
//     }

//     // Exists in map => visited
//     using VisitedEdges = std::unordered_set<DependencyEdge*>;
//     // [x, true] => visited, [y, false] => not visited
//     using VisitedNodes = std::unordered_map<VariableDependency*, bool>;

//     VisitedNodes X;
//     X.reserve(n);
//     for (VariableDependency& v : _vars) {
//         X.emplace(&v, false);
//     }

//     return cycles;
// }

std::vector<VariableDependencyGraph::Cycle> VariableDependencyGraph::cycleBasis() {
    using NodeSet = std::set<VariableDependency*>;
    using PredMap = std::unordered_map<VariableDependency*, VariableDependency*>;
    using UsedMap = std::unordered_map<VariableDependency*, NodeSet>;

    NodeSet gnodes;
    for (VariableDependency& v : _vars) {
        gnodes.insert(&v);
    }

    std::vector<Cycle> cycles;

    if (gnodes.empty()) {
        return cycles;
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

                // Recurse in DFS
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

                // Parallel edge: u is neighbour's direct tree-parent, so pred walk would
                // start above u and never descend to neighbour. Emit 2-cycle directly.
                const bool isParallelEdge = pred.contains(neighbour) && pred[neighbour] == u;
                if (isParallelEdge) {
                    cycles.push_back({u, neighbour, u});
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

std::vector<VariableDependency*> VariableDependencyGraph::getCycle() {
    std::unordered_set<const VariableDependency*> visited;
    std::vector<VariableDependency*> path;
    std::unordered_set<const VariableDependency*> pathSet;

    const auto meta = [](const DependencyEdge* e) {
        return EdgeMetadata::isMetaEdge(e->data().type());
    };

    spdlog::info("\n\nfind cycles...");
    for (VariableDependency& node : _vars) {
        spdlog::info("Considering {}", node.getName());

        // Cannot be the head of a cycle degree less than 2
        if (node.edges().size() < 2) {
            spdlog::info("\t degree too low");
            continue;
        }

        // If all meta, cycle has been resolved
        const bool allMeta = std::ranges::all_of(node.edges(), meta);
        if (allMeta) {
            spdlog::info("\t all meta");
            continue;
        }
        
        if (!visited.contains(&node)) {
            auto result = findCycle(&node, nullptr, visited, path, pathSet);
            if (!result.empty()) {
                spdlog::info("\t FOUND");
                return result;
            }
        }
        spdlog::info("\t no cycle found");
    }

    return {};
}

std::vector<VariableDependency*> VariableDependencyGraph::findCycle(
    VariableDependency* curr, VariableDependency* prev,
    std::unordered_set<const VariableDependency*>& visited,
    std::vector<VariableDependency*>& path,
    std::unordered_set<const VariableDependency*>& pathSet) {
    visited.insert(curr);
    path.push_back(curr);
    pathSet.insert(curr);

    for (DependencyEdge* edge : curr->edges()) {
        // if (EdgeMetadata::isMetaEdge(edge->data().type())) {
            // continue;
        // }
        spdlog::info("\t\tConsidering {}->{}", edge->src()->getName(), edge->tgt()->getName());

        bioassert(edge->src() != edge->tgt(), "Invalid edge.");

        VariableDependency* adj = edge->_src == curr ? edge->_tgt : edge->_src;

        if (adj == prev) {
            spdlog::warn("\t\t skipping");
            continue;
        }

        if (pathSet.contains(adj)) {
            path.push_back(adj);
            const auto cycleStart = std::ranges::find(path, adj);
            std::vector result(cycleStart, end(path));
            path.pop_back();
            path.pop_back();
            pathSet.erase(curr);
            return result;
        }

        if (!visited.contains(adj)) {
            if (auto result = findCycle(adj, curr, visited, path, pathSet);
                !result.empty()) {
                path.pop_back();
                pathSet.erase(curr);
                return result;
            }
        }
    }

    path.pop_back();
    pathSet.erase(curr);
    return {};
}

void VariableDependencyGraph::rewriteCycle(const Cycle& cyc) {
    if (cyc.empty()) {
        return;
    }

    bioassert(cyc.front() == cyc.back(), "Invalid cycle.");
    bioassert(cyc.size() >= 3, "Invalid cycle.");

    VariableDependency* head = cyc.front();
    const std::string_view headName = head->getName();

    const std::string fstName = std::string(headName) + "'";
    const std::string sndName = std::string(headName) + "''";

    VariableDependency* newHead = newVariable(fstName);
    VariableDependency* newTail = newVariable(sndName);

    auto edges = head->edges();

    const VariableDependency* nxt = *next(begin(cyc));
    const VariableDependency* prv = *prev(prev(end(cyc)));

    // replace head and tail of loop with new vars
    for (DependencyEdge* e : edges) {
        // TODO: remove all these cases, should be explicit which is src/tgt
        if (e->src() == nxt && e->tgt() == head) {
            e->_tgt = newHead;
        } else if (e->tgt() == nxt && e->src() == head) {
            e->_src = newHead;
        } else if (e->src() == prv && e->tgt() == head) {
            e->_tgt = newTail;
        } else if (e->tgt() == prv && e->src() == head) {
            e->_src = newTail;
        }
    }

    addDirected(newHead, head, EdgeMetadata{EdgeMetadata::EdgeType::MERGE});
    addDirected(newTail, head, EdgeMetadata{EdgeMetadata::EdgeType::MERGE});
}

void VariableDependencyGraph::resetCycleState() {
    _cyclicParents.clear();
    _cyclicVisited.clear();
}


VariableDependency* VariableDependencyGraph::_dfs(VariableDependency* u, VariableDependency* par) {
    _cyclicVisited[u] = true;

    for (const DependencyEdge* e : u->edges()) {
        VariableDependency* adj = e->src() == u ? e->_tgt : e->_src;
        if (adj == par) {
            continue;
        }

        if (_cyclicVisited[adj]) {
            _cyclicParents[adj] = u;
            return adj;
        }

        _cyclicParents[adj] = u;
        VariableDependency* res = _dfs(adj, u);
        if (res) {
            return res;
        }
    }

    return nullptr;
}

VariableDependencyGraph::Cycle VariableDependencyGraph::_getCycle() {
    resetCycleState();

    const auto meta = [](const DependencyEdge* e) {
        return EdgeMetadata::isMetaEdge(e->data().type());
    };

    for (VariableDependency& node : _vars) {
        // Cannot be the head of a cycle, degree less than 2
        if (node.edges().size() < 2) {
            continue;
        }

        // If all meta, cycle has been resolved
        const bool allMeta = std::ranges::all_of(node.edges(), meta);
        if (allMeta) {
            continue;
        }

        // already visited, cannot have cycle
        if (_cyclicVisited.contains(&node)){
            continue;
        }

        VariableDependency* start = _dfs(&node, nullptr);
        fmt::println("");
        int max = 100;
        if (start) {
            Cycle out;
            out.push_back(start);
            VariableDependency* cur = _cyclicParents[start];
            while (cur != start && max-- && _cyclicParents.contains(cur)) {
                out.push_back(cur);
                cur = _cyclicParents[cur];
            }
            out.push_back(start);
            return out;
        }
    }

    return {};
}

bool VariableDependencyGraph::patchEdgeSrc(DependencyEdge* e,
                                           VariableDependency* oldSrc,
                                           VariableDependency* newSrc) {
    VariableDependency* src = e->_src;
    if (src != oldSrc) {
        return false;
    }

    e->_src = newSrc;
    return true;
}

bool VariableDependencyGraph::patchEdgeTgt(DependencyEdge* e,
                                           VariableDependency* oldtgt,
                                           VariableDependency* newTgt) {
    VariableDependency* tgt = e->_tgt;
    if (tgt != oldtgt) {
        return false;
    }

    e->_tgt = newTgt;
    return true;
}

void VariableDependencyGraph::addMerge(VariableDependency* from1,
                                       VariableDependency* from2,
                                       VariableDependency* into,
                                       VariableDependency* via1,
                                       VariableDependency* via2) {
    addBetween(from1, via1, into);
    IRDumper::dumpMermaid(*this, std::cout);
    addBetween(from2, via2, into);
    IRDumper::dumpMermaid(*this, std::cout);
}

void VariableDependencyGraph::detachCycle(const Cycle& cyc) {
    if (cyc.empty()) {
        return;
    }

    bioassert(cyc.size() >= 3, "Invalid cycle.");

    VariableDependency* head = cyc.front();

    VariableDependency* newHead = nullptr;
    VariableDependency* newTail = nullptr;

    VariableDependency* merged1 = nullptr;
    VariableDependency* merged2 = nullptr;

    std::erase_if(head->_incoming, [&](DependencyEdge* e) {
        const bool meta = EdgeMetadata::isMetaEdge(e->data().type());
        if (!meta) {
            return false;
        }

        if (!merged1) {
            merged1 = e->_src;
            return true;
        }
        if (!merged2) {
            merged2 = e->_src;
            return true;
        }

        return false;
    });

    bioassert(!((bool)merged1 ^ (bool)merged2), "Invalid merge state");

    if (merged1 && merged2) {
        newHead = newVariable(getNextAnonymisation(head));
        addDirected(merged1, newHead, EdgeMetadata(EdgeMetadata::EdgeType::MERGE));
        addDirected(merged2, newHead, EdgeMetadata(EdgeMetadata::EdgeType::MERGE));
    }

    newHead = newHead ? newHead : newVariable(getNextAnonymisation(head));
    newTail = newVariable(getNextAnonymisation(head));
    spdlog::info("nh = {}, nt = {}", newHead->getName(), newTail->getName());

    VariableDependency* nxt = *next(begin(cyc));
    VariableDependency* prv = *prev(end(cyc));

    addMerge(nxt, prv, head, newHead, newTail);
}

void VariableDependencyGraph::addBetweenOutImpl(VariableDependency* s,
                                                VariableDependency* mid,
                                                VariableDependency* t,
                                                DependencyEdge* e) {
    std::erase_if(s->_outgoing, [e](DependencyEdge* f) { return f == e; });
    std::erase_if(t->_incoming, [e](DependencyEdge* f) { return f == e; });

    const EdgeMetadata& data = e->data();
    addDirected(s, mid, data);
    addDirected(mid, t, EdgeMetadata(EdgeMetadata::EdgeType::MERGE));
}

void VariableDependencyGraph::addBetweenIncImpl(VariableDependency* s,
                                                VariableDependency* mid,
                                                VariableDependency* t,
                                                DependencyEdge* e) {
    std::erase_if(s->_incoming, [e](DependencyEdge* f) { return f == e; });
    std::erase_if(t->_outgoing, [e](DependencyEdge* f) { return f == e; });

    const EdgeMetadata& data = e->data();
    addDirected(mid, s, data);
    addDirected(t, mid, EdgeMetadata(EdgeMetadata::EdgeType::MERGE));
}

void VariableDependencyGraph::addBetween(VariableDependency* s,
                                         VariableDependency* mid,
                                         VariableDependency* t) {
    spdlog::info("Attempting to add {} between {} and {}", mid->getName(), s->getName(), t->getName());
    const auto findOut = std::ranges::find_if(
        s->_outgoing, [t](DependencyEdge* e) { return e->_tgt == t; });
    if (findOut != end(s->_outgoing)) {
        addBetweenOutImpl(s, mid, t, *findOut);
        return;
    }

    const auto findIn = std::ranges::find_if(
        s->_incoming, [t](DependencyEdge* e) { return e->_src == t; });
    if (findIn != end(s->_incoming)) {
        addBetweenIncImpl(s, mid, t, *findOut);
        return;
    }

    bioassert(false, "Attempted to addBetween on two nodes that were not connected.");
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

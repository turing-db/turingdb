#include "VariableDependencyGraph.h"

#include <algorithm>
#include <stack>
#include <string_view>
#include <unordered_set>
#include <utility>

#include <range/v3/view/chunk.hpp>
#include <range/v3/view/concat.hpp>

#include "EdgePattern.h"
#include "EntityPattern.h"
#include "PatternElement.h"

#include "BioAssert.h"
#include "decl/VarDecl.h"
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

std::vector<VariableDependencyGraph::Cycle> VariableDependencyGraph::cycleBasis() {
    using NodeSet = std::unordered_set<VariableDependency*>;
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

    PredMap pred;
    UsedMap used;
    std::vector<VariableDependency*> stack;

    Cycle cycle;

    // Outer loop ensures all connected components are traversed
    while (!gnodes.empty()) {
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
                VariableDependency* neighbor = edge->_src == u ? edge->_tgt : edge->_src;
                bioassert(neighbor != u, "Invalid self loop.");

                // Recurse in DFS
                const bool encountered = used.contains(neighbor);
                if (!encountered) {
                    stack.push_back(neighbor);
                    pred[neighbor] = u;
                    used[neighbor] = {u};
                    continue;
                }

                // Otherwise, already encountered: found a cycle.

                const bool cycleAlreadyLogged = usedThisTraversal.contains(neighbor);
                if (cycleAlreadyLogged) {
                    continue;
                }

                // Form the cycle
                cycle.clear();
                cycle.push_back(neighbor);
                cycle.push_back(u);

                // Get the nodes used on the path of this cycle
                const NodeSet& pn = used[neighbor];

                VariableDependency* p = pred[u];
                while (!pn.contains(p)) {
                    cycle.push_back(p);
                    p = pred[p];
                }

                cycle.push_back(p);
                cycle.push_back(u); // Ensure cycle is of the form (u, v, ..., x, u)

                cycles.push_back(cycle);

                used[neighbor].insert(u);
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

void VariableDependencyGraph::detachCycle(const Cycle& cyc) {
    if (cyc.empty()) {
        return;
    }

    bioassert(cyc.front() == cyc.back(), "Invalid cycle.");
    bioassert(cyc.size() >= 3, "Invalid cycle.");

    VariableDependency* head = cyc.front();

    const std::string fstName = getNextAnonymisation(head);
    const std::string sndName = getNextAnonymisation(head);

    VariableDependency* newHead = newVariable(fstName);
    VariableDependency* newTail = newVariable(sndName);

    const VariableDependency* nxt = *next(begin(cyc));
    const VariableDependency* prv = *prev(prev(end(cyc)));

    const auto updateAndDeleteOutgoing = [&](DependencyEdge* e) -> bool {
        const bool first = e->tgt() == nxt;
        const bool last = e->tgt() == prv;
        if (!first && !last) {
            return false;
        }

        if (first) {e->_src = newHead; newHead->addOutgoing(e);}
        if (last) {e->_src = newTail; newTail->addOutgoing(e);}

        return true;
    };

    const auto updateAndDeleteIncoming = [&](DependencyEdge* e) -> bool {
        const bool first = e->src() == nxt;
        const bool last = e->src() == prv;
        if (!first && !last) {
            return false;
        }

        if (first) {e->_tgt = newHead; newHead->addIncoming(e);}
        if (last) {e->_tgt = newTail; newTail->addIncoming(e);}

        return true;
    };

    std::erase_if(head->_outgoing, updateAndDeleteOutgoing);
    std::erase_if(head->_incoming, updateAndDeleteIncoming);

    addDirected(head, newHead, EdgeMetadata{EdgeMetadata::EdgeType::MERGE});
    addDirected(head, newTail, EdgeMetadata{EdgeMetadata::EdgeType::MERGE});
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

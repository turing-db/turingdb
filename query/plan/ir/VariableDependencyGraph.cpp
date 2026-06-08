#include "VariableDependencyGraph.h"

#include <algorithm>
#include <string_view>
#include <unordered_set>
#include <utility>

#include <range/v3/view/chunk.hpp>

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

void VariableDependency::addEdge(DependencyEdge* newEdge) {
    _edges.push_back(newEdge);
}

const DependencyEdge* VariableDependencyGraph::connect(VariableDependency* u,
                                                       VariableDependency* v,
                                                       const EdgeMetadata& data) {
    DependencyEdge& newEdge = _edges.emplace_back(u, v, data);

    u->addEdge(&newEdge);
    v->addEdge(&newEdge);

    return &newEdge;
}

void VariableDependencyGraph::registerPatternElement(const PatternElement* ptn) {
    const EntityPattern* origin = ptn->getRootEntity();

    VariableDependency* originVar = getOrCreateVariable(origin);

    const auto& chain = ptn->getElementChain();

    VariableDependency* prev = originVar;
    for (const auto& [edge, tgt] : chain) {
        VariableDependency* edgeVar = getOrCreateVariable(edge);
        VariableDependency* tgtVar = getOrCreateVariable(tgt);

        const EdgePattern::Direction direction = edge->getDirection();
        const EdgeMetadata::EdgeType type = directionToType(direction);
        spdlog::info("dir = {}, type = {}", std::to_underlying(direction), std::to_underlying(type));

        connect(prev, edgeVar, EdgeMetadata {type});
        connect(edgeVar, tgtVar, EdgeMetadata{type});

        prev = tgtVar;
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

std::vector<VariableDependency*> VariableDependencyGraph::getCycle() {
    std::unordered_set<const VariableDependency*> visited;
    std::vector<VariableDependency*> path;
    std::unordered_set<const VariableDependency*> pathSet;

    for (VariableDependency& node : _vars) {
        if (!visited.contains(&node)) {
            if (auto result = findCycle(&node, nullptr, visited, path, pathSet);
                !result.empty()) {
                return result;
            }
        }
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
        if (EdgeMetadata::isMetaEdge(edge->data().type())) {
            continue;
        }

        // handle self-loop
        if (edge->u() == edge->v()) {
            path.pop_back();
            pathSet.erase(curr);
            return {curr};
        }

        VariableDependency* adj = edge->_u == curr ? edge->_v : edge->_u;

        if (adj == prev) {
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

void VariableDependencyGraph::rewriteCycle() {
    auto cyc = getCycle();

    fmt::print("Cycle of length {}:\n", cyc.size());
    for (const VariableDependency* v : cyc) {
        fmt::print("{} ", v->getName());
    }
    fmt::println("");

    bioassert(cyc.front() == cyc.back(), "Invalid cycle.");
    bioassert(cyc.size() >= 3, "Invalid cycle.");

    VariableDependency* head = cyc[0];
    const std::string_view headName = head->getName();

    const std::string fstName = std::string(headName) + std::string {"o"};
    const std::string sndName = std::string(headName) + std::string {"oo"};

    VariableDependency* newHead = newVariable(fstName);
    VariableDependency* newTail = newVariable(sndName);

    VariableDependency::Edges& edges = head->_edges;

    const VariableDependency* nxt = *next(begin(cyc));
    const VariableDependency* prv = *prev(prev(end(cyc)));

    // replace head and tail of loop with new vars
    for (DependencyEdge* e : edges) {
        if (e->u() == nxt && e->v() == head) {
            e->_v = newHead;
        } else if (e->v() == nxt && e->u() == head) {
            e->_u = newHead;
        } else if (e->u() == prv && e->v() == head) {
            e->_v = newTail;
        } else if (e->v() == prv && e->u() == head) {
            e->_u = newTail;
        }
    }

    connect(newHead, head, EdgeMetadata{EdgeMetadata::EdgeType::MERGE});
    connect(newTail, head, EdgeMetadata{EdgeMetadata::EdgeType::MERGE});
}

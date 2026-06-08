#include "VariableDependencyGraph.h"

#include <algorithm>
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
        if (edge->src() == edge->tgt()) {
            path.pop_back();
            pathSet.erase(curr);
            return {curr};
        }

        VariableDependency* adj = edge->_src == curr ? edge->_tgt : edge->_src;

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

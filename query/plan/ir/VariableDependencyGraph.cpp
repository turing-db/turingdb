#include "VariableDependencyGraph.h"

#include <algorithm>
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

VariableDependency* VariableDependencyGraph::getOrCreateVariable(const EntityPattern* entity) {
    bioassert(entity->getDecl(), "Variable with null declaration.");
    const auto match = [entity](const VariableDependency& dep) {
        return entity->getDecl()->getName() == dep.getName();
    };
    const auto foundIt = std::ranges::find_if(_vars, match);
    const bool exists  = foundIt != _vars.end();

    return exists ? &*foundIt : newVariable(entity);
}


#include "VariableDependencyGraph.h"

#include <algorithm>
#include <utility>

#include "EdgePattern.h"
#include "EntityPattern.h"
#include "PatternElement.h"

#include "BioAssert.h"
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

        // NOTE: Edge direction does not matter, as we can get out, in, or both edges
        prev->requiredFor(edgeVar, type);
        edgeVar->requiredFor(tgtVar, type);

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

void VariableDependency::dependsOn(VariableDependency* dep, EdgeMetadata::EdgeType type) {
    const EdgeMetadata data(type);
    this->_incoming.emplace_back(dep, data);
    dep->_outgoing.emplace_back(this, data);
}

void VariableDependency::requiredFor(VariableDependency* dep, EdgeMetadata::EdgeType type) {
    const EdgeMetadata data(type);
    this->_outgoing.emplace_back(dep, data);
    dep->_incoming.emplace_back(this, data);
}

void VariableDependencyGraph::forestify() {
    for (VariableDependency& var : _vars) {
        const VariableDependency::Edges& incoming = var.getIncoming();

        const bool singlePath = incoming.size() == 1;
        if (singlePath) {
            continue;
        }

        [[maybe_unused]] const auto nonMetaEdge = [](const DependencyEdge& e) {
            return EdgeMetadata::isMetaEdge(e.data().type());
        };

        // for (const DependencyEdge& incEdge : incoming) {
        // }
    }
}

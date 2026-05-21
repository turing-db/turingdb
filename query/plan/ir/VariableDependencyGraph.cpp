#include "VariableDependencyGraph.h"

#include <algorithm>

#include "EdgePattern.h"
#include "EntityPattern.h"
#include "PatternElement.h"

#include "BioAssert.h"

using namespace db;

void VariableDependencyGraph::registerPatternElement(const PatternElement* ptn) {
    const EntityPattern* origin = ptn->getRootEntity();

    VariableDependency* originVar = getOrCreateVariable(origin);

    const auto& chain = ptn->getElementChain();

    VariableDependency* prev = originVar;
    for (const auto& [edge, tgt] : chain) {
        const auto* eptn = dynamic_cast<const EdgePattern*>(edge);
        bioassert(eptn, "Invalid edge pattern");

        const auto direction = eptn->getDirection();
        const bool bidirected = direction == EdgePattern::Direction::Undirected;
        const bool outwards = direction == EdgePattern::Direction::Forward;
        const bool inwards = direction == EdgePattern::Direction::Backward;

        VariableDependency* edgeVar = getOrCreateVariable(edge);
        VariableDependency* tgtVar = getOrCreateVariable(tgt);

        // NOTE: Treating bidirectional edges as the leftmost node in pattern being the
        // dependency provider. This may change/be wrong, but has implications on IR
        // generated.
        if (outwards || bidirected) {
            prev->requiredFor(edgeVar);
            edgeVar->requiredFor(tgtVar);
        }
        if (inwards) {
            prev->dependsOn(edgeVar);
            edgeVar->dependsOn(tgtVar);
        }

        prev = tgtVar;
    }
}

VariableDependency* VariableDependencyGraph::newVariable(const EntityPattern* entity) {
    bioassert(entity->getDecl(), "Variable without declaration.");
    return &_vars.emplace_back(entity);
}

VariableDependency* VariableDependencyGraph::getOrCreateVariable(const EntityPattern* entity) {
    bioassert(entity->getDecl(), "Variable with declaration.");
    const auto match = [entity](const VariableDependency& dep) {
        return entity->getDecl() == dep.entity()->getDecl();
    };
    const auto foundIt = std::ranges::find_if(_vars, match);
    const bool exists  = foundIt != _vars.end();

    return exists ? &*foundIt : newVariable(entity);
}

void VariableDependency::dependsOn(VariableDependency* dep) {
    this->_incoming.push_back(dep);
    dep->_outgoing.push_back(this);
}

void VariableDependency::requiredFor(VariableDependency* dep) {
    this->_outgoing.push_back(dep);
    dep->_incoming.push_back(this);
}

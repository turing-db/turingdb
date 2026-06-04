#include "VariableDependencyGraph.h"

#include <algorithm>

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
        VariableDependency* edgeVar = getOrCreateVariable(edge);
        VariableDependency* tgtVar = getOrCreateVariable(tgt);

        // NOTE: Edge direction does not matter, as we can get out, in, or both edges
        prev->requiredFor(edgeVar);
        edgeVar->requiredFor(tgtVar);

        prev = tgtVar;
    }
}

VariableDependency* VariableDependencyGraph::newVariable(const EntityPattern* entity) {
    bioassert(entity->getDecl(), "Variable without declaration.");
    return &_vars.emplace_back(entity->getDecl());
}

VariableDependency* VariableDependencyGraph::getOrCreateVariable(const EntityPattern* entity) {
    bioassert(entity->getDecl(), "Variable with declaration.");
    const auto match = [entity](const VariableDependency& dep) {
        return entity->getDecl() == dep.getDecl();
    };
    const auto foundIt = std::ranges::find_if(_vars, match);
    const bool exists  = foundIt != _vars.end();

    return exists ? &*foundIt : newVariable(entity);
}

void VariableDependency::dependsOn(VariableDependency* dep) {
    this->_incoming.emplace_back(dep);
    dep->_outgoing.emplace_back(this);
}

void VariableDependency::requiredFor(VariableDependency* dep) {
    this->_outgoing.emplace_back(dep);
    dep->_incoming.emplace_back(this);
}

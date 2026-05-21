#include "VariableDependencyGraph.h"

#include <algorithm>
#include <ostream>

#include "EntityPattern.h"
#include "PatternElement.h"
#include "decl/VarDecl.h"

#include "BioAssert.h"

using namespace db;

void VariableDependencyGraph::registerPatternElement(const PatternElement* ptn) {
    const EntityPattern* origin = ptn->getRootEntity();

    VariableDependency* originVar = getOrCreateVariable(origin);

    const auto& chain = ptn->getElementChain();

    for (const auto& [edge, tgt] : chain) {
        VariableDependency* edgeVar = getOrCreateVariable(edge);
        VariableDependency* tgtVar = getOrCreateVariable(tgt);

        originVar->addOutgoing(edgeVar);
        originVar->addOutgoing(tgtVar);
    }
}

VariableDependency* VariableDependencyGraph::newVariable(const EntityPattern* entity) {
    bioassert(entity->getDecl(), "Variable without declaration.");
    return &_vars.emplace_back(entity);
}

void VariableDependencyGraph::dump(std::ostream& out) const {
    out << "VariableDependencyGraph (" << _vars.size() << " nodes):\n";

    for (const VariableDependency& var : _vars) {
        const std::string_view name = var.entity()->getDecl()->getName();
        const bool unnamed = var.entity()->getDecl()->isUnnamed();

        out << "  " << (unnamed ? "<unnamed>" : name);

        if (var.isRoot()) {
            out << " [root]";
        }


        if (!var.getOutgoing().empty()) {
            out << " ->";
            for (const VariableDependency* dep : var.getOutgoing()) {
                const std::string_view depName = dep->entity()->getDecl()->getName();
                const bool depUnnamed = dep->entity()->getDecl()->isUnnamed();
                out << " " << (depUnnamed ? "<unnamed>" : depName);
            }
        }

        out << "\n";
    }
}

VariableDependency* VariableDependencyGraph::getOrCreateVariable(const EntityPattern* entity) {
    bioassert(entity->getDecl(), "Variable with declaration.");
    const auto match = [entity](const VariableDependency& dep) {
        return entity->getDecl() == dep.entity()->getDecl();
    };
    const auto foundIt = std::ranges::find_if(_vars, match);
    const bool exists  = foundIt != end(_vars);

    return exists ? &*foundIt : newVariable(entity);
}

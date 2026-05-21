#include "VariableDependencyGraph.h"

#include <algorithm>
#include <ostream>
#include <unordered_map>

#include "EntityPattern.h"
#include "PatternElement.h"
#include "decl/VarDecl.h"

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

        prev->addOutgoing(edgeVar);
        edgeVar->addOutgoing(tgtVar);

        prev = tgtVar;
    }
}

VariableDependency* VariableDependencyGraph::newVariable(const EntityPattern* entity) {
    bioassert(entity->getDecl(), "Variable without declaration.");
    return &_vars.emplace_back(entity);
}

void VariableDependencyGraph::dump(std::ostream& out) const {
    out << "flowchart TD\n";

    std::unordered_map<const VariableDependency*, size_t> nodeIds;
    for (size_t i = 0; i < _vars.size(); i++) {
        nodeIds[&_vars[i]] = i;
    }

    const auto nodeDef = [&](const VariableDependency* var) {
        const std::string_view name = var->entity()->getDecl()->getName();
        const std::string_view label = name.empty() ? "<unnamed>" : name;
        return "v" + std::to_string(nodeIds.at(var)) + "[\"" + std::string(label) + "\"]";
    };

    for (const VariableDependency& var : _vars) {
        const bool isolated = var.isRoot() && var.getOutgoing().empty();

        if (isolated) {
            out << "    " << nodeDef(&var) << "\n";
        } else {
            for (const VariableDependency* dep : var.getOutgoing()) {
                out << "    " << nodeDef(&var) << " --> " << nodeDef(dep) << "\n";
            }
        }
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

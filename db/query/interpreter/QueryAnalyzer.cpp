#include "QueryAnalyzer.h"

#include <unordered_set>

#include "QueryCommand.h"
#include "SelectField.h"
#include "PathPattern.h"
#include "FromTarget.h"
#include "VarDecl.h"
#include "VarExpr.h"

using namespace db;

namespace {

bool analyzeEntityPattern(DeclContext* declContext, EntityPattern* entity) {
    VarExpr* var = entity->getVar();
    if (var) {
        // Check that the variable has not been already declared
        if (declContext->getDecl(var->getName())) {
            return false;
        }

        // Create the variable declaration in the scope of the command
        VarDecl* decl = VarDecl::create(declContext, var->getName());
        var->setDecl(decl);
    }

    return true;
}

template <class Pattern>
bool analyzeNodeOrEdgePattern(DeclContext* declContext, Pattern* pattern) {
    return analyzeEntityPattern(declContext, pattern->getEntity());
}

bool analyzeSelect(SelectCommand* cmd) {
    // From targets
    for (const FromTarget* target : cmd->fromTargets()) {
        const PathPattern* pattern = target->getPattern();
        const NodePattern* nodeOrigin = pattern->getOrigin();
        if (nodeOrigin) {
            // Just analyze the node pattern for the origin of the path
            if (!analyzeNodeOrEdgePattern(cmd, nodeOrigin)) {
                return false;
            }
        } else {
            // If we have no node origin, then the rest of the path must be empty
            if (!target->elements().empty()) {
                return false;
            }
        }

        for (const PathElement* elem : pattern->getElements()) {
            EdgePattern* edge = elem->getEdge();
            NodePattern* node = elem->getNode();
            if (!edge || !node) {
                return false;
            }

            if (!analyzeNodeOrEdgePattern(cmd, node)) {
                return false;
            }

            if (!analyzeNodeOrEdgePattern(cmd, node)) {
                return false;
            }
        }
    }

    // Select fields
    const auto& selectFields = cmd->selectFields();
    for (const SelectField* field : selectFields) {
        // Check that if we have a star field it is the only select field
        if (field->isAll()) {
            return (selectFields.size() == 1);
        }
    }

    return true;
}

}

QueryAnalyzer::QueryAnalyzer(QueryCommand* cmd)
    : _cmd(cmd)
{
}

QueryAnalyzer::~QueryAnalyzer() {
}

bool QueryAnalyzer::analyze() {
    switch (_cmd->getKind()) {
        case QueryCommand::QCOM_LIST_COMMAND:
            return true;
        break;

        case QueryCommand::QCOM_OPEN_COMMAND:
            return true;
        break;

        case QueryCommand::QCOM_SELECT_COMMAND:
            return analyzeSelect(static_cast<SelectCommand*>(_cmd));
        break;

        default:
        return false;
    }

    return true;
}

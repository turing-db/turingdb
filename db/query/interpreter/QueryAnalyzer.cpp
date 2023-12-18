#include "QueryAnalyzer.h"

#include <unordered_set>

#include "QueryCommand.h"
#include "SelectField.h"
#include "PathPattern.h"
#include "FromTarget.h"
#include "VarDecl.h"
#include "Expr.h"
#include "DeclContext.h"

using namespace db;

QueryAnalyzer::QueryAnalyzer(ASTContext* ctxt)
    : _ctxt(ctxt)
{
}

QueryAnalyzer::~QueryAnalyzer() {
}

bool QueryAnalyzer::analyze(QueryCommand* cmd) {
    switch (cmd->getKind()) {
        case QueryCommand::QCOM_LIST_COMMAND:
            return true;
        break;

        case QueryCommand::QCOM_OPEN_COMMAND:
            return true;
        break;

        case QueryCommand::QCOM_SELECT_COMMAND:
            return analyzeSelect(static_cast<SelectCommand*>(cmd));
        break;

        default:
        return false;
    }

    return true;
}

bool QueryAnalyzer::analyzeSelect(SelectCommand* cmd) {
    // From targets
    DeclContext* declContext = cmd->getDeclContext();
    for (const FromTarget* target : cmd->fromTargets()) {
        const PathPattern* pattern = target->getPattern();
        const NodePattern* nodeOrigin = pattern->getOrigin();
        if (nodeOrigin) {
            // Just analyze the node pattern for the origin of the path
            if (!analyzeNodeOrEdgePattern(declContext, nodeOrigin)) {
                return false;
            }
        } else {
            // If we have no node origin, then the rest of the path must be empty
            if (!pattern->elements().empty()) {
                return false;
            }
        }

        for (const PathElement* elem : pattern->elements()) {
            EdgePattern* edge = elem->getEdge();
            NodePattern* node = elem->getNode();
            if (!edge || !node) {
                return false;
            }

            if (!analyzeNodeOrEdgePattern(declContext, node)) {
                return false;
            }

            if (!analyzeNodeOrEdgePattern(declContext, edge)) {
                return false;
            }
        }
    }

    // Select fields
    const auto& selectFields = cmd->selectFields();
    for (SelectField* field : selectFields) {
        // Check that if we have a star field it is the only select field
        if (field->isAll()) {
            return (selectFields.size() == 1);
        } else {
            const auto& name = field->getName();
            // Check that the variable exists in the declContext
            VarDecl* decl = declContext->getDecl(name);
            if (!decl) {
                return false;
            }

            field->setDecl(decl);
        }
    }

    return true;
}

template <class Pattern>
bool QueryAnalyzer::analyzeNodeOrEdgePattern(DeclContext* declContext, Pattern* pattern) {
    return analyzeEntityPattern(declContext, pattern->getEntity());
}

bool QueryAnalyzer::analyzeEntityPattern(DeclContext* declContext, EntityPattern* entity) {
    VarExpr* var = entity->getVar();
    if (!var) {
        return true;
    }

    // Check that the variable has not been already declared
    if (declContext->getDecl(var->getName())) {
        return false;
    }

    // Create the variable declaration in the scope of the command
    VarDecl* decl = VarDecl::create(_ctxt, declContext, var->getName());
    if (!decl) {
        return false;
    }

    var->setDecl(decl);

    return true;
}
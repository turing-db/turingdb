#include "QueryAnalyzer.h"

#include "QueryCommand.h"
#include "SelectField.h"
#include "PathPattern.h"
#include "FromTarget.h"
#include "VarDecl.h"
#include "Expr.h"
#include "DeclContext.h"
#include "SelectProjection.h"

using namespace db;

namespace {

void selectAllVariables(SelectCommand* cmd) {
    for (const FromTarget* target : cmd->fromTargets()) {
        const PathPattern* pattern = target->getPattern();
        for (EntityPattern* entityPattern : pattern->elements()) {
            if (VarExpr* var = entityPattern->getVar()) {
                if (VarDecl* decl = var->getDecl()) {
                    decl->setSelected(true);
                }
            }
        }
    }
}

}

QueryAnalyzer::QueryAnalyzer(ASTContext* ctxt)
    : _ctxt(ctxt)
{
}

QueryAnalyzer::~QueryAnalyzer() {
}

bool QueryAnalyzer::analyze(QueryCommand* cmd) {
    switch (cmd->getKind()) {
        case QueryCommand::Kind::SELECT_COMMAND:
            return analyzeSelect(static_cast<SelectCommand*>(cmd));
        break;

        case QueryCommand::Kind::CREATE_GRAPH_COMMAND:
            return analyzeCreateGraph(static_cast<CreateGraphCommand*>(cmd));
        break;

        case QueryCommand::Kind::LIST_GRAPH_COMMAND:
            return true;
        break;

        case QueryCommand::Kind::LOAD_GRAPH_COMMAND:
            return analyzeLoadGraph(static_cast<LoadGraphCommand*>(cmd));
        break;

        case QueryCommand::Kind::EXPLAIN_COMMAND:
            return analyzeExplain(static_cast<ExplainCommand*>(cmd));
        break;

        default:
        return false;
    }

    return true;
}

bool QueryAnalyzer::analyzeExplain(ExplainCommand* cmd) {
    return analyze(cmd->getQuery());
}

bool QueryAnalyzer::analyzeCreateGraph(CreateGraphCommand* cmd) {
    const std::string& name = cmd->getName();
    if (name.empty()) {
        return false;
    }

    // Check that the graph name is only [A-Z0-9_]+
    for (char c : name) {
        if (!(isalnum(c) || c == '_')) {
            return false;
        }
    }

    return true;
}

bool QueryAnalyzer::analyzeSelect(SelectCommand* cmd) {
    SelectProjection* proj = cmd->getProjection();
    if (!proj) {
        return false;
    }

    // From targets
    DeclContext* declContext = cmd->getDeclContext();
    for (const FromTarget* target : cmd->fromTargets()) {
        const PathPattern* pattern = target->getPattern();
        for (EntityPattern* entityPattern : pattern->elements()) {
            if (!analyzeEntityPattern(declContext, entityPattern)) {
                return false;
            }
        }
    }

    // Select fields
    const auto& selectFields = proj->selectFields();
    bool selectAll = false;
    for (SelectField* field : selectFields) {
        if (field->isAll()) {
            selectAll = true;
            continue;
        } else {
            const auto& name = field->getName();
            // Check that the variable exists in the declContext
            VarDecl* decl = declContext->getDecl(name);
            if (!decl) {
                return false;
            }

            decl->setSelected(true);
            field->setDecl(decl);
        }
    }

    // At this point: a declaration has been created for each variable
    // in each pattern and select fields are connected to the var decl

    if (selectAll) {
        selectAllVariables(cmd);
    }

    return true;
}

bool QueryAnalyzer::analyzeEntityPattern(DeclContext* declContext,
                                         EntityPattern* entity) {
    VarExpr* var = entity->getVar();
    if (!var) {
        var = VarExpr::create(_ctxt, createVarName());
        entity->setVar(var);
    }

    // Create the variable declaration in the scope of the command
    VarDecl* decl = VarDecl::create(_ctxt, declContext, var->getName());
    if (!decl) {
        return false;
    }

    var->setDecl(decl);

    return true;
}

std::string QueryAnalyzer::createVarName() {
    return std::to_string(_nextNewVarID++);
}

bool QueryAnalyzer::analyzeLoadGraph(LoadGraphCommand* cmd) {
    const std::string& name = cmd->getName();
    if (name.empty()) {
        return false;
    }

    // Check that the graph name is only [A-Z0-9_]+
    for (char c : name) {
        if (!(isalnum(c) || c == '_')) {
            return false;
        }
    }

    return true;
}

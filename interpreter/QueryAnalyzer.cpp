#include "QueryAnalyzer.h"

#include <range/v3/view.hpp>

#include "metadata/PropertyTypeMap.h"
#include "DeclContext.h"
#include "Expr.h"
#include "MatchTarget.h"
#include "PathPattern.h"
#include "QueryCommand.h"
#include "ReturnField.h"
#include "ASTContext.h"
#include "ReturnProjection.h"
#include "VarDecl.h"
#include "BioAssert.h"

using namespace db;
namespace rv = ranges::views;

namespace {

void returnAllVariables(MatchCommand* cmd) {
    for (const MatchTarget* target : cmd->matchTargets()) {
        const PathPattern* pattern = target->getPattern();
        for (EntityPattern* entityPattern : pattern->elements()) {
            if (VarExpr* var = entityPattern->getVar()) {
                if (VarDecl* decl = var->getDecl()) {
                    decl->setReturned(true);
                }
            }
        }
    }
}

}

QueryAnalyzer::QueryAnalyzer(ASTContext* ctxt, const PropertyTypeMap& propTypeMap)
    : _ctxt(ctxt),
    _propTypeMap(propTypeMap)
{
}

QueryAnalyzer::~QueryAnalyzer() {
}
bool QueryAnalyzer::analyze(QueryCommand* cmd) {
    switch (cmd->getKind()) {
        case QueryCommand::Kind::MATCH_COMMAND:
            return analyzeMatch(static_cast<MatchCommand*>(cmd));
        break;

        case QueryCommand::Kind::CREATE_COMMAND:
            return analyzeCreate(static_cast<CreateCommand*>(cmd));
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

        case QueryCommand::Kind::HISTORY_COMMAND:
        case QueryCommand::Kind::CHANGE_COMMAND:
            return true;
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

bool QueryAnalyzer::analyzeMatch(MatchCommand* cmd) {
    ReturnProjection* proj = cmd->getProjection();
    if (!proj) {
        return false;
    }

    // match targets
    DeclContext* declContext = cmd->getDeclContext();
    for (const MatchTarget* target : cmd->matchTargets()) {
        const PathPattern* pattern = target->getPattern();
        const auto& elements = pattern->elements();

        EntityPattern* entityPattern = elements[0];
        entityPattern->setKind(DeclKind::NODE_DECL);
        if (!analyzeEntityPattern(declContext, entityPattern)) {
            return false;
        }

        if (elements.size() >= 2) {
            bioassert(elements.size() >= 3);
            for (auto triple : elements | rv::drop(1) | rv::chunk(2)) {
                EntityPattern* edge = triple[0];
                EntityPattern* target = triple[1];

                edge->setKind(DeclKind::EDGE_DECL);
                target->setKind(DeclKind::NODE_DECL);

                if (!analyzeEntityPattern(declContext, edge)) {
                    return false;
                }

                if (!analyzeEntityPattern(declContext, target)) {
                    return false;
                }
            }
        }
    }

    // Return fields
    const auto& returnFields = proj->returnFields();
    bool returnAll = false;
    for (ReturnField* field : returnFields) {
        if (field->isAll()) {
            returnAll = true;
            continue;
        } else {
            const auto& name = field->getName();
            // Check that the variable exists in the declContext
            VarDecl* decl = declContext->getDecl(name);
            if (!decl) {
                return false;
            }

            decl->setReturned(true);
            field->setDecl(decl);
        }
    }

    // At this point: a declaration has been created for each variable
    // in each pattern and return fields are connected to the var decl

    if (returnAll) {
        returnAllVariables(cmd);
    }

    return true;
}

bool QueryAnalyzer::analyzeCreate(CreateCommand* cmd) {
    DeclContext* declContext = cmd->getDeclContext();
    const auto& targets = cmd->createTargets();
    for (const CreateTarget* target : targets) {
        const PathPattern* pattern = target->getPattern();
        const auto& elements = pattern->elements();

        EntityPattern* entityPattern = elements[0];
        if (elements.empty()) {
            return false;
        }

        entityPattern->setKind(DeclKind::NODE_DECL);
        if (!analyzeEntityPattern(declContext, entityPattern)) {
            return false;
        }

        if (elements.size() >= 2) {
            bioassert(elements.size() >= 3);
            for (auto triple : elements | rv::drop(1) | rv::chunk(2)) {
                EntityPattern* edge = triple[0];
                EntityPattern* target = triple[1];

                edge->setKind(DeclKind::EDGE_DECL);
                target->setKind(DeclKind::NODE_DECL);

                if (!analyzeEntityPattern(declContext, edge)) {
                    return false;
                }

                if (!analyzeEntityPattern(declContext, target)) {
                    return false;
                }
            }
        }
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
    VarDecl* decl = VarDecl::create(_ctxt,
            declContext,
            var->getName(),
            entity->getKind(),
            entity->getEntityID());
    if (!decl) {
        // decl already exists from prev targets
        decl = declContext->getDecl(var->getName());
        if (decl->getEntityID() != entity->getEntityID()) {
            return false;
        }
    }

    if (auto* exprConstraint = entity->getExprConstraint()) {
        for (auto* binExpr : exprConstraint->getExpressions()) {
            if (binExpr->getOpType() != BinExpr::OP_EQUAL) {
                return false;
            }
        }
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

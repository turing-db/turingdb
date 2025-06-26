#include "QueryAnalyzer.h"

#include <optional>
#include <range/v3/view.hpp>

#include "AnalyzeException.h"
#include "Profiler.h"
#include "metadata/PropertyType.h"
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


#include "reader/GraphReader.h"

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

QueryAnalyzer::QueryAnalyzer(const GraphView& view,
                             ASTContext* ctxt,
                             const PropertyTypeMap& propTypeMap) 
    : _view(view),
    _ctxt(ctxt),
    _propTypeMap(propTypeMap)
{
}

QueryAnalyzer::~QueryAnalyzer() {
}

bool QueryAnalyzer::analyze(QueryCommand* cmd) {
    Profile profile {"QueryAnalyzer::analyze"};

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
        case QueryCommand::Kind::COMMIT_COMMAND:
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
    bool isCreate{false}; // Flag to distinguish create and match commands
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
        if (!analyzeEntityPattern(declContext, entityPattern, isCreate)) {
            return false;
        }

        if (elements.size() >= 2) {
            bioassert(elements.size() >= 3);
            for (auto pair : elements | rv::drop(1) | rv::chunk(2)) {
                EntityPattern* edge = pair[0];
                EntityPattern* target = pair[1];

                edge->setKind(DeclKind::EDGE_DECL);
                target->setKind(DeclKind::NODE_DECL);

                if (!analyzeEntityPattern(declContext, edge, isCreate)) {
                    return false;
                }

                if (!analyzeEntityPattern(declContext, target, isCreate)) {
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
            const auto& memberName = field->getMemberName();

            // If returning a member, get (and check) its type
            if (!memberName.empty()) {
                const auto propTypeRes = _propTypeMap.get(memberName);
                if (!propTypeRes) {
                    throw AnalyzeException("Property type not found for property member \""
                                           + field->getMemberName() + "\"");
                }

                const auto propType = propTypeRes.value();
                field->setMemberType(propType);
            }
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
    bool isCreate{true}; // Flag to distinguish create and match commands
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
        if (!analyzeEntityPattern(declContext, entityPattern, isCreate)) {
            return false;
        }

        if (elements.size() >= 2) {
            bioassert(elements.size() >= 3);
            for (auto pair : elements | rv::drop(1) | rv::chunk(2)) {
                EntityPattern* edge = pair[0];
                EntityPattern* target = pair[1];

                edge->setKind(DeclKind::EDGE_DECL);
                target->setKind(DeclKind::NODE_DECL);

                if (!analyzeEntityPattern(declContext, edge,isCreate)) {
                    return false;
                }

                if (!analyzeEntityPattern(declContext, target, isCreate)) {
                    return false;
                }
            }
        }
    }
    return true;
}

bool QueryAnalyzer::typeCheckBinExprConstr(const ValueType lhs, const ValueType rhs) {
    // FIXME: Should there be non-equal types that are allowed to be compared?
    // (e.g. int64 and uint64)
    if (lhs != rhs) {
        return false;
    }
    return true;
}

bool QueryAnalyzer::analyzeBinExprConstraint(const BinExpr* binExpr,
                                             bool isCreate) {
        // Currently only support equals
        if (binExpr->getOpType() != BinExpr::OP_EQUAL) {
            throw AnalyzeException("Unsupported operator");
        }

        // XXX: Assumes that variable is left operand, constant is right operand
        const VarExpr* leftOperand =
            static_cast<VarExpr*>(binExpr->getLeftExpr());
        const ExprConst* rightOperand =
            static_cast<ExprConst*>(binExpr->getRightExpr());

        // Query graph for name and type of variable
        const std::string& lhsName = leftOperand->getName();
        const GraphReader reader = _view.read();
        const auto lhsPropTypeOpt = reader.getMetadata().propTypes().get(lhsName);

        // Property type does not exist
        if (lhsPropTypeOpt == std::nullopt) {
            // If this is a match query: error
            if (!isCreate) {
                throw AnalyzeException("Variable '" +
                                       lhsName +
                                       "' has invalid property type");
            }
            else { // If a create query: no need to type check
                return true;
            }
        }

        // Else: property type exists: get types and type check
        const PropertyType lhsPropType = lhsPropTypeOpt.value();
        // NOTE: Directly accessing struct member
        const ValueType lhsType = lhsPropType._valueType; 

        const ValueType rhsType = rightOperand->getType();

        if (!QueryAnalyzer::typeCheckBinExprConstr(lhsType, rhsType)) {
            std::string varTypeName = std::string(ValueTypeName::value(lhsType));
            std::string exprTypeName = std::string(ValueTypeName::value(rhsType));
            std::string verb = isCreate ? "assigned" : "compared to";
            throw AnalyzeException(
                                   "Variable '" + lhsName +
                                   "' of type " + varTypeName +
                                   " cannot be " + verb +
                                   " value of type " +
                                   exprTypeName
            );
        }
    return true;
}

bool QueryAnalyzer::analyzeEntityPattern(DeclContext* declContext,
                                         EntityPattern* entity,
                                         bool isCreate) {
    VarExpr* var = entity->getVar();
    // Handle the case where the entity is unlabeled edge (--)
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
        VarDecl* existingDecl = declContext->getDecl(var->getName());
        if (existingDecl->getEntityID() != entity->getEntityID()) {
            return false;
        }
    }

    auto* exprConstraint = entity->getExprConstraint();
    // If there are no constraints, no need to type check
    if (!exprConstraint) {
        var->setDecl(decl);
        return true;
    }

    // Otherwise, verify all constraints are valid
    const auto binExprs = exprConstraint->getExpressions();
    for (const BinExpr* binExpr : binExprs) {
        if (!QueryAnalyzer::analyzeBinExprConstraint(binExpr, isCreate)) {
          return false;
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

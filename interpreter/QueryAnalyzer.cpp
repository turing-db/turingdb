#include "QueryAnalyzer.h"

#include <cstdint>
#include <optional>
#include <range/v3/view.hpp>
#include <range/v3/action/sort.hpp>
#include <range/v3/algorithm/adjacent_find.hpp>

#include "AnalyzeException.h"
#include "Profiler.h"
#include "metadata/PropertyType.h"
#include "reader/GraphReader.h"
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

QueryAnalyzer::QueryAnalyzer(const GraphView& view, ASTContext* ctxt)
    : _view(view),
    _ctxt(ctxt)
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

// Checks if a match target contains multiple variables
void QueryAnalyzer::ensureMatchVarsUnique(const MatchTarget* target) {
    using PathElements = std::vector<EntityPattern*>;

    const PathPattern* ptn = target->getPattern();

    const PathElements& elements = ptn->elements();

    std::vector<std::string> varNames;
    for (auto&& e : elements) {
        if (e && e->getVar()) {
            varNames.push_back(e->getVar()->getName());
        }
    }
    std::ranges::sort(varNames);

    // Adjacent find returns an iterator to the first occurences of two consecutive
    // identical elements. Since the vector is sorted, identical variable names are
    // contiguous, and thus if adjacent_find returns no such two consecutive
    // identical elements, all variables must be unique.
    const auto duplicateVarIt = ranges::adjacent_find(varNames);
    const bool hasDuplicate = duplicateVarIt != std::end(varNames);

    if (hasDuplicate) [[unlikely]] {
        const std::string duplicateVarName = *duplicateVarIt;
        throw AnalyzeException(fmt::format(
            "Variable {} occurs multiple times in MATCH query",
            duplicateVarName));
    }
}

bool QueryAnalyzer::analyzeMatch(MatchCommand* cmd) {
    bool isCreate {false}; // Flag to distinguish create and match commands
    ReturnProjection* proj = cmd->getProjection();
    if (!proj) {
        return false;
    }

    // match targets
    DeclContext* declContext = cmd->getDeclContext();
    for (const MatchTarget* target : cmd->matchTargets()) {
        const PathPattern* pattern = target->getPattern();
        if (pattern == nullptr) {
            throw new AnalyzeException("Match target path pattern not found");
        }

        ensureMatchVarsUnique(target);
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

            const auto& propTypeMap = _view.metadata().propTypes();
            // If returning a member, get (and check) its type
            if (!memberName.empty()) {
                const auto propTypeRes = propTypeMap.get(memberName);
                if (!propTypeRes) {
                    throw AnalyzeException(
                        "Property type not found for property member \""
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
    bool isCreate {true}; // Flag to distinguish create and match commands
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

                if (!analyzeEntityPattern(declContext, edge, isCreate)) {
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

bool QueryAnalyzer::typeCheckBinExprConstr(const PropertyType lhs,
                                           const ExprConst* rhs) {
    // NOTE: Directly accessing struct member
    const ValueType lhsType = lhs._valueType;
    const ValueType rhsType = rhs->getType();

    if (lhsType == rhsType) {
        return true;
    } else if (lhsType == ValueType::UInt64 && rhsType == ValueType::Int64) {
        const auto rhsI64 = static_cast<const Int64ExprConst*>(rhs);
        const int64_t rhsValue = rhsI64->getVal();
        // Ensure the Int64 is not out of UInt64 range
        if (rhsValue >= 0) {
            return true;
        }
    }
    return false;
}

bool QueryAnalyzer::analyzeBinExprConstraint(const BinExpr* binExpr,
                                             bool isCreate) {
    // Currently only support equals
    if (binExpr->getOpType() != BinExpr::OP_EQUAL) {
        throw AnalyzeException("Unsupported operator");
    }

    // Assumes that variable is left operand, constant is right operand
    const VarExpr* lhsExpr =
        static_cast<VarExpr*>(binExpr->getLeftExpr());
    const ExprConst* rhsExpr =
        static_cast<ExprConst*>(binExpr->getRightExpr());

    // Query graph for name and type of variable
    const std::string& lhsName = lhsExpr->getName();
    const GraphReader reader = _view.read();
    const auto lhsPropTypeOpt = reader.getMetadata().propTypes().get(lhsName);

    // Property type does not exist
    if (lhsPropTypeOpt == std::nullopt) {
        // If this is a match query: error
        if (!isCreate) [[unlikely]] {
            throw AnalyzeException("Variable '" + lhsName +
                                   "' has invalid property type");
        } else { // If a create query: no need to type check
            return true;
        }
    }

    // If property type exists: get types and type check
    const PropertyType lhsPropType = lhsPropTypeOpt.value();

    if (!QueryAnalyzer::typeCheckBinExprConstr(lhsPropType, rhsExpr)) [[unlikely]] {
        const ValueType lhsType = lhsPropType._valueType;
        const ValueType rhsType = rhsExpr->getType();
        const std::string varTypeName = std::string(ValueTypeName::value(lhsType));
        const std::string exprTypeName = std::string(ValueTypeName::value(rhsType));
        const std::string verb = isCreate ? "assigned" : "compared to";
        throw AnalyzeException("Variable '" + lhsName + "' of type " + varTypeName +
                               " cannot be " + verb + " value of type " + exprTypeName);
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
    if (exprConstraint == nullptr) {
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

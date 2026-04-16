#include "ExprDependencies.h"

#include "FunctionInvocation.h"
#include "PlanGraph.h"
#include "PlannerException.h"
#include "PlanGraphTopology.h"
#include "decl/VarDecl.h"
#include "expr/ExprChain.h"
#include "expr/SymbolExpr.h"
#include "nodes/PlanGraphNode.h"
#include "nodes/VarNode.h"

#include "expr/BinaryExpr.h"
#include "expr/Expr.h"
#include "expr/EntityTypeExpr.h"
#include "expr/PropertyExpr.h"
#include "expr/StringExpr.h"
#include "expr/UnaryExpr.h"
#include "expr/FunctionInvocationExpr.h"
#include "expr/IndexExpr.h"
#include "expr/ListExpr.h"

#include "BioAssert.h"

using namespace db;

ExprDependencies::ExprDependencies()
{
}

ExprDependencies::~ExprDependencies() {
}

void ExprDependencies::genExprDependencies(PlanGraphVariables& variables, Expr* expr) {
    switch (expr->getKind()) {
        case Expr::Kind::BINARY: {
            const BinaryExpr* binary = static_cast<BinaryExpr*>(expr);
            genExprDependencies(variables, binary->getLHS());
            genExprDependencies(variables, binary->getRHS());
        } break;

        case Expr::Kind::UNARY: {
            const UnaryExpr* unary = static_cast<UnaryExpr*>(expr);
            genExprDependencies(variables, unary->getSubExpr());
        } break;

        case Expr::Kind::STRING: {
            const StringExpr* string = static_cast<StringExpr*>(expr);
            genExprDependencies(variables, string->getLHS());
            genExprDependencies(variables, string->getRHS());
        } break;

        case Expr::Kind::ENTITY_TYPES: {
            const EntityTypeExpr* entityType = static_cast<EntityTypeExpr*>(expr);
            PlanGraphNode* rawVar = variables.getProducer(entityType->getEntityVarDecl());
            bioassert(rawVar, "VarDecl not found");

            auto* var = dynamic_cast<VarNode*>(rawVar);
            if (!var) {
                throw PlannerException("Can only reference entity types from matched variables");
            }

            _varDeps.emplace_back(var, expr);
        } break;

        case Expr::Kind::PROPERTY: {
            const PropertyExpr* prop = static_cast<PropertyExpr*>(expr);

            if (prop->isStringTableHeaderAccess()) {
                break;
            }

            {
                // Check if a variable is produced by a processor such as @ref
                // PathExplorerProcessor which does not produce a single entity column
                const VarDecl* var = prop->getEntityVarDecl();
                const PlanGraphNode* producer = variables.getProducer(var);
                const PlanGraphOpcode code = producer->getOpcode();

                const bool isFromExplorer = code == PlanGraphOpcode::PATH_EXPLORER;
                if (isFromExplorer) {
                    bioassert(var, "Null var.");
                    const std::string_view varName = var->getName();
                    std::string err =
                        fmt::format("Fetching properties from an arbitrary path "
                                    "({}) is not yet supported.",
                                    varName);
                    throw PlannerException(std::move(err));
                }

                // Check that the producer is a VarNode: well defined column to get
                // properties from
                const bool isFromVar = code == PlanGraphOpcode::VAR;
                if (!isFromVar) {
                    bioassert(var, "Null var.");
                    const std::string_view varName = var->getName();
                    std::string err = fmt::format(
                        "Cannot fetch property from ambiguously sourced variable {}.",
                        varName);
                    throw PlannerException(std::move(err));
                }
            }

            PlanGraphNode* producer = variables.getProducer(prop->getEntityVarDecl());
            bioassert(producer, "Variable source not found.");

            auto* p = dynamic_cast<VarNode*>(producer);
            bioassert(p, "Failed to validate VarNode.");

            _varDeps.emplace_back(p, expr);
        } break;

        case Expr::Kind::FUNCTION_INVOCATION: {
            const FunctionInvocationExpr* func = static_cast<FunctionInvocationExpr*>(expr);
            const ExprChain* arguments = func->getFunctionInvocation()->getArguments();

            for (Expr* arg : *arguments) {
                genExprDependencies(variables, arg);
            }

            _funcDeps.emplace_back(func);
        } break;

        case Expr::Kind::SYMBOL: {
            const SymbolExpr* symbol = static_cast<SymbolExpr*>(expr);
            PlanGraphNode* producer = variables.getProducer(symbol->getDecl());
            bioassert(producer, "VarDecl not found");

            _varDeps.emplace_back(producer, expr);
        } break;

        case Expr::Kind::PATH:
            // throwError("Path expression not supported yet", expr);
            // TODO Find a way to get access to throwError
            throw PlannerException("Path expression not supported yet");
            break;

        case Expr::Kind::LITERAL:
            // Reached end
            break;

        case Expr::Kind::INDEX: {
            const IndexExpr* index = static_cast<IndexExpr*>(expr);
            genExprDependencies(variables, index->getBase());
            genExprDependencies(variables, index->getIndexExpr());
        } break;

        case Expr::Kind::LIST: {
            const ListExpr* list = static_cast<const ListExpr*>(expr);
            for (Expr* elem : list->getElements()) {
                genExprDependencies(variables, elem);
            }
        } break;

        case Expr::Kind::_SIZE:
            throw PlannerException("Unknown expression type in ExprDependencies.");
        break;

    }
}

VarNode* ExprDependencies::findCommonSuccessor(PlanGraphTopology* topology, VarNode* var) const {
    for (const VarDependency& dep : _varDeps) {
        PlanGraphNode* successor = topology->findCommonSuccessor(var, dep._producerNode);

        if (successor) {
            var = topology->findNextVar(successor);

            if (!var) [[unlikely]] {
                throw PlannerException("Unknown error. Cannot find a common successor");
            }
        }
    }

    return var;
}

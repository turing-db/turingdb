#pragma once

#include "FunctionInvocation.h"
#include "PlanGraphVariables.h"
#include "PlannerException.h"
#include "PlanGraphTopology.h"
#include "expr/ExprChain.h"
#include "expr/SymbolExpr.h"
#include "nodes/VarNode.h"

#include "expr/BinaryExpr.h"
#include "expr/Expr.h"
#include "expr/EntityTypeExpr.h"
#include "expr/PropertyExpr.h"
#include "expr/StringExpr.h"
#include "expr/UnaryExpr.h"
#include "expr/FunctionInvocationExpr.h"

#include "BioAssert.h"

namespace db {

class ExprDependencies {
public:
    struct VarDependency {
        PlanGraphNode* _producerNode {nullptr};
        Expr* _expr {nullptr};
    };

    struct FuncDependency {
        const FunctionInvocationExpr* _expr {nullptr};
    };

    using VarDepVector = std::vector<VarDependency>;
    using FuncDepVector = std::vector<FuncDependency>;

    const VarDepVector& getVarDeps() const {
        return _varDeps;
    }

    VarDepVector& getVarDeps() {
        return _varDeps;
    }

    const FuncDepVector& getFuncDeps() const {
        return _funcDeps;
    }

    bool empty() const {
        return _varDeps.empty() && _funcDeps.empty();
    }

    void genExprDependencies(PlanGraphVariables& variables, Expr* expr) {
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
                PlanGraphNode* producer = variables.getProducer(prop->getEntityVarDecl());
                bioassert(producer, "VarDecl not found");

                auto* p = dynamic_cast<VarNode*>(producer);
                if (!p) {
                    throw PlannerException("Can only reference properties from matched variables");
                }

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
        }
    }

    VarNode* findCommonSuccessor(PlanGraphTopology* topology, VarNode* var) const {
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

private:
    VarDepVector _varDeps;
    FuncDepVector _funcDeps;
};

}

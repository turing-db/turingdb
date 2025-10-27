#pragma once

#include "PlanGraphVariables.h"
#include "PlannerException.h"
#include "PlanGraphTopology.h"
#include "expr/SymbolExpr.h"
#include "nodes/VarNode.h"

#include "expr/BinaryExpr.h"
#include "expr/Expr.h"
#include "expr/EntityTypeExpr.h"
#include "expr/PropertyExpr.h"
#include "expr/StringExpr.h"
#include "expr/UnaryExpr.h"
#include "expr/FunctionInvocationExpr.h"

namespace db::v2 {

class ExprDependencies {
public:
    struct VarDependency {
        VarNode* _var {nullptr};
        const Expr* _expr {nullptr};
    };

    struct FuncDependency {
        const FunctionInvocationExpr* _expr {nullptr};
    };

    using VarDepVector = std::vector<VarDependency>;
    using FuncDepVector = std::vector<FuncDependency>;

    const VarDepVector& getVarDeps() const {
        return _varDeps;
    }

    const FuncDepVector& getFuncDeps() const {
        return _funcDeps;
    }

    void genExprDependencies(const PlanGraphVariables& variables, const Expr* expr) {
        switch (expr->getKind()) {
            case Expr::Kind::BINARY: {
                const BinaryExpr* binary = static_cast<const BinaryExpr*>(expr);
                genExprDependencies(variables, binary->getLHS());
                genExprDependencies(variables, binary->getRHS());
            } break;

            case Expr::Kind::UNARY: {
                const UnaryExpr* unary = static_cast<const UnaryExpr*>(expr);
                genExprDependencies(variables, unary->getSubExpr());
            } break;

            case Expr::Kind::STRING: {
                const StringExpr* string = static_cast<const StringExpr*>(expr);
                genExprDependencies(variables, string->getLHS());
                genExprDependencies(variables, string->getRHS());
            } break;

            case Expr::Kind::ENTITY_TYPES: {
                const EntityTypeExpr* entityType = static_cast<const EntityTypeExpr*>(expr);
                _varDeps.emplace_back(variables.getVarNode(entityType->getDecl()), expr);
            } break;

            case Expr::Kind::PROPERTY: {
                const PropertyExpr* prop = static_cast<const PropertyExpr*>(expr);
                _varDeps.emplace_back(variables.getVarNode(prop->getDecl()), expr);
            } break;

            case Expr::Kind::FUNCTION_INVOCATION: {
                const FunctionInvocationExpr* func = static_cast<const FunctionInvocationExpr*>(expr);
                _funcDeps.emplace_back(func);
            } break;

            case Expr::Kind::SYMBOL: {
                const SymbolExpr* symbol = static_cast<const SymbolExpr*>(expr);
                _varDeps.emplace_back(variables.getVarNode(symbol->getDecl()), expr);
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
            PlanGraphNode* successor = topology->findCommonSuccessor(var, dep._var);

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

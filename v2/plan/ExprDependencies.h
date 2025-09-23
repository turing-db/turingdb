#pragma once

#include <variant>
#include <unordered_set>

#include "metadata/LabelSet.h"

#include "PlanGraphVariables.h"
#include "PlannerException.h"

#include "expr/BinaryExpr.h"
#include "expr/Expr.h"
#include "expr/NodeLabelExpr.h"
#include "expr/PropertyExpr.h"
#include "expr/StringExpr.h"
#include "expr/UnaryExpr.h"

namespace db::v2 {

class ExprDependencies {
public:
    struct LabelDependency {
        const LabelSet* _labelSet {nullptr};
    };

    struct PropertyDependency {
        const PropertyTypeID _propTypeID;
    };

    using Dependency = std::variant<LabelDependency, PropertyDependency>;

    struct ExprDependency {
        VarNode* _var {nullptr};
        Dependency _dep;

        struct Hasher {
            size_t operator()(const ExprDependency& v) const {
                return std::hash<const VarNode*> {}(v._var);
            }
        };

        struct Predicate {
            bool operator()(const ExprDependency& a, const ExprDependency& b) const {
                return a._var == b._var;
            }
        };
    };

    using Container = std::unordered_set<ExprDependency, ExprDependency::Hasher, ExprDependency::Predicate>;

    const Container& getDependencies() const {
        return _dependencies;
    }

    void genExprDependencies(const PlanGraphVariables& variables, const Expr* expr) {
        switch (expr->getKind()) {
            case Expr::Kind::BINARY:
                genExprDependencies(variables, static_cast<const BinaryExpr*>(expr));
                break;

            case Expr::Kind::UNARY:
                genExprDependencies(variables, static_cast<const UnaryExpr*>(expr));
                break;

            case Expr::Kind::STRING:
                genExprDependencies(variables, static_cast<const StringExpr*>(expr));
                break;

            case Expr::Kind::NODE_LABEL:
                genExprDependencies(variables, static_cast<const NodeLabelExpr*>(expr));
                break;

            case Expr::Kind::PROPERTY:
                genExprDependencies(variables, static_cast<const PropertyExpr*>(expr));
                break;

            case Expr::Kind::PATH:
                // throwError("Path expression not supported yet", expr);
                // TODO Find a way to get access to throwError
                throw PlannerException("Path expression not supported yet");
                break;

            case Expr::Kind::SYMBOL:
                // throwError("Symbol expression not supported yet", expr);
                // TODO Find a way to get access to throwError
                throw PlannerException("Symbol expression not supported yet");
                break;

            case Expr::Kind::LITERAL:
                // Reached end
                break;
        }
    }

private:
    Container _dependencies;

    void genExprDependencies(const PlanGraphVariables& variables, const BinaryExpr* expr) {
        genExprDependencies(variables, expr->getLHS());
        genExprDependencies(variables, expr->getRHS());
    }

    void genExprDependencies(const PlanGraphVariables& variables, const UnaryExpr* expr) {
        genExprDependencies(variables, expr->getSubExpr());
    }

    void genExprDependencies(const PlanGraphVariables& variables, const StringExpr* expr) {
        genExprDependencies(variables, expr->getLHS());
        genExprDependencies(variables, expr->getRHS());
    }

    void genExprDependencies(const PlanGraphVariables& variables, const NodeLabelExpr* expr) {
        _dependencies.emplace(
            variables.getVarNode(expr->getDecl()),
            LabelDependency {&expr->labelSet()});
    }

    void genExprDependencies(const PlanGraphVariables& variables, const PropertyExpr* expr) {
        _dependencies.emplace(variables.getVarNode(expr->getDecl()),
                              PropertyDependency {expr->getPropertyType()});
    }
};

}

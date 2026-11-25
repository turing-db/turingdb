#include "DBClauseItems.h"

#include "expr/Operators.h"

#include "expr/LogicalExpr.h"

using namespace db;

const VarDecl* db::projectedVariable(const Projection::ReturnItem& item) {
    if (VarDecl* const* itemDecl = std::get_if<VarDecl*>(&item)) {
        return *itemDecl;
    }

    const Expr* itemExpr = *std::get_if<Expr*>(&item);
    if (itemExpr->getKind() != Expr::Kind::SYMBOL) {
        return nullptr;
    }

    return itemExpr->getExprVarDecl();
}

void db::flattenConjuncts(const Expr* expr, std::vector<const Expr*>& conjuncts) {
    if (expr->getKind() == Expr::Kind::LOGICAL) {
        const LogicalExpr* logicalExpr = static_cast<const LogicalExpr*>(expr);

        // Two links of a comparison chain read the same operand, so a filter standing
        // between them would leave the second link reading a column laid out over rows the
        // first one cut. The chain is one predicate and filters once
        const bool splits = logicalExpr->getOperator() == LogicalOperator::And
                            && !logicalExpr->isComparisonChain();

        if (splits) {
            for (const Expr* operand : logicalExpr->getOperands()) {
                flattenConjuncts(operand, conjuncts);
            }
            return;
        }
    }

    conjuncts.push_back(expr);
}

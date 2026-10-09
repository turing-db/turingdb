#include "LogicalExpr.h"

#include "CypherAST.h"

using namespace db;

LogicalExpr::LogicalExpr(LogicalOperator op)
    : Expr(Kind::LOGICAL),
    _operator(op)
{
}

LogicalExpr::~LogicalExpr() {
}

void LogicalExpr::addOperand(Expr* operand) {
    _operands.push_back(operand);
}

LogicalExpr* LogicalExpr::create(CypherAST* ast, LogicalOperator op) {
    LogicalExpr* expr = new LogicalExpr(op);
    ast->addExpr(expr);

    return expr;
}

LogicalExpr* LogicalExpr::append(CypherAST* ast, LogicalOperator op, Expr* lhs, Expr* rhs) {
    if (lhs->getKind() == Kind::LOGICAL) {
        LogicalExpr* chain = static_cast<LogicalExpr*>(lhs);

        const bool extendsChain = chain->_operator == op
                                  && !chain->_comparisonChain
                                  && !chain->isParenthesized();

        if (extendsChain) {
            chain->addOperand(rhs);
            return chain;
        }
    }

    LogicalExpr* expr = create(ast, op);
    expr->addOperand(lhs);
    expr->addOperand(rhs);

    return expr;
}

LogicalExpr* LogicalExpr::appendComparison(CypherAST* ast, Expr* chain, Expr* comparison) {
    if (chain->getKind() == Kind::LOGICAL) {
        LogicalExpr* logical = static_cast<LogicalExpr*>(chain);

        if (logical->_comparisonChain) {
            logical->addOperand(comparison);
            return logical;
        }
    }

    LogicalExpr* expr = create(ast, LogicalOperator::And);
    expr->_comparisonChain = true;
    expr->addOperand(chain);
    expr->addOperand(comparison);

    return expr;
}

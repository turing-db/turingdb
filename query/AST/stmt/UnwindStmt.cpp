#include "UnwindStmt.h"

#include <algorithm>

#include "CypherAST.h"
#include "Literal.h"
#include "expr/LiteralExpr.h"

using namespace db;

namespace {

bool isLiteralExpr(const Expr* expr) {
    if (expr->getKind() != Expr::Kind::LITERAL) {
        return false;
    }

    const Literal* literal = static_cast<const LiteralExpr*>(expr)->getLiteral();
    if (literal->getKind() != Literal::Kind::LIST) {
        return true;
    }

    const ListLiteral::Items& items = static_cast<const ListLiteral*>(literal)->items();
    return std::ranges::all_of(items, isLiteralExpr);
}

}

UnwindStmt::~UnwindStmt() {
}

bool UnwindStmt::unwindsLiteral() const {
    return isLiteralExpr(_arg);
}

UnwindStmt* UnwindStmt::create(CypherAST* ast, Expr* expr, Symbol* sym) {
    UnwindStmt* stmt = new UnwindStmt(expr, sym);
    ast->addStmt(stmt);
    return stmt;
}

#include "UnwindStmt.h"

#include "CypherAST.h"
#include "Literal.h"
#include "expr/LiteralExpr.h"

using namespace db;

UnwindStmt::~UnwindStmt() {
}

bool UnwindStmt::unwindsLiteral() const {
    if (_arg->getKind() != Expr::Kind::LITERAL) {
        return false;
    }

    const Literal* literal = static_cast<const LiteralExpr*>(_arg)->getLiteral();
    if (literal->getKind() != Literal::Kind::LIST) {
        return true;
    }

    return static_cast<const ListLiteral*>(literal)->isLiteralTree();
}

UnwindStmt* UnwindStmt::create(CypherAST* ast, Expr* expr, Symbol* sym) {
    UnwindStmt* stmt = new UnwindStmt(expr, sym);
    ast->addStmt(stmt);
    return stmt;
}

#include "UnwindStmt.h"

#include "CypherAST.h"
#include "Literal.h"
#include "expr/LiteralExpr.h"
#include "decl/VarDecl.h"

using namespace db;

UnwindStmt::~UnwindStmt() {
}

bool UnwindStmt::unwindsLiteral() const {
    const bool unwindsALiteral = _arg->getKind() == Expr::Kind::LITERAL;
    const bool isCastToAnEntity = _decl->isCastToAnEntity();

    if (!unwindsALiteral || isCastToAnEntity) {
        return false;
    }

    const Literal* literal = static_cast<const LiteralExpr*>(_arg)->getLiteral();

    return literal->isLiteralTree();
}

UnwindStmt* UnwindStmt::create(CypherAST* ast, Expr* expr, Symbol* sym) {
    UnwindStmt* stmt = new UnwindStmt(expr, sym);
    ast->addStmt(stmt);
    return stmt;
}

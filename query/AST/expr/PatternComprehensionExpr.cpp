#include "PatternComprehensionExpr.h"

#include "CypherAST.h"
#include "stmt/MatchStmt.h"

using namespace db;

PatternComprehensionExpr::PatternComprehensionExpr(MatchStmt* match, Expr* projection)
    : Expr(Expr::Kind::PATTERN_COMPREHENSION),
    _match(match),
    _projection(projection)
{
}

PatternComprehensionExpr::~PatternComprehensionExpr() {
}

void PatternComprehensionExpr::setOwnDecls(const OwnDecls& decls) {
    _ownDecls = decls;
}

const Pattern* PatternComprehensionExpr::getPattern() const {
    return _match->getPattern();
}

PatternComprehensionExpr* PatternComprehensionExpr::create(CypherAST* ast,
                                                           MatchStmt* match,
                                                           Expr* projection) {
    PatternComprehensionExpr* expr = new PatternComprehensionExpr(match, projection);
    ast->addExpr(expr);

    return expr;
}

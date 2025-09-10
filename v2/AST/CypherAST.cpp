#include "CypherAST.h"

#include "expr/Expr.h"
#include "Pattern.h"
#include "PatternElement.h"
#include "EntityPattern.h"
#include "Projection.h"
#include "stmt/Stmt.h"
#include "QueryCommand.h"

using namespace db::v2;

CypherAST::CypherAST()
{
}

CypherAST::~CypherAST() {
    for (Expr* expr : _expressions) {
        delete expr;
    }

    for (Pattern* pattern : _patterns) {
        delete pattern;
    }

    for (PatternElement* element : _patternElems) {
        delete element;
    }

    for (EntityPattern* pattern : _entityPatterns) {
        delete pattern;
    }

    for (Projection* projection : _projections) {
        delete projection;
    }

    for (Stmt* stmt : _stmts) {
        delete stmt;
    }

    for (QueryCommand* query : _queries) {
        delete query;
    }
}

void CypherAST::addExpr(Expr* expr) {
    _expressions.push_back(expr);
}

void CypherAST::addPattern(Pattern* pattern) {
    _patterns.push_back(pattern);
}

void CypherAST::addPatternElement(PatternElement* element) {
    _patternElems.push_back(element);
}

void CypherAST::addEntityPattern(EntityPattern* pattern) {
    _entityPatterns.push_back(pattern);
}

void CypherAST::addProjection(Projection* projection) {
    _projections.push_back(projection);
}

void CypherAST::addStmt(Stmt* stmt) {
    _stmts.push_back(stmt);
}

void CypherAST::addQuery(QueryCommand* query) {
    _queries.push_back(query);
}

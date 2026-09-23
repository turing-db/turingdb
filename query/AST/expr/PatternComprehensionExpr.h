#pragma once

#include "Expr.h"

namespace db {

class CypherAST;
class MatchStmt;
class Pattern;

// A Cypher pattern comprehension: `[(a)-[:KNOWS]->(b) WHERE b.age > 20 | b.name]`. The
// pattern is matched once per row in flight and the projection is the value each match
// contributes to that row's list, which is empty where the pattern matches nothing. A
// variable the pattern names that is already in scope joins onto it; the ones it binds of
// its own are read by the WHERE and the projection alone.
//
// The pattern is held as a MATCH of its own - the WHERE included, as a MATCH carries it -
// which is what declares the variables it binds and what the traversal is generated from.
class PatternComprehensionExpr : public Expr {
public:
    MatchStmt* getMatch() const { return _match; }
    Expr* getProjection() const { return _projection; }

    const Pattern* getPattern() const;

    static PatternComprehensionExpr* create(CypherAST* ast, MatchStmt* match, Expr* projection);

private:
    MatchStmt* _match {nullptr};
    Expr* _projection {nullptr};

    PatternComprehensionExpr(MatchStmt* match, Expr* projection);
    ~PatternComprehensionExpr() override;
};

}

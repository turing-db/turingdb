#pragma once

#include "Expr.h"

namespace db {

class CypherAST;
class Pattern;
class RegularQuery;

// A Cypher existential subquery: `EXISTS { MATCH (p)-[:KNOWS]->(k) }`. The body is a
// read-only query of its own, correlated with the scope around it - the variables in
// flight are readable inside it - and the expression is true for a row exactly when the
// body produces a row of its own for it. The pattern shorthand `EXISTS { (p)-[:KNOWS]->(k) }`
// and the bare pattern predicate `(p)-[:KNOWS]->(k)` parse into the same body, holding the
// one MATCH the pattern stands for.
class ExistsExpr : public Expr {
public:
    const Pattern* getPredicatePattern() const { return _predicatePattern; }

    void setPredicatePattern(const Pattern* pattern) { _predicatePattern = pattern; }

    RegularQuery* getBody() const { return _body; }

    static ExistsExpr* create(CypherAST* ast, RegularQuery* body);

private:
    RegularQuery* _body {nullptr};
    const Pattern* _predicatePattern {nullptr};

    explicit ExistsExpr(RegularQuery* body);
    ~ExistsExpr() override;
};

}

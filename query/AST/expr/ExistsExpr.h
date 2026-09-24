#pragma once

#include "Expr.h"

namespace db {

class CypherAST;
class Pattern;
class SinglePartQuery;

// A Cypher existential subquery: `EXISTS { MATCH (p)-[:KNOWS]->(k) }`. The body is a
// read-only query of its own, correlated with the scope around it - the variables in
// flight are readable inside it - and the expression is true for a row exactly when the
// body produces a row of its own for it. The pattern shorthand `EXISTS { (p)-[:KNOWS]->(k) }`
// and the bare pattern predicate `(p)-[:KNOWS]->(k)` parse into the same body, holding the
// one MATCH the pattern stands for.
class ExistsExpr : public Expr {
public:
    SinglePartQuery* getBody() const { return _body; }
    const Pattern* getPredicatePattern() const { return _predicatePattern; }

    void setPredicatePattern(const Pattern* pattern) { _predicatePattern = pattern; }

    static ExistsExpr* create(CypherAST* ast, SinglePartQuery* body);

private:
    SinglePartQuery* _body {nullptr};
    const Pattern* _predicatePattern {nullptr};

    explicit ExistsExpr(SinglePartQuery* body);
    ~ExistsExpr() override;
};

}

#pragma once

#include <vector>

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
//
// A body that is a UNION holds one query per branch. It produces a row exactly when one
// of them does, whatever the UNION dedups, so the operators joining them are not kept.
class ExistsExpr : public Expr {
public:
    using Branches = std::vector<SinglePartQuery*>;

    const Pattern* getPredicatePattern() const { return _predicatePattern; }

    void setPredicatePattern(const Pattern* pattern) { _predicatePattern = pattern; }

    const Branches& branches() const { return _branches; }

    static ExistsExpr* create(CypherAST* ast, const Branches& branches);

private:
    Branches _branches;
    const Pattern* _predicatePattern {nullptr};

    explicit ExistsExpr(const Branches& branches);
    ~ExistsExpr() override;
};

}

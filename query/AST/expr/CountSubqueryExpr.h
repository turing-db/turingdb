#pragma once

#include <vector>

#include "Expr.h"
#include "UnionQuery.h"

namespace db {

class CypherAST;

// A Cypher COUNT subquery: `COUNT { MATCH (p)-[:KNOWS]->(k) }`. The body is read-only and
// correlated, as an EXISTS body is, and the expression is the number of rows the body
// produces for each row in flight. A body that is a UNION holds one query per branch, with
// the operator joining it to the ones before it: a UNION dedups the rows it counts. A body
// that is a WHEN holds one query per branch too, and one predicate per WHEN.
class CountSubqueryExpr : public Expr {
public:
    using Branches = UnionQuery::Branches;
    using Conditions = std::vector<Expr*>;

    const Branches& getBranches() const { return _branches; }

    const Conditions& getConditions() const { return _conditions; }
    void setConditions(const Conditions& conditions) { _conditions = conditions; }
    bool isConditional() const { return !_conditions.empty(); }

    static CountSubqueryExpr* create(CypherAST* ast, const Branches& branches);

private:
    Branches _branches;
    Conditions _conditions;

    explicit CountSubqueryExpr(const Branches& branches);
    ~CountSubqueryExpr() override;
};

}

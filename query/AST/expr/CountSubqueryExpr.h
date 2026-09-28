#pragma once

#include <stddef.h>

#include "Expr.h"
#include "UnionQuery.h"

namespace db {

class CypherAST;

// A Cypher COUNT subquery: `COUNT { MATCH (p)-[:KNOWS]->(k) }`. The body is read-only and
// correlated, as an EXISTS body is, and the expression is the number of rows the body
// produces for each row in flight. A body that is a UNION holds one query per branch, with
// the operator joining it to the ones before it: a UNION dedups the rows it counts.
class CountSubqueryExpr : public Expr {
public:
    using Branches = UnionQuery::Branches;

    const Branches& getBranches() const { return _branches; }

    // How many leading branches dedup against one another, as UnionQuery counts them
    size_t getDedupedBranchCount() const;

    static CountSubqueryExpr* create(CypherAST* ast, const Branches& branches);

private:
    Branches _branches;

    explicit CountSubqueryExpr(const Branches& branches);
    ~CountSubqueryExpr() override;
};

}

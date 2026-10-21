#pragma once

#include "Expr.h"
#include "UnionQuery.h"

namespace db {

class CypherAST;

// A Cypher COLLECT subquery: `COLLECT { MATCH (p)-[:KNOWS]->(k) RETURN k.name }`. The body
// is read-only and correlated, as a COUNT body is, and ends in a RETURN of one column. The
// expression is the list of the values that column holds for each row in flight. A body
// that is a UNION holds one query per branch, with the operator joining it to the ones
// before it: a UNION dedups the values it collects.
class CollectSubqueryExpr : public Expr {
public:
    using Branches = UnionQuery::Branches;

    const Branches& getBranches() const { return _branches; }

    static CollectSubqueryExpr* create(CypherAST* ast, const Branches& branches);

private:
    Branches _branches;

    explicit CollectSubqueryExpr(const Branches& branches);
    ~CollectSubqueryExpr() override;
};

}

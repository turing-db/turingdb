#pragma once

#include "Expr.h"

namespace db {

class CypherAST;
class RegularQuery;

// A Cypher COLLECT subquery: `COLLECT { MATCH (p)-[:KNOWS]->(k) RETURN k.name }`. The body
// is read-only and correlated, as a COUNT body is, and ends in a RETURN of one column. The
// expression is the list of the values that column holds for each row in flight. A UNION
// in the body dedups the values it collects.
class CollectSubqueryExpr : public Expr {
public:
    RegularQuery* getBody() const { return _body; }

    static CollectSubqueryExpr* create(CypherAST* ast, RegularQuery* body);

private:
    RegularQuery* _body {nullptr};

    explicit CollectSubqueryExpr(RegularQuery* body);
    ~CollectSubqueryExpr() override;
};

}

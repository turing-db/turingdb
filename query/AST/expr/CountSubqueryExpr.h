#pragma once

#include "Expr.h"

namespace db {

class CypherAST;
class RegularQuery;

// A Cypher COUNT subquery: `COUNT { MATCH (p)-[:KNOWS]->(k) }`. The body is read-only and
// correlated, as an EXISTS body is, and the expression is the number of rows the body
// produces for each row in flight. A UNION in the body dedups the rows it counts.
class CountSubqueryExpr : public Expr {
public:
    RegularQuery* getBody() const { return _body; }

    static CountSubqueryExpr* create(CypherAST* ast, RegularQuery* body);

private:
    RegularQuery* _body {nullptr};

    explicit CountSubqueryExpr(RegularQuery* body);
    ~CountSubqueryExpr() override;
};

}

#pragma once

namespace db::v2 {

class CypherAST;
class Expr;

class WhereClause {
public:
    friend CypherAST;

    static WhereClause* create(CypherAST* ast, Expr* expr);

    Expr* getExpr() const { return _expr; }

    void setExpr(Expr* expr) { _expr = expr; }

private:
    Expr* _expr {nullptr};

    WhereClause(Expr* expr)
        : _expr(expr)
    {
    }

    ~WhereClause() = default;
};

}

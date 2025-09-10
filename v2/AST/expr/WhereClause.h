#pragma once

namespace db::v2 {

class Expr;

class WhereClause {
public:
    WhereClause() = default;
    ~WhereClause() = default;

    WhereClause(const WhereClause&) = default;
    WhereClause(WhereClause&&) = default;
    WhereClause& operator=(const WhereClause&) = default;
    WhereClause& operator=(WhereClause&&) = default;

    WhereClause(Expr* expr)
        : _expr(expr)
    {
    }

    Expr* getExpr() const {
        return _expr;
    }

    void setExpr(Expr* expr) {
        _expr = expr;
    }

private:
    Expr* _expr {nullptr};
};

}

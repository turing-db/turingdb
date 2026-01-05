#pragma once

#include "SubStmt.h"

namespace db::v2 {

class Expr;
class CypherAST;

class Limit : public SubStmt {
public:
    static Limit* create(CypherAST* ast, Expr* expr);

    const Expr* getExpr() const { return _expr; }
    Expr* getExpr() { return _expr; }

    void setExpr(Expr* expr) { _expr = expr; }

private:
    Expr* _expr {nullptr};

    Limit(Expr* expr);
    ~Limit() override;
};

}

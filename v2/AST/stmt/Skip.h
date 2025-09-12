#pragma once

#include "SubStmt.h"

namespace db::v2 {

class Expr;

class Skip : public SubStmt {
public:
    static Skip* create(CypherAST* ast, Expr* expr);

    const Expr* getExpr() const { return _expr; }

    void setExpr(Expr* expr) { _expr = expr; }

private:
    Expr* _expr {nullptr};

    Skip(Expr* expr);
    ~Skip() override;
};

}

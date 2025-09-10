#pragma once

#include "Expr.h"

#include "Literal.h"

namespace db::v2 {

class CypherAST;

class LiteralExpr : public Expr {
public:
    static LiteralExpr* create(CypherAST* ast, const Literal& literal);

    const Literal& literal() const { return _literal; }

private:
    Literal _literal;

    explicit LiteralExpr(const Literal& literal)
        : Expr(Kind::LITERAL),
        _literal(literal)
    {
    }

    ~LiteralExpr() override;
};

}

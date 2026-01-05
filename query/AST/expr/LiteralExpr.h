#pragma once

#include "Expr.h"

namespace db::v2 {

class CypherAST;
class Literal;

class LiteralExpr : public Expr {
public:
    static LiteralExpr* create(CypherAST* ast, Literal* literal);

    Literal* getLiteral() const { return _literal; }

    EvaluatedType getType() const override;

private:
    Literal* _literal {nullptr};

    explicit LiteralExpr(Literal* literal)
        : Expr(Kind::LITERAL),
        _literal(literal)
    {
    }

    ~LiteralExpr() override;
};

}

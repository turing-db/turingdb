#pragma once

#include <memory>

#include "Expression.h"
#include "types/Literal.h"

namespace db {

class LiteralExpression : public Expression {
public:
    ~LiteralExpression() override = default;

    explicit LiteralExpression(const Literal& literal)
        : Expression(ExpressionType::Literal),
        _literal(literal)
    {
    }

    LiteralExpression(const LiteralExpression&) = delete;
    LiteralExpression(LiteralExpression&&) = delete;
    LiteralExpression& operator=(const LiteralExpression&) = delete;
    LiteralExpression& operator=(LiteralExpression&&) = delete;

    static std::unique_ptr<LiteralExpression> create(const Literal& literal) {
        return std::make_unique<LiteralExpression>(literal);
    }

    const Literal& literal() const {
        return _literal;
    }

private:
    Literal _literal {nullptr};
};

}

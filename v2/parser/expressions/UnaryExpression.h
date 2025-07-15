#pragma once

#include <memory>

#include "Operators.h"
#include "Expression.h"

namespace db {

class UnaryExpression : public Expression {
public:
    UnaryExpression()
        : Expression(ExpressionType::Unary) {
    }

    ~UnaryExpression() override = default;

    UnaryExpression(const UnaryExpression&) = delete;
    UnaryExpression(UnaryExpression&&) = delete;
    UnaryExpression& operator=(const UnaryExpression&) = delete;
    UnaryExpression& operator=(UnaryExpression&&) = delete;

    UnaryOperator getUnaryOperator() const { return _operator; }
    Expression& right() { return *_right; }

    static Expression* create(
        UnaryOperator op,
        std::unique_ptr<Expression> right) {

        return new UnaryExpression {
            op,
            std::move(right),
        };
    }

private:
    UnaryExpression(UnaryOperator op, std::unique_ptr<Expression> right)
        : Expression(ExpressionType::Unary),
          _right(std::move(right)),
          _operator(op) {}

    std::unique_ptr<Expression> _right;
    UnaryOperator _operator {};
};

}

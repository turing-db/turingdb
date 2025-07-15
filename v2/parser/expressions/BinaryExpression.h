#pragma once

#include <memory>

#include "Expression.h"
#include "Operators.h"

namespace db {

class BinaryExpression : public Expression {
public:
    BinaryExpression()
        : Expression(ExpressionType::Binary) {}

    ~BinaryExpression() override = default;

    BinaryExpression(const BinaryExpression&) = delete;
    BinaryExpression(BinaryExpression&&) = delete;
    BinaryExpression& operator=(const BinaryExpression&) = delete;
    BinaryExpression& operator=(BinaryExpression&&) = delete;

    BinaryOperator getBinaryOperator() const { return _operator; }
    Expression& left() { return *_left; }
    Expression& right() { return *_right; }

    static std::unique_ptr<BinaryExpression> create(
        BinaryOperator op,
        std::unique_ptr<Expression> left,
        std::unique_ptr<Expression> right) {

        return std::unique_ptr<BinaryExpression> {
            new BinaryExpression {op, std::move(left), std::move(right)}
        };
    }

private:
    BinaryExpression(BinaryOperator op, std::unique_ptr<Expression> left, std::unique_ptr<Expression> right)
        : Expression(ExpressionType::Binary),
          _left(std::move(left)),
          _right(std::move(right)),
          _operator(op) {}

    std::unique_ptr<Expression> _left;
    std::unique_ptr<Expression> _right;
    BinaryOperator _operator {};
};

}

#pragma once

#include <memory>

#include "Expression.h"
#include "Operators.h"

namespace db {

class BinaryExpression : public Expression {
public:
    BinaryExpression() = delete;
    ~BinaryExpression() override = default;

    BinaryExpression(const Expression* left,
                     BinaryOperator op,
                     const Expression* right)
        : Expression(ExpressionType::Binary),
        _left(left),
        _right(right),
        _operator(op)
    {
    }

    BinaryExpression(const BinaryExpression&) = delete;
    BinaryExpression(BinaryExpression&&) = delete;
    BinaryExpression& operator=(const BinaryExpression&) = delete;
    BinaryExpression& operator=(BinaryExpression&&) = delete;

    BinaryOperator getBinaryOperator() const { return _operator; }
    const Expression& left() const { return *_left; }
    const Expression& right() const { return *_right; }

    static std::unique_ptr<BinaryExpression> create(const Expression* left,
                                                    BinaryOperator op,
                                                    const Expression* right) {
        return std::make_unique<BinaryExpression>(left, op, right);
    }

private:
    const Expression* _left {nullptr};
    const Expression* _right {nullptr};
    BinaryOperator _operator {};
};

}

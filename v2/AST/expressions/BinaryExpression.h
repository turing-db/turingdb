#pragma once

#include <memory>

#include "Expression.h"
#include "Operators.h"

namespace db::v2 {

class BinaryExpression : public Expression {
public:
    BinaryExpression() = delete;
    ~BinaryExpression() override = default;

    BinaryExpression(Expression* left,
                     BinaryOperator op,
                     Expression* right)
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
    Expression& left() { return *_left; }
    Expression& right() { return *_right; }

    static std::unique_ptr<BinaryExpression> create(Expression* left,
                                                    BinaryOperator op,
                                                    Expression* right) {
        return std::make_unique<BinaryExpression>(left, op, right);
    }

private:
    Expression* _left {nullptr};
    Expression* _right {nullptr};
    BinaryOperator _operator {};
};

}

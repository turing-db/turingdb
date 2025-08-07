#pragma once

#include <memory>

#include "Operators.h"
#include "Expression.h"

namespace db {

class UnaryExpression : public Expression {
public:
    UnaryExpression() = delete;
    ~UnaryExpression() override = default;

    UnaryExpression(UnaryOperator op, Expression* right)
        : Expression(ExpressionType::Unary),
        _right(right),
        _operator(op)
    {
    }

    UnaryExpression(const UnaryExpression&) = delete;
    UnaryExpression(UnaryExpression&&) = delete;
    UnaryExpression& operator=(const UnaryExpression&) = delete;
    UnaryExpression& operator=(UnaryExpression&&) = delete;

    UnaryOperator getUnaryOperator() const { return _operator; }
    const Expression& right() const { return *_right; }
    Expression& right() { return *_right; }

    static std::unique_ptr<UnaryExpression> create(UnaryOperator op, Expression* right) {
        return std::make_unique<UnaryExpression>(op, right);
    }

private:
    Expression* _right {nullptr};
    UnaryOperator _operator {};
};

}

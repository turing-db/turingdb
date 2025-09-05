#pragma once

#include <memory>

#include "Expression.h"
#include "Operators.h"

namespace db::v2 {

class StringExpression : public Expression {
public:
    StringExpression() = delete;
    ~StringExpression() override = default;

    StringExpression(Expression* left, StringOperator op, Expression* right)
        : Expression(ExpressionType::String),
        _left(left),
        _right(right),
        _operator(op)
    {
    }

    StringExpression(const StringExpression&) = delete;
    StringExpression(StringExpression&&) = delete;
    StringExpression& operator=(const StringExpression&) = delete;
    StringExpression& operator=(StringExpression&&) = delete;

    StringOperator getStringOperator() const { return _operator; }

    const Expression& left() const { return *_left; }
    const Expression& right() const { return *_right; }

    Expression& left() { return *_left; }
    Expression& right() { return *_right; }

    static std::unique_ptr<StringExpression> create(Expression* left,
                                                    StringOperator op,
                                                    Expression* right) {
        return std::make_unique<StringExpression>(left, op, right);
    }

private:
    Expression* _left {nullptr};
    Expression* _right {nullptr};
    StringOperator _operator {};
};

}

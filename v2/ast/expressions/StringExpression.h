#pragma once

#include <memory>

#include "Expression.h"
#include "Operators.h"

namespace db {

class StringExpression : public Expression {
public:
    StringExpression() = delete;
    ~StringExpression() override = default;

    StringExpression(const Expression* left, StringOperator op, const Expression* right)
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


    static std::unique_ptr<StringExpression> create(const Expression* left,
                                                    StringOperator op,
                                                    const Expression* right) {
        return std::make_unique<StringExpression>(left, op, right);
    }

private:
    const Expression* _left {nullptr};
    const Expression* _right {nullptr};
    StringOperator _operator {};
};

}

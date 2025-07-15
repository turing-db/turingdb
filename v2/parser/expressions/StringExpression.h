#pragma once

#include <memory>

#include "Expression.h"
#include "Operators.h"

namespace db {

class StringExpression : public Expression {
public:
    StringExpression()
        : Expression(ExpressionType::String) {}

    ~StringExpression() override = default;

    StringExpression(const StringExpression&) = delete;
    StringExpression(StringExpression&&) = delete;
    StringExpression& operator=(const StringExpression&) = delete;
    StringExpression& operator=(StringExpression&&) = delete;

    StringOperator getStringOperator() const { return _operator; }
    Expression& right() { return *_right; }

private:
    std::unique_ptr<Expression> _right;
    StringOperator _operator {};
};

}

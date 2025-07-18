#pragma once

#include <memory>

#include "Expression.h"
#include "Operators.h"

namespace db {

class StringExpression : public Expression {
public:
    StringExpression() = delete;
    ~StringExpression() override = default;

    StringExpression(StringOperator op, Expression* right)
        : Expression(ExpressionType::String),
          _right(right),
          _operator(op)
    {
    }

    StringExpression(const StringExpression&) = delete;
    StringExpression(StringExpression&&) = delete;
    StringExpression& operator=(const StringExpression&) = delete;
    StringExpression& operator=(StringExpression&&) = delete;

    StringOperator getStringOperator() const { return _operator; }
    Expression& right() { return *_right; }


    static std::unique_ptr<StringExpression> create(StringOperator op,
                                              Expression* right) {
        return std::make_unique<StringExpression>(op, right);
    }

private:
    Expression* _right = nullptr;
    StringOperator _operator {};
};

}

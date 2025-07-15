#pragma once

#include "Expression.h"

namespace db {

class AtomicExpression : public Expression {
public:
    enum class AtomicType {
    };

    AtomicExpression(ExpressionType type)
        : Expression(type) {}

    ~AtomicExpression() override = default;

    AtomicExpression(const AtomicExpression&) = delete;
    AtomicExpression(AtomicExpression&&) = delete;
    AtomicExpression& operator=(const AtomicExpression&) = delete;
    AtomicExpression& operator=(AtomicExpression&&) = delete;
};

}

#pragma once

#include <memory>

#include "SubStatement.h"

namespace db::v2 {

class Expression;

class Limit : public SubStatement {
public:
    Limit() = default;
    ~Limit() override = default;

    explicit Limit(Expression* expression)
        : _expression(expression)
    {
    }

    Limit(const Limit&) = default;
    Limit& operator=(const Limit&) = default;
    Limit(Limit&&) = default;
    Limit& operator=(Limit&&) = default;

    static std::unique_ptr<Limit> create(Expression* expression) {
        return std::make_unique<Limit>(expression);
    }

    const Expression& getExpression() const {
        return *_expression;
    }

    void setExpression(Expression* expression) {
        _expression = expression;
    }

private:
    Expression* _expression {nullptr};
};

}

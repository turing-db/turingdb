#pragma once

#include <memory>

#include "Statement.h"

namespace db {

class Expression;

class Skip : public Statement {
public:
    Skip() = default;
    ~Skip() = default;

    explicit Skip(Expression* expression)
        : _expression(expression)
    {
    }

    Skip(const Skip&) = default;
    Skip& operator=(const Skip&) = default;
    Skip(Skip&&) = default;
    Skip& operator=(Skip&&) = default;

    static std::unique_ptr<Skip> create(Expression* expression) {
        return std::make_unique<Skip>(expression);
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

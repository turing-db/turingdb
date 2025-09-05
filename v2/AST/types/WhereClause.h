#pragma once

#include "expressions/Expression.h"

namespace db::v2 {

class WhereClause {
public:
    WhereClause() = default;
    ~WhereClause() = default;

    WhereClause(const WhereClause&) = default;
    WhereClause(WhereClause&&) = default;
    WhereClause& operator=(const WhereClause&) = default;
    WhereClause& operator=(WhereClause&&) = default;

    WhereClause(Expression* expression)
        : _expression(expression)
    {
    }

    Expression& getExpression() const {
        return *_expression;
    }

    void setExpression(Expression* expression) {
        _expression = expression;
    }

private:
    Expression* _expression {nullptr};
};

}

#pragma once

namespace db {

#include "Expression.h"

class WhereClause {
public:
    WhereClause() = default;
    ~WhereClause() = default;

    WhereClause(const WhereClause&) = delete;
    WhereClause(WhereClause&&) = delete;
    WhereClause& operator=(const WhereClause&) = delete;
    WhereClause& operator=(WhereClause&&) = delete;


private:
    std::shared_ptr<Expression> _expression;
};

}

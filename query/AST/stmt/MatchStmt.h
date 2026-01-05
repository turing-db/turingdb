#pragma once

#include "Stmt.h"

namespace db::v2 {

class Pattern;
class OrderBy;
class Skip;
class Limit;
class CypherAST;

class MatchStmt : public Stmt {
public:
    static MatchStmt* create(CypherAST* ast, Pattern* pattern);

    Kind getKind() const override { return Kind::MATCH; }

    bool isOptional() const { return _optional; }

    bool hasOrderBy() const { return _orderBy != nullptr; }

    bool hasLimit() const { return _limit != nullptr; }

    bool hasSkip() const { return _skip != nullptr; }

    OrderBy* getOrderBy() const { return _orderBy; }
    Limit* getLimit() const { return _limit; }
    Skip* getSkip() const { return _skip; }

    const Pattern* getPattern() const { return _pattern; }

    void setOrderBy(OrderBy* orderBy) { _orderBy = orderBy; }
    void setLimit(Limit* limit) { _limit = limit; }
    void setSkip(Skip* skip) { _skip = skip; }
    void setOptional(bool optional) { _optional = optional; }

private:
    Pattern* _pattern {nullptr};
    OrderBy* _orderBy {nullptr};
    Limit* _limit {nullptr};
    Skip* _skip {nullptr};
    bool _optional {false};

    MatchStmt(Pattern* pattern)
        : _pattern(pattern)
    {
    }

    ~MatchStmt() override;
};

}

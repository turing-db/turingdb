#pragma once

#include "Stmt.h"

namespace db::v2 {

class Pattern;
class Skip;
class Limit;
class CypherAST;

class MatchStmt : public Stmt {
public:
    static MatchStmt* create(CypherAST* ast, Pattern* pattern);

    bool isOptional() const { return _optional; }

    bool hasLimit() const { return _limit != nullptr; }

    bool hasSkip() const { return _skip != nullptr; }

    const Limit* getLimit() const { return _limit; }

    const Skip* getSkip() const { return _skip; }

    const Pattern* getPattern() const { return _pattern; }

    void setLimit(Limit* limit) { _limit = limit; }
    void setSkip(Skip* skip) { _skip = skip; }
    void setOptional(bool optional) { _optional = optional; }

private:
    Pattern* _pattern {nullptr};
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

#pragma once

#include "Stmt.h"

namespace db::v2 {

class Pattern;
class Skip;
class Limit;
class CypherAST;

class MatchStmt : public Stmt {
public:
    static MatchStmt* create(CypherAST* ast,
                             Pattern* pattern,
                             Skip* skip = nullptr,
                             Limit* limit = nullptr,
                             bool optional = false);

    bool hasPattern() const { return _pattern != nullptr; }

    bool isOptional() const { return _optional; }

    bool hasLimit() const { return _limit != nullptr; }

    bool hasSkip() const { return _skip != nullptr; }

    const Limit* getLimit() const { return _limit; }

    const Skip* getSkip() const { return _skip; }

    const Pattern* getPattern() const { return _pattern; }

private:
    Pattern* _pattern {nullptr};
    Limit* _limit {nullptr};
    Skip* _skip {nullptr};
    bool _optional {false};

    MatchStmt(Pattern* pattern,
              Skip* skip = nullptr,
              Limit* limit = nullptr,
              bool optional = false)
        : _pattern(pattern),
        _limit(limit),
        _skip(skip),
        _optional(optional)
    {
    }

    ~MatchStmt() override;
};

}

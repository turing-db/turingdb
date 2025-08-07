#pragma once

#include <memory>

#include "types/Pattern.h"
#include "Skip.h"
#include "Statement.h"
#include "Limit.h"

namespace db {

class Expression;

class Match : public Statement {
public:
    Match() = default;
    ~Match() override = default;

    Match(const Match&) = delete;
    Match& operator=(const Match&) = delete;
    Match(Match&&) = delete;
    Match& operator=(Match&&) = delete;

    Match(Pattern* pattern,
          Skip* skip = nullptr,
          Limit* limit = nullptr,
          bool optional = false)
        : _pattern(pattern),
        _limit(limit),
        _skip(skip),
        _optional(optional)
    {
    }

    static std::unique_ptr<Match> create(Pattern* pattern,
                                         Skip* skip = nullptr,
                                         Limit* limit = nullptr,
                                         bool optional = false) {
        return std::make_unique<Match>(pattern, skip, limit, optional);
    }

    bool hasPattern() const {
        return _pattern != nullptr;
    }

    bool isOptional() const {
        return _optional;
    }

    bool hasLimit() const {
        return _limit;
    }

    bool hasSkip() const {
        return _skip;
    }

    const Limit& getLimit() const {
        return *_limit;
    }

    const Skip& getSkip() const {
        return *_skip;
    }

    const Pattern& getPattern() const {
        return *_pattern;
    }

private:
    Pattern* _pattern {nullptr};
    Limit* _limit {nullptr};
    Skip* _skip {nullptr};

    bool _optional {false};
};

}

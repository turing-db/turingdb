#pragma once

namespace db {

class ASTContext;
class PathPattern;

class MatchTarget {
public:
    friend ASTContext;

    static MatchTarget* create(ASTContext* ctxt, PathPattern* pattern);

    PathPattern* getPattern() const { return _pattern; }

private:
    PathPattern* _pattern {nullptr};

    MatchTarget(PathPattern* pattern);
    ~MatchTarget();
};

}

#pragma once

#include <vector>

namespace db {

class ASTContext;
class PathPattern;

class CreateTarget {
public:
    friend ASTContext;

    static CreateTarget* create(ASTContext* ctxt, PathPattern* pattern);

    const PathPattern* getPattern() const { return _pattern; }
    PathPattern* getPattern() { return _pattern; }

private:
    PathPattern* _pattern {nullptr};

    explicit CreateTarget(PathPattern* pattern)
        : _pattern(pattern)
    {
    }

    CreateTarget() = default;
};

class CreateTargets : public std::vector<CreateTarget*> {
public:
    friend ASTContext;

    static CreateTargets* create(ASTContext* ctxt);

private:
    CreateTargets() = default;
};

}

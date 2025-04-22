#pragma once

namespace db {

class ASTContext;
class PathPattern;

class CreateTarget {
public:
    explicit CreateTarget(PathPattern* pattern)
        : _pattern(pattern)
    {
    }

    const PathPattern* getPattern() const { return _pattern; }
    PathPattern* getPattern() { return _pattern; }

private:
    PathPattern* _pattern {nullptr};
};

}

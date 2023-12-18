#pragma once

#include <vector>

namespace db {

class ASTContext;
class PathPattern;

class FromTarget {
public:
    friend ASTContext;

    static FromTarget* create(ASTContext* ctxt, PathPattern* pattern);

    PathPattern* getPattern() const { return _pattern; }

private:
    PathPattern* _pattern {nullptr};

    FromTarget(PathPattern* pattern);
    ~FromTarget();
};

}

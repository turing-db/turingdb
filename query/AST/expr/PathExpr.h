#pragma once

#include "Expr.h"

namespace db::v2 {

class CypherAST;
class PatternElement;
class EntityPattern;

class PathExpr : public Expr {
public:
    static PathExpr* create(CypherAST* ast, PatternElement* pattern);

    PatternElement* pattern() const { return _pattern; }

    void addEntity(EntityPattern* entity);

private:
    PatternElement* _pattern {nullptr};

    PathExpr(PatternElement* pattern)
        : Expr(Kind::PATH),
        _pattern(pattern)
    {
    }

    ~PathExpr() override;
};

}

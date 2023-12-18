#pragma once

#include <vector>

namespace db {

class VarExpr;
class ASTContext;

class TypeConstraint {
public:
    friend ASTContext;
    using TypeNames = std::vector<VarExpr*>;

    static TypeConstraint* create(ASTContext* ctxt);

    void addType(VarExpr* typeName);

    const TypeNames& getTypeNames() const { return _types; } 

private:
    TypeNames _types;

    TypeConstraint();
    ~TypeConstraint();
};

}

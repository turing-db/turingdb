#pragma once

#include <vector>

#include "DeclKind.h"

namespace db {

class ASTContext;
class VarExpr;
class TypeConstraint;
class ExprConstraint;

class EntityPattern {
public:
    friend ASTContext;

    static EntityPattern* create(ASTContext* ctxt,
                                 VarExpr* var,
                                 TypeConstraint* typeConstr,
                                 ExprConstraint* exprConstr);

    void setKind(DeclKind kind) { _kind = kind; }
    DeclKind getKind() const { return _kind; }

    void setVar(VarExpr* var) { _var = var; }

    VarExpr* getVar() const { return _var; }
    TypeConstraint* getTypeConstraint() const { return _typeConstr; }
    ExprConstraint* getExprConstraint() const { return _exprConstr; }

private:
    DeclKind _kind {DeclKind::UNKNOWN};
    VarExpr* _var {nullptr};
    TypeConstraint* _typeConstr {nullptr};
    ExprConstraint* _exprConstr {nullptr};

    EntityPattern(VarExpr* var,
                  TypeConstraint* typeConstr,
                  ExprConstraint* exprConstr);
    ~EntityPattern();
};

class PathPattern {
public:
    friend ASTContext;
    using PathElements = std::vector<EntityPattern*>;

    static PathPattern* create(ASTContext* ctxt);

    const PathElements& elements() const { return _elements; }

    void addElement(EntityPattern* entity);

private:
    PathElements _elements;

    PathPattern();
    ~PathPattern();
};

}

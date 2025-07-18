#pragma once

#include <cstdint>
#include <vector>

#include "DeclKind.h"
#include "ID.h"

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

    // User defined entityID for create_node_pattern
    static EntityPattern* create(ASTContext* ctxt,
                                 VarExpr* var,
                                 uint64_t entityID);

    void setKind(DeclKind kind) { _kind = kind; }
    DeclKind getKind() const { return _kind; }

    void setVar(VarExpr* var) { _var = var; }
    void setTypeConstraint(TypeConstraint* typeConstr) { _typeConstr = typeConstr; }
    void setExprConstraint(ExprConstraint* exprConstr) { _exprConstr = exprConstr; }

    VarExpr* getVar() const { return _var; }
    TypeConstraint* getTypeConstraint() const { return _typeConstr; }
    ExprConstraint* getExprConstraint() const { return _exprConstr; }
    uint64_t getEntityID() const { return _entityID; }

private:
    DeclKind _kind {DeclKind::UNKNOWN};
    VarExpr* _var {nullptr};
    TypeConstraint* _typeConstr {nullptr};
    ExprConstraint* _exprConstr {nullptr};
    uint64_t _entityID {UINT64_MAX};

protected:
    EntityPattern(VarExpr* var,
                  TypeConstraint* typeConstr,
                  ExprConstraint* exprConstr,
                  uint64_t entityID);
    virtual ~EntityPattern();
};

class InjectedNodes : public EntityPattern {
public:
    friend ASTContext;

    static InjectedNodes* create(ASTContext* ctxt);

    static EntityPattern* create(ASTContext* ctxt,
                                 VarExpr* var,
                                 TypeConstraint* typeConstr,
                                 ExprConstraint* exprConstr);

    const std::vector<NodeID>& nodes() const { return _nodeIds; }

    void addNode(NodeID id);

private:
    std::vector<NodeID> _nodeIds;

    InjectedNodes();
    ~InjectedNodes() override;
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

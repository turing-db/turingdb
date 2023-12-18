#pragma once

#include <vector>

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

    VarExpr* getVar() const { return _var; }
    TypeConstraint* getTypeConstraint() const { return _typeConstr; }
    ExprConstraint* getExprConstraint() const { return _exprConstr; }

private:
    VarExpr* _var {nullptr};
    TypeConstraint* _typeConstr {nullptr};
    ExprConstraint* _exprConstr {nullptr};

    EntityPattern(VarExpr* var,
                  TypeConstraint* typeConstr,
                  ExprConstraint* exprConstr);
    ~EntityPattern();
};

class NodePattern {
public:
    friend ASTContext;

    static NodePattern* create(ASTContext* ctxt, EntityPattern* entity);

    EntityPattern* getEntity() const { return _entity; }

private:
    EntityPattern* _entity {nullptr};

    NodePattern(EntityPattern* entity);
    ~NodePattern();
};

class EdgePattern {
public:
    friend ASTContext;

    static EdgePattern* create(ASTContext* ctxt, EntityPattern* entity);

    EntityPattern* getEntity() const { return _entity; }

private:
    EntityPattern* _entity {nullptr};

    EdgePattern(EntityPattern* entity);
    ~EdgePattern();
};

class PathElement {
public:
    friend ASTContext;
    static PathElement* create(ASTContext* ctxt,
                               EdgePattern* edge,
                               NodePattern* node);

    EdgePattern* getEdge() const { return _edge; }
    NodePattern* getNode() const { return _node; }

private:
    EdgePattern* _edge {nullptr};
    NodePattern* _node {nullptr};

    PathElement(EdgePattern* edge, NodePattern* node);
    ~PathElement();
};

class PathPattern {
public:
    friend ASTContext;
    using PathElements = std::vector<PathElement*>;

    static PathPattern* create(ASTContext* ctxt);

    void setOrigin(NodePattern* node) { _origin = node; }
    NodePattern* getOrigin() const { return _origin; }

    const PathElements& elements() const { return _elements; }

    void addElement(PathElement* element);

private:
    NodePattern* _origin {nullptr};
    PathElements _elements;

    PathPattern();
    ~PathPattern();
};

}

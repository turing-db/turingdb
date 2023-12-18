#include "PathPattern.h"

#include "ASTContext.h"

using namespace db;

// EntityPattern
EntityPattern::EntityPattern(VarExpr* var,
                             TypeConstraint* typeConstr,
                             ExprConstraint* exprConstr)
    : _var(var),
    _typeConstr(typeConstr),
    _exprConstr(exprConstr)
{
}

EntityPattern::~EntityPattern() {
}

EntityPattern* EntityPattern::create(ASTContext* ctxt,
                                     VarExpr* var,
                                     TypeConstraint* typeConstr,
                                     ExprConstraint* exprConstr) {
    EntityPattern* pattern = new EntityPattern(var, typeConstr, exprConstr);
    ctxt->addEntityPattern(pattern);
    return pattern;
}

// NodePattern
NodePattern::NodePattern(EntityPattern* entity)
    : _entity(entity)
{
}

NodePattern::~NodePattern() {
}

NodePattern* NodePattern::create(ASTContext* ctxt, EntityPattern* entity) {
    NodePattern* pattern = new NodePattern(entity);
    ctxt->addNodePattern(pattern);
    return pattern;
}

// EdgePattern
EdgePattern::EdgePattern(EntityPattern* entity)
    : _entity(entity)
{
}

EdgePattern::~EdgePattern() {
}

EdgePattern* EdgePattern::create(ASTContext* ctxt, EntityPattern* entity) {
    EdgePattern* pattern = new EdgePattern(entity);
    ctxt->addEdgePattern(pattern);
    return pattern;
}

// PathElement
PathElement::PathElement(EdgePattern* edge, NodePattern* node)
    : _edge(edge), _node(node)
{
}

PathElement::~PathElement() {
}

PathElement* PathElement::create(ASTContext* ctxt,
                                 EdgePattern* edge,
                                 NodePattern* node) {
    PathElement* step = new PathElement(edge, node);
    ctxt->addPathElement(step);
    return step;
}

// PathPattern
PathPattern::PathPattern()
{
}

PathPattern::~PathPattern() {
}

PathPattern* PathPattern::create(ASTContext* ctxt) {
    PathPattern* pattern = new PathPattern();
    ctxt->addPathPattern(pattern);
    return pattern;
}

void PathPattern::addElement(PathElement* elem) {
    _elements.push_back(elem);
}

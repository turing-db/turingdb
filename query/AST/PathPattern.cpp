#include "PathPattern.h"

#include "ASTContext.h"

using namespace db;

// EntityPattern
EntityPattern::EntityPattern(VarExpr* var,
                             TypeConstraint* typeConstr,
                             ExprConstraint* exprConstr,
                             uint64_t entityID)
    : _var(var),
    _typeConstr(typeConstr),
    _exprConstr(exprConstr),
    _entityID(entityID)
{
}

EntityPattern::~EntityPattern() {
}

EntityPattern* EntityPattern::create(ASTContext* ctxt,
                                     VarExpr* var,
                                     TypeConstraint* typeConstr,
                                     ExprConstraint* exprConstr) {
    EntityPattern* pattern = new EntityPattern(var,
                                               typeConstr,
                                               exprConstr,
                                               UINT64_MAX);
    ctxt->addEntityPattern(pattern);
    return pattern;
}

EntityPattern* EntityPattern::create(ASTContext* ctxt,
                                     VarExpr* var,
                                     uint64_t entityID) {
    EntityPattern* pattern = new EntityPattern(var,
                                               nullptr,
                                               nullptr,
                                               entityID);
    ctxt->addEntityPattern(pattern);
    return pattern;
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

void PathPattern::addElement(EntityPattern* pattern) {
    _elements.push_back(pattern);
}

#include "ASTContext.h"

#include "QueryCommand.h"
#include "SelectField.h"
#include "FromTarget.h"
#include "PathPattern.h"
#include "Expr.h"
#include "TypeConstraint.h"
#include "ExprConstraint.h"

using namespace db;

ASTContext::ASTContext()
{
}

ASTContext::~ASTContext() {
    for (QueryCommand* cmd : _cmds) {
        delete cmd;
    }

    for (SelectField* field : _selectFields) {
        delete field;
    }

    for (FromTarget* target : _fromTargets) {
        delete target;
    }

    for (PathPattern* pattern : _pathPatterns) {
        delete pattern;
    }

    for (PathElement* element : _pathElements) {
        delete element;
    }

    for (EntityPattern* pattern : _entityPatterns) {
        delete pattern;
    }

    for (NodePattern* pattern : _nodePatterns) {
        delete pattern;
    }

    for (EdgePattern* pattern : _edgePatterns) {
        delete pattern;
    }

    for (TypeConstraint* constr : _typeConstraints) {
        delete constr;
    }

    for (ExprConstraint* constr : _exprConstraints) {
        delete constr;
    }

    for (Expr* expr : _expr) {
        delete expr;
    }
}

void ASTContext::addCmd(QueryCommand* cmd) {
    _cmds.push_back(cmd);
}

void ASTContext::addSelectField(SelectField* field) {
    _selectFields.push_back(field);
}

void ASTContext::addFromTarget(FromTarget* target) {
    _fromTargets.push_back(target);
}

void ASTContext::addPathPattern(PathPattern* pattern) {
    _pathPatterns.push_back(pattern);
}

void ASTContext::addPathElement(PathElement* element) {
    _pathElements.push_back(element);
}

void ASTContext::addEntityPattern(EntityPattern* pattern) {
    _entityPatterns.push_back(pattern);
}

void ASTContext::addNodePattern(NodePattern* pattern) {
    _nodePatterns.push_back(pattern);
}

void ASTContext::addEdgePattern(EdgePattern* pattern) {
    _edgePatterns.push_back(pattern);
}

void ASTContext::addExpr(Expr* expr) {
    _expr.push_back(expr);
}

void ASTContext::addTypeConstraint(TypeConstraint* constr) {
    _typeConstraints.push_back(constr);
}

void ASTContext::addExprConstraint(ExprConstraint* constr) {
    _exprConstraints.push_back(constr);
}

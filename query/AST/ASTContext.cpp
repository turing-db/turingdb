#include "ASTContext.h"

#include "Expr.h"
#include "ExprConstraint.h"
#include "MatchTarget.h"
#include "PathPattern.h"
#include "QueryCommand.h"
#include "ReturnField.h"
#include "ReturnProjection.h"
#include "TypeConstraint.h"
#include "VarDecl.h"

using namespace db;

ASTContext::ASTContext()
{
}

ASTContext::~ASTContext() {
    for (QueryCommand* cmd : _cmds) {
        delete cmd;
    }

    for (ReturnField* field : _returnFields) {
        delete field;
    }

    for (MatchTarget* target : _matchTargets) {
        delete target;
    }

    for (PathPattern* pattern : _pathPatterns) {
        delete pattern;
    }

    for (EntityPattern* pattern : _entityPatterns) {
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

    for (VarDecl* decl : _varDecls) {
        delete decl;
    }

    for (ReturnProjection* proj : _returnProjections) {
        delete proj;
    }
}

void ASTContext::addCmd(QueryCommand* cmd) {
    _cmds.push_back(cmd);
}

void ASTContext::addReturnField(ReturnField* field) {
    _returnFields.push_back(field);
}

void ASTContext::addMatchTarget(MatchTarget* target) {
    _matchTargets.push_back(target);
}

void ASTContext::addPathPattern(PathPattern* pattern) {
    _pathPatterns.push_back(pattern);
}

void ASTContext::addEntityPattern(EntityPattern* pattern) {
    _entityPatterns.push_back(pattern);
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

void ASTContext::addVarDecl(VarDecl* decl) {
    _varDecls.push_back(decl);
}

void ASTContext::addReturnProjection(ReturnProjection* proj) {
    _returnProjections.push_back(proj);
}

#pragma once

#include <vector>

#include "CreateTarget.h"

namespace db {

class QueryCommand;
class ReturnField;
class MatchTarget;
class CreateTarget;
class CreateTargets;
class PathPattern;
class EntityPattern;
class TypeConstraint;
class ExprConstraint;
class Expr;
class VarDecl;
class ReturnProjection;

class ASTContext {
public:
    friend QueryCommand;
    friend ReturnField;
    friend MatchTarget;
    friend CreateTarget;
    friend CreateTargets;
    friend PathPattern;
    friend EntityPattern;
    friend TypeConstraint;
    friend ExprConstraint;
    friend Expr;
    friend VarDecl;
    friend ReturnProjection;

    ASTContext();
    ASTContext(ASTContext&& other) = delete;
    ASTContext(const ASTContext& other) = delete;
    ASTContext& operator=(ASTContext&& other) = delete;
    ASTContext& operator=(const ASTContext& other) = delete;

    ~ASTContext();

    QueryCommand* getRoot() const { return _root; }

    void setRoot(QueryCommand* cmd) { _root = cmd; }

    bool hasError() const { return _isError; }
    void setError(bool hasError) { _isError = hasError; }

private:
    bool _isError {false};
    QueryCommand* _root {nullptr};
    std::vector<QueryCommand*> _cmds;
    std::vector<ReturnField*> _returnFields;
    std::vector<MatchTarget*> _matchTargets;
    std::vector<PathPattern*> _pathPatterns;
    std::vector<EntityPattern*> _entityPatterns;
    std::vector<TypeConstraint*> _typeConstraints;
    std::vector<ExprConstraint*> _exprConstraints;
    std::vector<CreateTarget*> _createTargets;
    std::vector<CreateTargets*> _createTargetVectors;
    std::vector<Expr*> _expr;
    std::vector<VarDecl*> _varDecls;
    std::vector<ReturnProjection*> _returnProjections;

    void addCmd(QueryCommand* cmd);
    void addReturnField(ReturnField* field);
    void addMatchTarget(MatchTarget* target);
    void addCreateTarget(CreateTarget* target);
    void addCreateTargets(CreateTargets* targets);
    void addPathPattern(PathPattern* pattern);
    void addEntityPattern(EntityPattern* pattern);
    void addTypeConstraint(TypeConstraint* constr);
    void addExprConstraint(ExprConstraint* constr);
    void addExpr(Expr* expr);
    void addVarDecl(VarDecl* decl);
    void addReturnProjection(ReturnProjection* proj);
};

} // namespace db

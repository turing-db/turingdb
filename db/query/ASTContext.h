#pragma once

#include <vector>

namespace db {

class QueryCommand;
class SelectField;
class FromTarget;
class PathPattern;
class PathElement;
class EntityPattern;
class NodePattern;
class EdgePattern;
class TypeConstraint;
class ExprConstraint;
class Expr;

class ASTContext {
public:
    friend QueryCommand;
    friend SelectField;
    friend FromTarget;
    friend PathPattern;
    friend PathElement;
    friend EntityPattern;
    friend NodePattern;
    friend EdgePattern;
    friend TypeConstraint;
    friend ExprConstraint;
    friend Expr;

    ASTContext();
    ~ASTContext();

    QueryCommand* getRoot() const { return _root; } 

    void setRoot(QueryCommand* cmd) { _root = cmd; }

    bool hasError() const { return _isError; }
    void setError(bool hasError) { _isError = hasError; }

private:
    bool _isError {false};
    QueryCommand* _root {nullptr};
    std::vector<QueryCommand*> _cmds;
    std::vector<SelectField*> _selectFields;
    std::vector<FromTarget*> _fromTargets;
    std::vector<PathPattern*> _pathPatterns;
    std::vector<PathElement*> _pathElements;
    std::vector<EntityPattern*> _entityPatterns;
    std::vector<NodePattern*> _nodePatterns;
    std::vector<EdgePattern*> _edgePatterns;
    std::vector<TypeConstraint*> _typeConstraints;
    std::vector<ExprConstraint*> _exprConstraints;
    std::vector<Expr*> _expr;

    void addCmd(QueryCommand* cmd);
    void addSelectField(SelectField* field);
    void addFromTarget(FromTarget* target);
    void addPathElement(PathElement* element);
    void addPathPattern(PathPattern* pattern);
    void addEntityPattern(EntityPattern* pattern);
    void addNodePattern(NodePattern* pattern);
    void addEdgePattern(EdgePattern* pattern);
    void addTypeConstraint(TypeConstraint* constr);
    void addExprConstraint(ExprConstraint* constr);
    void addExpr(Expr* expr);
};

} 

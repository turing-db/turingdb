#pragma once

#include "Expr.h"

namespace db {

class QualifiedName;
class CypherAST;
class VarDecl;

class PropertyExpr : public Expr {
public:
    const QualifiedName* getFullName() const { return _fullName; }

    static PropertyExpr* create(CypherAST* ast, QualifiedName* name);

    VarDecl* getEntityVarDecl() const { return _entityDecl; }

    std::string_view getPropName() const { return _propName; }

    void setEntityVarDecl(VarDecl* entityDecl) { _entityDecl = entityDecl; }

    void setPropertyName(std::string_view propName) { _propName = propName; }

    bool isStringTableHeaderAccess() const { return _stringTableHeaderAccess; }
    void setStringTableHeaderAccess(bool csvHeaderAccess) {
        _stringTableHeaderAccess = csvHeaderAccess;
    }

private:
    QualifiedName* _fullName {nullptr};
    VarDecl* _entityDecl {nullptr};
    std::string_view _propName;
    bool _stringTableHeaderAccess {false};

    PropertyExpr(QualifiedName* name)
        : Expr(Kind::PROPERTY),
        _fullName(name)
    {
    }

    ~PropertyExpr() override;
};

}

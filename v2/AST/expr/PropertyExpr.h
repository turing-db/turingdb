#pragma once

#include "Expr.h"
#include "ID.h"

namespace db::v2 {

class QualifiedName;
class CypherAST;
class VarDecl;

class PropertyExpr : public Expr {
public:
    const QualifiedName* getName() const { return _name; }

    static PropertyExpr* create(CypherAST* ast, QualifiedName* name);

    VarDecl* getDecl() const { return _decl; }

    PropertyTypeID getPropertyType() const { return _propTypeID; }

    void setDecl(VarDecl* decl) { _decl = decl; }

    void setPropertyType(PropertyTypeID propTypeID) { _propTypeID = propTypeID; }

private:
    QualifiedName* _name {nullptr};
    VarDecl* _decl {nullptr};
    PropertyTypeID _propTypeID;

    PropertyExpr(QualifiedName* name)
        : Expr(Kind::PROPERTY),
        _name(name)
    {
    }

    ~PropertyExpr() override;
};

}

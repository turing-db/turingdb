#pragma once

#include "Expr.h"

namespace db::v2 {

class QualifiedName;
class CypherAST;
class VarDecl;

class PropertyExpr : public Expr {
public:
    const QualifiedName* getName() const { return _name; }

    static PropertyExpr* create(CypherAST* ast, QualifiedName* name);

    VarDecl* getDecl() const { return _decl; }

    void setDecl(VarDecl* decl) { _decl = decl; }

private:
    QualifiedName* _name {nullptr};
    VarDecl* _decl {nullptr};

    PropertyExpr(QualifiedName* name)
        : Expr(Kind::PROPERTY),
        _name(name)
    {
    }

    ~PropertyExpr() override;
};

}

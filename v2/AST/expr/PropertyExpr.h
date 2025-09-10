#pragma once

#include "Expr.h"

#include "QualifiedName.h"

namespace db::v2 {

class VarDecl;
class CypherAST;

class PropertyExpr : public Expr {
public:
    bool hasDecl() const { return _decl != nullptr; }

    const VarDecl& decl() const { return *_decl; }
    const QualifiedName& name() const { return _name; }

    void setDecl(const VarDecl* decl) { _decl = decl; }

    static PropertyExpr* create(CypherAST* ast, const QualifiedName& name);

private:
    QualifiedName _name;
    const VarDecl* _decl {nullptr};

    PropertyExpr(const QualifiedName& name)
        : Expr(Kind::PROPERTY),
        _name(name)
    {
    }

    ~PropertyExpr() override;
};

}

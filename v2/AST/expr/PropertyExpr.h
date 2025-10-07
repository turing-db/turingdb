#pragma once

#include "Expr.h"

namespace db::v2 {

class QualifiedName;
class CypherAST;
class VarDecl;

class PropertyExpr : public Expr {
public:
    const QualifiedName* getFullName() const { return _fullName; }

    static PropertyExpr* create(CypherAST* ast, QualifiedName* name);

    VarDecl* getDecl() const { return _decl; }

    std::string_view getPropName() const { return _propName; }

    void setDecl(VarDecl* decl) { _decl = decl; }

    void setPropertyName(std::string_view propName) { _propName = propName; }

private:
    QualifiedName* _fullName {nullptr};
    VarDecl* _decl {nullptr};
    std::string_view _propName;

    PropertyExpr(QualifiedName* name)
        : Expr(Kind::PROPERTY),
        _fullName(name)
    {
    }

    ~PropertyExpr() override;
};

}

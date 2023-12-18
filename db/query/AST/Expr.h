#pragma once

#include <string>

namespace db {

class ASTContext;
class VarDecl;

class Expr {
public:
    friend ASTContext;

    enum Kind {
        EK_VAR_EXPR    
    };

    virtual Kind getKind() const = 0;

protected:
    Expr() = default;
    virtual ~Expr();
    void postCreate(ASTContext* ctxt);
};

class VarExpr : public Expr {
public:
    static VarExpr* create(ASTContext* ctxt, const std::string& varName);

    Expr::Kind getKind() const override { return EK_VAR_EXPR; }

    const std::string& getName() const { return _varName; }

    VarDecl* getDecl() const { return _decl; }

    void setDecl(VarDecl* decl) { _decl = decl; }

private:
    const std::string _varName;
    VarDecl* _decl {nullptr};

    VarExpr(const std::string& varName);
    ~VarExpr();
};

}

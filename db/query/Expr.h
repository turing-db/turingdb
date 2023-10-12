#pragma once

#include <string>

namespace db {

class ASTContext;

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

private:
    const std::string _varName;

    VarExpr(const std::string& varName);
    ~VarExpr();
};

}

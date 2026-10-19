#pragma once

#include "Expr.h"

namespace db {

class CypherAST;
class Parameter;
class ParameterDecl;

class ParameterExpr : public Expr {
public:
    static ParameterExpr* create(CypherAST* ast, Parameter* parameter);

    Parameter* getParameter() const { return _parameter; }

    ParameterDecl* getDecl() const { return _decl; }

    void setDecl(ParameterDecl* decl) { _decl = decl; }

private:
    Parameter* _parameter {nullptr};
    ParameterDecl* _decl {nullptr};

    explicit ParameterExpr(Parameter* parameter)
        : Expr(Kind::PARAMETER),
        _parameter(parameter)
    {
    }

    ~ParameterExpr() override;
};

}

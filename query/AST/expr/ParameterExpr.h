#pragma once

#include "Expr.h"

namespace db {

class CypherAST;
class Parameter;

class ParameterExpr : public Expr {
public:
    static ParameterExpr* create(CypherAST* ast, Parameter* parameter);

    Parameter* getParameter() const { return _parameter; }

private:
    Parameter* _parameter {nullptr};

    explicit ParameterExpr(Parameter* parameter)
        : Expr(Kind::PARAMETER),
        _parameter(parameter)
    {
    }

    ~ParameterExpr() override;
};

}

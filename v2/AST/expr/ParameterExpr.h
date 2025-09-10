#pragma once

#include "Expr.h"
#include "Parameter.h"

namespace db::v2 {

class CypherAST;

class ParameterExpr : public Expr {
public:
    static ParameterExpr* create(CypherAST* ast, const Parameter& param);

    const Parameter& param() const { return _param; }

private:
    Parameter _param;

    ParameterExpr(const Parameter& param)
        : Expr(Kind::PARAMETER),
        _param(param)
    {
    }

    ~ParameterExpr() override;
};

}

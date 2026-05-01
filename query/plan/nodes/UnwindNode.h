#pragma once

#include "PlanGraphNode.h"

#include "decl/EvaluatedType.h"
#include "decl/VarDecl.h"

namespace db {

class Expr;
class Symbol;

class UnwindNode final : public PlanGraphNode {
public:
    UnwindNode(const Expr* arg, const VarDecl* var)
        : PlanGraphNode(PlanGraphOpcode::UNWIND),
        _arg(arg),
        _var(var)
    {
    }

    UnwindNode(const Expr* arg, const VarDecl* var, EvaluatedType homogeneity)
        : PlanGraphNode(PlanGraphOpcode::UNWIND),
        _arg(arg),
        _var(var),
        _homogeneity(homogeneity)
    {
    }

    const Expr* arg() const { return _arg; }
    const VarDecl* var() const { return _var; }

private:
    const Expr* _arg {nullptr};
    const VarDecl* _var {nullptr};

    std::optional<EvaluatedType> _homogeneity;
};

}

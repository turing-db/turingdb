#pragma once

#include "PlanGraphNode.h"

#include "decl/EvaluatedType.h"
#include "decl/VarDecl.h"

namespace db {

class Expr;
class Symbol;

class UnwindNode final : public PlanGraphNode {
public:
    UnwindNode(PlanGraphNodeID id, const Expr* arg, const VarDecl* var)
        : PlanGraphNode(id, PlanGraphOpcode::UNWIND),
        _arg(arg),
        _var(var)
    {
    }

    UnwindNode(PlanGraphNodeID id, const Expr* arg, const VarDecl* var, EvaluatedType homogeneity)
        : PlanGraphNode(id, PlanGraphOpcode::UNWIND),
        _arg(arg),
        _var(var),
        _homogeneity(homogeneity)
    {
    }

    const Expr* arg() const { return _arg; }
    const VarDecl* var() const { return _var; }

    bool isHomogeneous() const { return _homogeneity.has_value(); }
    std::optional<EvaluatedType> homogeneity() const { return _homogeneity; }

private:
    const Expr* _arg {nullptr};
    const VarDecl* _var {nullptr};

    std::optional<EvaluatedType> _homogeneity;
};

}

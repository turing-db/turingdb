#pragma once

#include "decl/VarDecl.h"
#include "nodes/PlanGraphNode.h"

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

    const Expr* arg() const { return _arg; }
    const VarDecl* var() const { return _var; }

private:
    const Expr* _arg {nullptr};
    const VarDecl* _var {nullptr};
};

}

#pragma once

#include "nodes/PlanGraphNode.h"

namespace db {

class Expr;
class Symbol;

class UnwindNode final : public PlanGraphNode {
public:
    UnwindNode(const Expr* arg, const Symbol* sym)
        : PlanGraphNode(PlanGraphOpcode::UNWIND),
        _arg(arg),
        _sym(sym)
    {
    }

    const Expr* arg() const { return _arg; }
    const Symbol* sym() const { return _sym; }

private:
    const Expr* _arg {nullptr};
    const Symbol* _sym {nullptr};
};

}

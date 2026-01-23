#pragma once

#include "PlanGraphNode.h"

namespace db {

class VarDecl;

class VarNode : public PlanGraphNode {
public:
    VarNode(const VarDecl* varDecl)
        : PlanGraphNode(PlanGraphOpcode::VAR),
        _varDecl(varDecl)
    {
    }

    const VarDecl* getVarDecl() const { return _varDecl; }

private:
    const VarDecl* _varDecl {nullptr};
};

}

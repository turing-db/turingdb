#pragma once

#include "PlanGraphNode.h"

namespace db::v2 {

class VarDecl;

class VarNode : public PlanGraphNode {
public:
    explicit VarNode(const VarDecl* varDecl)
        : PlanGraphNode(PlanGraphOpcode::VAR),
        _varDecl(varDecl)
    {
    }

    const VarDecl* getVarDecl() const { return _varDecl; }

private:
    const VarDecl* _varDecl {nullptr};
};


}

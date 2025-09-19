#pragma once

#include "PlanGraphNode.h"

namespace db::v2 {

class VarDecl;

class VarNode : public PlanGraphNode {
public:
    VarNode(const VarDecl* varDecl, uint32_t declOdrer)
        : PlanGraphNode(PlanGraphOpcode::VAR),
        _varDecl(varDecl),
        _declOdrer(declOdrer)
    {
    }

    uint32_t getDeclOdrer() const { return _declOdrer; }

    const VarDecl* getVarDecl() const { return _varDecl; }

private:
    const VarDecl* _varDecl {nullptr};
    uint32_t _declOdrer {0};
};


}

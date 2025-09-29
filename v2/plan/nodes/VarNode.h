#pragma once

#include "PlanGraphNode.h"

namespace db::v2 {

class VarDecl;

class VarNode : public PlanGraphNode {
public:
    VarNode(const VarDecl* varDecl, uint32_t declOrder)
        : PlanGraphNode(PlanGraphOpcode::VAR),
        _varDecl(varDecl),
        _declOrder(declOrder)
    {
    }

    void setDeclOrder(uint32_t declOrder) { _declOrder = declOrder; }

    uint32_t getDeclOrder() const { return _declOrder; }
    const VarDecl* getVarDecl() const { return _varDecl; }

private:
    const VarDecl* _varDecl {nullptr};
    uint32_t _declOrder {0};
};


}

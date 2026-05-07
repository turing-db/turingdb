#pragma once

#include "nodes/PlanGraphNode.h"

namespace db {

class VarDecl;

class VarDeclProviderNode : public PlanGraphNode {
public:
    ~VarDeclProviderNode() override;
    const VarDecl* getVarDecl() const { return _varDecl; }

protected:

    VarDeclProviderNode(PlanGraphNodeID id, PlanGraphOpcode opcode, const VarDecl* varDecl)
    : PlanGraphNode(id, opcode),
    _varDecl(varDecl)
    {
    }

    const VarDecl* _varDecl {nullptr};
};
}

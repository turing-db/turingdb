#pragma once

#include "VarDeclProviderNode.h"

namespace db {

class VarDecl;

class VarNode : public VarDeclProviderNode{
public:
    VarNode(PlanGraphNodeID id, const VarDecl* varDecl)
        : VarDeclProviderNode(id, PlanGraphOpcode::VAR, varDecl)
    {
    }
};

}

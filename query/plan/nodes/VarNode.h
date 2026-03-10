#pragma once

#include "VarDeclProviderNode.h"

namespace db {

class VarDecl;

class VarNode : public VarDeclProviderNode{
public:
    VarNode(const VarDecl* varDecl)
        : VarDeclProviderNode(PlanGraphOpcode::VAR, varDecl)
    {
    }
};

}

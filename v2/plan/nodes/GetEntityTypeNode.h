#pragma once

#include "PlanGraphNode.h"

namespace db::v2 {

class VarDecl;
class SymbolChain;

class GetEntityTypeNode : public PlanGraphNode {
public:
    explicit GetEntityTypeNode(const VarDecl* decl)
        : PlanGraphNode(PlanGraphOpcode::GET_ENTITY_TYPE),
        _decl(decl)
    {
    }

    const VarDecl* getVarDecl() const { return _decl; }

private:
    const VarDecl* _decl {nullptr};
};

}

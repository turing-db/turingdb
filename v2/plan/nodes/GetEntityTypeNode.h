#pragma once

#include "PlanGraphNode.h"

namespace db::v2 {

class VarDecl;
class SymbolChain;

class GetEntityTypeNode : public PlanGraphNode {
public:
    explicit GetEntityTypeNode(const VarDecl* decl)
        : PlanGraphNode(PlanGraphOpcode::GET_ENTITY_TYPE),
        _entityDecl(decl)
    {
    }

    const VarDecl* getEntityDecl() const { return _entityDecl; }
    const VarDecl* getEntityTypeDecl() const { return _entityTypeDecl; }

    void setEntityTypeDecl(const VarDecl* entityTypeDecl) {
        _entityTypeDecl = entityTypeDecl;
    }

private:
    const VarDecl* _entityDecl {nullptr};
    const VarDecl* _entityTypeDecl {nullptr};
};

}

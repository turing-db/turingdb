#pragma once

#include "PlanGraphNode.h"

namespace db::v2 {

class Expr;
class VarDecl;
class SymbolChain;

class GetEntityTypeNode : public PlanGraphNode {
public:
    explicit GetEntityTypeNode()
        : PlanGraphNode(PlanGraphOpcode::GET_ENTITY_TYPE)
    {
    }

    void setEntityVarDecl(const VarDecl* entityDecl) {
        _entityDecl = entityDecl;
    }

    void setExpr(const Expr* expr) {
        _expr = expr;
    }

    const VarDecl* getEntityVarDecl() const { return _entityDecl; }
    const Expr* getExpr() const { return _expr; }

private:
    const VarDecl* _entityDecl {nullptr};
    const Expr* _expr {nullptr};
};

}

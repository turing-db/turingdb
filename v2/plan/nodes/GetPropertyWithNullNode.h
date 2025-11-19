#pragma once

#include "PlanGraphNode.h"

namespace db::v2 {

class Expr;
class VarDecl;

class GetPropertyWithNullNode : public PlanGraphNode {
public:
    GetPropertyWithNullNode(std::string_view propName)
        : PlanGraphNode(PlanGraphOpcode::GET_PROPERTY_WITH_NULL),
        _propName(propName)
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
    const std::string_view& getPropName() const { return _propName; }

private:
    const VarDecl* _entityDecl {nullptr};
    const Expr* _expr {nullptr};
    std::string_view _propName;
};

}

#pragma once

#include "PlanGraphNode.h"

namespace db::v2 {

class VarDecl;

class GetPropertyNode : public PlanGraphNode {
public:
    GetPropertyNode(const VarDecl* decl, std::string_view propName)
        : PlanGraphNode(PlanGraphOpcode::GET_PROPERTY),
        _entityDecl(decl),
        _propName(propName)
    {
    }

    void setPropertyDecl(const VarDecl* propDecl) {
        _propDecl = propDecl;
    }

    const VarDecl* getEntityDecl() const { return _entityDecl; }
    const VarDecl* getPropertyDecl() const { return _propDecl; }
    const std::string_view& getPropName() const { return _propName; }

private:
    const VarDecl* _entityDecl {nullptr};
    const VarDecl* _propDecl {nullptr};
    std::string_view _propName;
};

}

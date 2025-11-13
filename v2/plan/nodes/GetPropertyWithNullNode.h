#pragma once

#include "PlanGraphNode.h"

namespace db::v2 {

class VarDecl;

class GetPropertyWithNullNode : public PlanGraphNode {
public:
    GetPropertyWithNullNode(const VarDecl* decl, std::string_view propName)
        : PlanGraphNode(PlanGraphOpcode::GET_PROPERTY_WITH_NULL),
        _decl(decl),
        _propName(propName)
    {
    }

    const VarDecl* getVarDecl() const { return _decl; }
    const std::string_view& getPropName() const { return _propName; }

private:
    const VarDecl* _decl {nullptr};
    std::string_view _propName;
};

}

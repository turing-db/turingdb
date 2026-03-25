#pragma once

#include "nodes/PlanGraphNode.h"

namespace db {

class CreatePropertyIndexNode : public PlanGraphNode {
public:
    explicit CreatePropertyIndexNode(std::string_view propertyName)
    : PlanGraphNode(PlanGraphOpcode::CREATE_PROPERTY_INDEX),
     _propertyName(propertyName)
    {
    }

    std::string_view propertyName() const { return _propertyName; }

private:
    std::string_view _propertyName;
};

}

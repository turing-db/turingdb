#pragma once

#include "PlanGraphNode.h"

namespace db {

class InstallExtensionNode : public PlanGraphNode {
public:
    InstallExtensionNode(PlanGraphNodeID id, std::string_view extensionName)
        : PlanGraphNode(id, PlanGraphOpcode::INSTALL_EXTENSION),
        _extensionName(extensionName)
    {
    }

    std::string_view getExtensionName() const { return _extensionName; }

private:
    std::string_view _extensionName;
};

}

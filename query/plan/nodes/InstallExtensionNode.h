#pragma once

#include "PlanGraphNode.h"

namespace db {

class InstallExtensionNode : public PlanGraphNode {
public:
    explicit InstallExtensionNode(std::string_view extensionName)
        : PlanGraphNode(PlanGraphOpcode::INSTALL_EXTENSION),
        _extensionName(extensionName)
    {
    }

    std::string_view getExtensionName() const { return _extensionName; }

private:
    std::string_view _extensionName;
};

}

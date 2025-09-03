#pragma once

#include <memory>
#include <set>

#include "ID.h"

namespace db {

class NodeContainer;

class NodeContainerModifier {
public:
    [[nodiscard]] static std::unique_ptr<NodeContainer> deleteNodes(const NodeContainer& original,
                                                                    const std::set<NodeID> toDelete);
};
    
}

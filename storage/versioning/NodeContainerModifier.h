#pragma once

#include <memory>
#include <set>

#include "ID.h"

namespace db {

class NodeContainer;

class NodeContainerModifier {
public:
    /**
     * @brief Creates and returns a new NodeContainer which is equivalent to @ref original
     * with the nodes with NodeIDs specified in @ref toDelete removed
     */
    [[nodiscard]] static std::unique_ptr<NodeContainer> deleteNodes(const NodeContainer& original,
                                                                    const std::set<NodeID>& toDelete);
};
}

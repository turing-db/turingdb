#pragma once

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
    static size_t deleteNodes(const NodeContainer& original, NodeContainer& newContainer,
                              const std::set<NodeID>& toDelete);
};
}

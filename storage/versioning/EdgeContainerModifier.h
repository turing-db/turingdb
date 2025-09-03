#pragma once

#include <memory>
#include <set>

#include "ID.h"

namespace db {

class EdgeContainer;

class EdgeContainerModifier {
public:
    /**
     * @brief Creates and returns a new EdgeContainer which is equivalent to @ref original,
     * but with edges which are incident to a node in @ref deletedNodes removed.
     */
    [[nodiscard]] static std::unique_ptr<EdgeContainer> deleteEdgesFromNodes(const EdgeContainer& original,
                                                                    const std::set<NodeID> deletedNodes);
};
}

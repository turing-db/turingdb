#pragma once

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
    static size_t deleteEdges(const EdgeContainer& original,
                                            EdgeContainer& newContainer,
                                            const std::set<EdgeID>& toDelete);
};
}

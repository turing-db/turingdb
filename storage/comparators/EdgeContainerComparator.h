#pragma once

#include <range/v3/view/zip.hpp>

#include "datapart/EdgeContainer.h"

namespace db {

class EdgeContainerComparator {
public:
    [[nodiscard]] static bool same(const EdgeContainer& a, const EdgeContainer& b) {
        namespace rv = ranges::views;

        if (a.size() != b.size()) {
            return false;
        }

        if (a.getFirstEdgeID() != b.getFirstEdgeID()) {
            return false;
        }

        for (const auto& [outA, outB] : rv::zip(a.getOuts(), b.getOuts())) {
            if (outA._edgeID != outB._edgeID) {
                return false;
            }

            if (outA._nodeID != outB._nodeID) {
                return false;
            }

            if (outA._otherID != outB._otherID) {
                return false;
            }

            if (outA._edgeTypeID != outB._edgeTypeID) {
                return false;
            }
        }

        return true;
    }
};

}

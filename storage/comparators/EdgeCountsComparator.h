#pragma once

#include <range/v3/view/zip.hpp>

#include "datapart/EdgeCounts.h"

namespace db {

class EdgeCountsComparator {
public:
    [[nodiscard]] static bool same(const EdgeCounts& a, const EdgeCounts& b) {
        namespace rv = ranges::views;

        const std::span<const EdgeCount> countsA = a.getCounts();
        const std::span<const EdgeCount> countsB = b.getCounts();

        if (countsA.size() != countsB.size()) {
            return false;
        }

        for (const auto& [countA, countB] : rv::zip(countsA, countsB)) {
            const bool sameTriple = countA._edgeTypeID == countB._edgeTypeID
                                 && countA._sourceLabelSet.getID() == countB._sourceLabelSet.getID()
                                 && countA._targetLabelSet.getID() == countB._targetLabelSet.getID();

            if (!sameTriple || countA._count != countB._count) {
                return false;
            }
        }

        return true;
    }
};

}

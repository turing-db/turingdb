#pragma once

#include "NodeContainer.h"

namespace db {

class NodeContainerComparator {
public:
    [[nodiscard]] static bool same(const NodeContainer& a, const NodeContainer& b) {
        if (a.size() != b.size()) {
            return false;
        }

        if (a.getFirstNodeID() != b.getFirstNodeID()) {
            return false;
        }

        const auto& indexerA = a.getLabelSetIndexer();
        const auto& indexerB = b.getLabelSetIndexer();

        if (indexerA.size() != indexerB.size()) {
            return false;
        }

        for (const auto [labelsetA, rangeA] : indexerA) {
            const auto itB = indexerB.find(labelsetA);

            if (itB == indexerB.end()) {
                return false;
            }

            const auto& [labelsetB, rangeB] = *itB;

            if (labelsetA != labelsetB) {
                return false;
            }

            if (rangeA._first != rangeB._first) {
                return false;
            }

            if (rangeA._count != rangeB._count) {
                return false;
            }
        }

        return true;
    }
};

}

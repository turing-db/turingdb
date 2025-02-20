#pragma once

#include <range/v3/view/zip.hpp>

#include "indexers/EdgeIndexer.h"

namespace db {

class EdgeIndexerComparator {
public:
    [[nodiscard]] static bool same(const EdgeIndexer& a, const EdgeIndexer& b) {
        namespace rv = ranges::views;

        if (a.getFirstEdgeID() != b.getFirstEdgeID()) {
            return false;
        }

        if (a.getFirstNodeID() != b.getFirstNodeID()) {
            return false;
        }

        const auto& outIndexerA = a.getOutsByLabelSet();
        const auto& outIndexerB = b.getOutsByLabelSet();

        if (outIndexerA.size() != outIndexerB.size()) {
            return false;
        }

        for (const auto& [labelsetA, rangesA] : outIndexerA) {
            const auto itB = outIndexerB.find(labelsetA);
            if (itB == outIndexerB.end()) {
                return false;
            }

            const auto& [labelsetB, rangesB] = *itB;

            if (rangesA.size() != rangesB.size()) {
                return false;
            }

            for (const auto& [rA, rB] : rv::zip(rangesA, rangesB)) {
                if (rA.size() != rB.size()) {
                    return false;
                }

                if (rA.size() == 0) {
                    continue;
                }

                if (rA.back()._edgeID != rB.back()._edgeID) {
                    return false;
                }

                if (rA.front()._edgeID != rB.front()._edgeID) {
                    return false;
                }
            }
        }

        const auto& inIndexerA = a.getInsByLabelSet();
        const auto& inIndexerB = b.getInsByLabelSet();

        if (inIndexerA.size() != inIndexerB.size()) {
            return false;
        }

        for (const auto& [labelsetA, rangesA] : inIndexerA) {
            const auto itB = inIndexerB.find(labelsetA);
            if (itB == inIndexerB.end()) {
                return false;
            }

            const auto& [labelsetB, rangesB] = *itB;

            if (rangesA.size() != rangesB.size()) {
                return false;
            }

            for (const auto& [rA, rB] : rv::zip(rangesA, rangesB)) {
                if (rA.size() != rB.size()) {
                    return false;
                }

                if (rA.size() == 0) {
                    continue;
                }

                if (rA.back()._edgeID != rB.back()._edgeID) {
                    return false;
                }

                if (rA.front()._edgeID != rB.front()._edgeID) {
                    return false;
                }
            }
        }

        return true;
    }
};

}

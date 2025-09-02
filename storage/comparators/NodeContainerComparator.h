#pragma once

#include "NodeContainer.h"
#include "spdlog/spdlog.h"
#include <numeric>

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

    [[nodiscard]] static bool correctlyMerged(const NodeContainer& merged,
                                              const std::vector<const NodeContainer*>& elements) {
        // Assumes a given node should only be stored in a single container
        // of @ref elements
        size_t totalSize = std::accumulate(
            elements.cbegin(), elements.cend(), size_t {0},
            [](size_t acc, const NodeContainer* cont) { return acc + cont->size(); });

        if (merged.size() != totalSize) {
            spdlog::error(
                "Merged NodeContainer had size {} whilst sum of elements had size {}",
                merged.size(), totalSize);
            return false;
        }

        // TODO: Some kind of firstNodeID check

        { // Check LabelSetIndexer

            // Calculate total count of all containers labelset entries
            size_t totalRangeCount {0};

            const auto mergedIndexer = merged.getLabelSetIndexer();

            for (const NodeContainer* cont : elements) {
                const auto& idx = cont->getLabelSetIndexer();

                for (const auto& [labelSet, range] : idx) {
                    totalRangeCount += range._count;

                    const auto mergedIt = mergedIndexer.find(labelSet);
                    if (mergedIt == mergedIndexer.end()) {
                        spdlog::error("Merged NodeContainer did not contain labelset: {}", labelSet);
                        return false;
                    }
                }
            }

            // Calculate total count of merged container labelset entries
            size_t mergedRangeCount = std::accumulate(
                mergedIndexer.begin(), mergedIndexer.end(), size_t {0},
                [](size_t acc, auto entry) { return acc + entry.second._count; });

            if (mergedRangeCount != totalRangeCount) {
                spdlog::error("Range count of merged NodeContainer was {} whilst total "
                              "of elements was {}",
                              mergedRangeCount, totalRangeCount);
                return false;
            }
        }

        return true;
    }
};

}

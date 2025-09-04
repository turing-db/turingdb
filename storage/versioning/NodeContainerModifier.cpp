#include "NodeContainerModifier.h"

#include <cstdint>
#include <set>

#include <range/v3/view/enumerate.hpp>
#include <range/v3/view/iota.hpp>
#include <spdlog/fmt/bundled/format.h>

#include "NodeContainer.h"
#include "NodeRange.h"
#include "TuringException.h"
#include "indexers/LabelSetIndexer.h"

namespace rg = ranges;
namespace rv = rg::views;

namespace {
    // Enumerate from starting value @ref start (inclusive)
    template <typename Rng>
    auto enumerate_from(std::ptrdiff_t start, Rng&& rng) {
        return rv::zip(rv::iota(start), std::forward<Rng>(rng));
    }
}

using namespace db;

size_t NodeContainerModifier::deleteNodes(const NodeContainer& original,
                                        NodeContainer& newContainer,
                                        const std::set<NodeID>& toDelete) {
    size_t numNodesDeleted {0};

    uint64_t ogFstID = original.getFirstNodeID().getValue();
    size_t ogSize = original.size();

    // Bounds check to ensure all nodes that are to be deleted are in this DP
    NodeID smallestNodeToDelete = *toDelete.cbegin();
    NodeID largestNodeToDelete = *toDelete.crbegin();
    if (smallestNodeToDelete < ogFstID) {
        throw TuringException(fmt::format("Node with ID {} is not in this datapart; "
                                          "smallest ID in this datapart is {}",
                                          smallestNodeToDelete, ogFstID));
    }
    if (largestNodeToDelete > ogFstID + ogSize - 1) {
        throw TuringException(fmt::format("Node with ID {} is not in this datapart; "
                                          "largest ID in this datapart is {}",
                                          largestNodeToDelete, ogFstID + ogSize - 1));
    }

    // If entire NodeContainer was deleted, exit early
    if (original.size() == toDelete.size()) {
        return numNodesDeleted;
    }

    uint64_t newFstID = UINT64_MAX;
    auto smallestDeleted = toDelete.cbegin();

    // Calculate the new first ID
    // The new first ID is the smallest ID in the original container which is not
    // deleted

    // If the smallest ID is not deleted, then the smallest ID remains the same
    if (*smallestDeleted != ogFstID) {
        newFstID = ogFstID;
    } else { // Otherwise, find the smallest ID which is not deleted
        // Never risk of @ref smallestDeleted > end() since in that case all nodes are
        // deleted, which is explicitly checked above
        while (*++smallestDeleted == ++newFstID)
            ;
    }

    auto ogRecords = original.records();
    // Create the new NodeRecords vector
    std::vector<NodeRecord> newRecords;
    newRecords.reserve(ogRecords.size() - toDelete.size());

    // Populate it with only records for non-deleted nodes
    for (const auto& [id, record] : enumerate_from(ogFstID, ogRecords)) {
        if (toDelete.contains(id)) [[unlikely]] {
            numNodesDeleted++;
            continue;
        } else {
            newRecords.emplace_back(record);
        }
    }

    // Since we have only removed elements from an already sorted @ref ogRecords
    // records vector, the @ref newRecords vector will also be sorted.

    // Generate new ranges for LabelSetIndexer
    LabelSetIndexer<NodeRange> newRanges {};
    auto it = newRecords.cbegin();
    while (it != newRecords.cend()) {
        LabelSetHandle currentLblSet = it->_labelset;
        // Calculate the first NodeID to have this labelset
        size_t currentStartOffset = std::distance(newRecords.cbegin(), it) + newFstID;

        NodeRange thisRange {currentStartOffset, 0};
        // Count how many nodes share this label set, incrementing the range
        while (it != newRecords.cend() && currentLblSet == it->_labelset) {
            thisRange._count++;
            it++;
        }

        // Update the map to have this range
        newRanges[currentLblSet] = thisRange;
    }

    newContainer._firstID = newFstID;
    newContainer._nodeCount = newRecords.size();
    newContainer._nodes = newRecords;
    newContainer._ranges = newRanges;

    return numNodesDeleted;
}

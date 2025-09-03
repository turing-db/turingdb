#pragma once

#include <cstdint>
#include <range/v3/view/enumerate.hpp>
#include "NodeRange.h"
#include "TuringException.h"
#include "indexers/LabelSetIndexer.h"
#include "range/v3/view/iota.hpp"
#include <memory>
#include <set>

#include "NodeContainer.h"

namespace rg = ranges;
namespace rv = rg::views;

namespace {
    // Enumerate from a starting value @ref start (inclusive)
    template <typename Rng>
    auto enumerate_from(std::ptrdiff_t start, Rng&& rng) {
        return rv::zip(rv::iota(start), std::forward<Rng>(rng));
    }
}


namespace db {

class NodeContainerModifier {
public:
    [[nodiscard]] static std::unique_ptr<NodeContainer> deleteNode(const NodeContainer& original,
                                                                   const std::set<NodeID> toDelete) {
        for (const NodeID id : toDelete) {
            if (!original.hasEntity(id)) {
                throw TuringException(
                    "Node {} to delete does not exist in NodeContainer");
            }
        }

        // If entire NodeContainer was deleted, exit early
        if (original.size() == toDelete.size()) {
            return {};
        }
        
        // TODO: NodeRange based solution: delete ranges to reduce ID shuffling

        uint64_t ogFstID = original.getFirstNodeID().getValue();
        auto ogRecords = original.records();

        // Calculate the new first ID
        // The new first ID is the smallest ID in the original container which is not
        // deleted
         
        uint64_t newFstID = UINT64_MAX;
        auto smallestDeleted = toDelete.cbegin();

        // If the smallest ID is not deleted, then the smallest ID remains the same
        if (*smallestDeleted != ogFstID) {
            newFstID = ogFstID;
        } else {
            while (*smallestDeleted++ == newFstID++) {
                // If all IDs are deleted NodeContainer has been deleted: return nullptr
                if (smallestDeleted == toDelete.end()) {
                    return {};
                }
            }
        }


        // Create the new NodeRecords vector
        std::vector<NodeRecord> newRecords;
        newRecords.reserve(ogRecords.size() - toDelete.size());

        // Populate it with only records for non-deleted nodes
        for (const auto& [id, record] : enumerate_from(ogFstID, ogRecords)) {
            if (toDelete.contains(id)) [[unlikely]] {
                continue;
            } else {
                newRecords.emplace_back(record);
            }
        }

        // Since we have only removed elements from an already sorted @ref ogRecords
        // records vector, the @ref newRecords vector will also be sorted.

        // Generate new ranges for LabelSetIndexer
        LabelSetIndexer<NodeRange> newRanges{};
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

        // NodeContainer private constructor so use new then create unique_ptr from raw
        NodeContainer* newContainer =
            new NodeContainer(newFstID, newRecords.size(), newRanges, newRecords);
        return std::unique_ptr<NodeContainer>(newContainer);
    }
};
    
}

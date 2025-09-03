#pragma once

#include <cstdint>
#include <range/v3/view/enumerate.hpp>
#include "TuringException.h"
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
        // TODO: NodeRange based solution: delete ranges to reduce ID shuffling

        uint64_t ogFstID = original.getFirstNodeID().getValue();
        auto ogRecords = original.records();

        // Calculate the new first ID
        // The new first ID is the smallest ID in the original container which is not
        // deleted
        uint64_t newFstId = ogFstID;
        auto smallestNonDeleted = toDelete.cbegin();
        while (*smallestNonDeleted == newFstId) {
            newFstId++;
            smallestNonDeleted++;
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

        return NodeContainer::create(newFstId, newRecords);
    }
};
    
}

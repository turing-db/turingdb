#include "EdgeContainerModifier.h"

#include <cstdint>
#include <memory>
#include <range/v3/view/enumerate.hpp>
#include <range/v3/view/iota.hpp>
#include <unordered_map>

#include "EdgeContainer.h"
#include "ID.h"
#include "TuringException.h"

using namespace db;

namespace rg = ranges;
namespace rv = rg::views;

namespace {
    // Enumerate from starting value @ref start (inclusive)
    template <typename Rng>
    auto enumerate_from(std::ptrdiff_t start, Rng&& rng) {
        return rv::zip(rv::iota(start), std::forward<Rng>(rng));
    }
}

size_t EdgeContainerModifier::deleteEdges(const EdgeContainer& original,
                                          EdgeContainer& newContainer,
                                          const std::set<EdgeID>& toDelete) {
    size_t numEdgesDeleted {0};
    
    uint64_t ogFstEdgeID = original.getFirstEdgeID().getValue();
    size_t ogSize = original.size();

    EdgeID smallestEdgeToDelete = *toDelete.cbegin();
    EdgeID largestEdgeToDelete = *toDelete.crbegin();
    if (smallestEdgeToDelete < ogFstEdgeID) {
        throw TuringException(fmt::format("Edge with ID {} is not in this datapart; "
                                          "smallest ID in this datapart is {}",
                                          smallestEdgeToDelete, ogFstEdgeID));
    }
    if (largestEdgeToDelete > ogFstEdgeID + ogSize - 1) {
        throw TuringException(fmt::format("Edge with ID {} is not in this datapart; "
                                          "largest ID in this datapart is {}",
                                          largestEdgeToDelete, ogFstEdgeID + ogSize - 1));
    }

    // If entire EdgeContainer was deleted, exit early
    if (original.size() == toDelete.size()) {
        return 0;
    }

    uint64_t newFstEdgeID = UINT64_MAX;
    auto smallestDeleted = toDelete.cbegin();

    // Calculate the new first ID
    // The new first ID is the smallest ID in the original container which is not
    // deleted

    // If the smallest ID is not deleted, then the smallest ID remains the same
    if (*smallestDeleted != ogFstEdgeID) {
        newFstEdgeID = ogFstEdgeID;
    } else { // Otherwise, find the smallest ID which is not deleted
        // Never risk of @ref smallestDeleted > end() since in that case all nodes are
        // deleted, which is explicitly checked above
        while (*++smallestDeleted == ++newFstEdgeID)
            ;
    }

    auto ogOutRecords = original.getOuts();
    std::vector<EdgeRecord> newOutRecords;
    // Do not reserve here as no way to estimate how many deleted edges are out edges

    auto ogInRecords = original.getIns();
    std::vector<EdgeRecord> newInRecords;

    // Create a map so that we can quickly find the inEdgeRecord given an outEdgeRecord
    // TODO: Pass and use original datapart's EdgeIndexer instead
    std::unordered_map<EdgeID, EdgeRecord> idInRecordMap;
    for (const auto& r : ogInRecords) {
        idInRecordMap.emplace(r._edgeID, r);
    }

    // Do not reserve here as no way to estimate how many deleted edges are in edges

    // For each outEdge, check if the edge should be deleted
    // If it should not, append the record to the new outEdges, and find the equivalent
    // inEdge record, appending that to the new inEdges
    for (const auto& [edgeId, edgeRecord] : enumerate_from(ogFstEdgeID, ogOutRecords)) {
        if (toDelete.contains(edgeRecord._edgeID)) {
            numEdgesDeleted++;
            continue;
        } else {
            newOutRecords.emplace_back(edgeRecord);
            // TODO: Pass and use original datapart's EdgeIndexer instead
            EdgeID thisEdgeID = edgeRecord._edgeID;
            newInRecords.emplace_back(idInRecordMap.at(thisEdgeID));
        }
    }
    // Sort inEdges w.r.t target as required by DataPart.
    std::ranges::sort(newInRecords, [](const EdgeRecord& a, const EdgeRecord& b) {
        return a._otherID < b._otherID;
    });

    NodeID newFstNodeID = newOutRecords.at(0)._nodeID;

    newContainer._firstEdgeID = newFstEdgeID;
    newContainer._firstNodeID = newFstNodeID;
    newContainer._inEdges = newInRecords;
    newContainer._outEdges = newOutRecords;

    return numEdgesDeleted;
}

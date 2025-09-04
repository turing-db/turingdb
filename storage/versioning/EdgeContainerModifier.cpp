#include "EdgeContainerModifier.h"
#include "EdgeContainer.h"
#include "TuringException.h"
#include <cstdint>

using namespace db;

std::unique_ptr<EdgeContainer> EdgeContainerModifier::deleteEdges(const EdgeContainer& original,
                                                                  const std::set<EdgeID>& toDelete) {
    uint64_t ogFstID = original.getFirstEdgeID().getValue();
    size_t ogSize = original.size();

    EdgeID smallestEdgeToDelete = *toDelete.cbegin();
    EdgeID largestEdgeToDelete = *toDelete.crbegin();
    if (smallestEdgeToDelete < ogFstID) {
        throw TuringException(fmt::format("Edge with ID {} is not in this datapart; "
                                          "smallest ID in this datapart is {}",
                                          smallestEdgeToDelete, ogFstID));
    }
    if (largestEdgeToDelete > ogFstID + ogSize - 1) {
        throw TuringException(fmt::format("Edge with ID {} is not in this datapart; "
                                          "largest ID in this datapart is {}",
                                          largestEdgeToDelete, ogFstID + ogSize - 1));
    }

    return {};
}

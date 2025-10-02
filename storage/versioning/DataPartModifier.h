#pragma once

#include <span>

#include "ArcManager.h"
#include "BioAssert.h"
#include "ID.h"
#include "TuringException.h"
#include "writers/DataPartBuilder.h"

namespace db {

class DataPart;

class DataPartModifier {
public:
    DataPartModifier(const WeakArc<DataPart> oldDP, DataPartBuilder& builder,
                         std::span<NodeID> nodesToDelete, std::span<EdgeID> edgesToDelete)
        : _oldDP(oldDP),
        _builder(&builder),
        _nodesToDelete(nodesToDelete),
        _edgesToDelete(edgesToDelete)
    {
    }
    
    DataPartModifier(const DataPartModifier&) = delete;
    DataPartModifier& operator=(const DataPartModifier&) = delete;
    DataPartModifier(DataPartModifier&&) = delete;
    DataPartModifier& operator=(DataPartModifier&&) = delete;

    ~DataPartModifier() = default;

    DataPartBuilder& builder() { return *_builder; }

    void applyModifications();

    /**
     * @brief Given a span of IDs to be deleted, and the first ID of the datapart which
     * the deletions are to be applied to, returns the new first ID for the modified
     * datpart.
     * @warn Assumes @param toDelete is sorted.
     * @warn Assumes @param toDelete is non-empty.
     */
    template <TypedInternalID IDT>
    static IDT findNewFirstID(std::span<IDT> toDelete, IDT oldFirstID);

private:
    const WeakArc<DataPart> _oldDP;
    DataPartBuilder* _builder;

    std::span<NodeID> _nodesToDelete;
    std::span<EdgeID> _edgesToDelete;
};

template <TypedInternalID IDT>
IDT DataPartModifier::findNewFirstID(std::span<IDT> toDelete, IDT oldFirstID) {
    msgbioassert(toDelete.size() == 0, "Attempted to get next ID for empty delete record.");
    msgbioassert(std::ranges::is_sorted(toDelete),
                 "Attempted to get next ID for unsorted delete record.");

    // If we are not deleting from the start, the first ID is unchanged
    if (toDelete.front() != oldFirstID) {
        return oldFirstID;
    }

    // Find the first occurence of an ID which is not deleted
    auto firstNonContiguousIt = std::ranges::adjacent_find(
        toDelete, [](IDT a, IDT b) { return a != b + 1; });

    if (firstNonContiguousIt == toDelete.end()) {
        return toDelete.back() + 1;
    }
}
}

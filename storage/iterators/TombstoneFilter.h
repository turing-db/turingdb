#pragma once

#include <algorithm>
#include <type_traits>
#include <unordered_set>
#include <variant>

#include "columns/ColumnEdgeTypes.h"
#include "columns/ColumnIDs.h"
#include "versioning/Tombstones.h"
#include "columns/ColumnVector.h"
#include "ID.h"

// This concept is required for a template parameter pack which ensures all arguments are
// pointers to ColumnVectors
template <typename Column>
concept ColumnVectorPtr =
    // Ensure ColumnVector::ValueType is defined
    requires { typename std::remove_pointer_t<Column>::ValueType; }
    && std::is_pointer_v<Column> // Ensure the Column is a pointer
    // Ensure removing the pointer gives ColumnVec
    && std::same_as<std::remove_pointer_t<Column>,
                    db::ColumnVector<typename std::remove_pointer_t<Column>::ValueType>>;

namespace db {

class Tombstones;

template <typename T>
class ColumnVector;

/**
 * @brief Provides generic methods for applying filtering based on tombstones to the
 * ColumnVector outputs of a ChunkWriter
 */
class TombstoneFilter {
public:
    TombstoneFilter(const Tombstones& tombstones);
    ~TombstoneFilter() = default;

    TombstoneFilter(const TombstoneFilter&) = delete;
    TombstoneFilter(TombstoneFilter&&) = delete;
    TombstoneFilter& operator=(const TombstoneFilter&) = delete;
    TombstoneFilter& operator=(TombstoneFilter&&) = delete;

    using ColumnIndices = ColumnVector<size_t>;

    void filterGetOutEdges(ColumnEdgeIDs* edgeIDs, ColumnNodeIDs* tgtIDs,
                           ColumnEdgeTypes* edgeTypes, ColumnIndices* indices);

    void setBaseColumn(ColumnVector<NodeID>* nodeColumn) {
        bioassert(nodeColumn);
        _baseCol = nodeColumn;
    }

    void setBaseColumn(ColumnVector<EdgeID>* edgeColumn) {
        bioassert(edgeColumn);
        _baseCol = edgeColumn;
    }

    /**
     * @brief Generalised filter operation for multiple columns that need be filtered
     * consistently.
     * @detail Populates @ref _deletedIndices by querying tombstones for ID columns
     * provided in @param columns, then applies the deletions to all non-null columns.
     */
    template <ColumnVectorPtr... Cols>
        requires (sizeof...(Cols) > 1)
    void filter(Cols... columns);

    /**
    * @brief Specialised filter operation when only filtering a single ID column.
    */
    template <ColumnVectorPtr Col>
    void filter(Col column);

    bool empty() const { return _deletedIndices.empty(); }

private:
    using DeletedIndices = std::unordered_set<size_t>;
    using DeletedVec = std::vector<size_t>;
    using BaseColumn = std::variant<ColumnVector<NodeID>*, ColumnVector<EdgeID>*>;

    const Tombstones& _tombstones;
    DeletedIndices _deletedIndices;
    DeletedVec _delVec;
    BaseColumn _baseCol;

    /**
     * @brief Specialised filtering method, used when only a single column of the output
     * of a ChunkWriter needs to be filtered, for example: @ref ScanNodesChunkWriter
     * only produces a NodeID column
     */
    template <TypedInternalID IDT>
    void onePassApplyFilter(ColumnVector<IDT>& column);

    /**
     * @brief Step 1 of a 2 step filtering process for chunk writers who produced
     * multiple columns which need to be filtered.
     * @detail Adds indexes which contain a Node or Edge ID which has a tombstone in
     * @ref _tombstones to @ref _deletedIndices.
     */
    template <TypedInternalID IDT>
    void populateDeletedIndices(const ColumnVector<IDT>& column);

    /**
     * @brief Step 2 of a 2 step filtering process for chunk writers who produced
     * multiple columns which need to be filtered.
     * @detail Stably moves all elements at indices which are not in @ref
     * _deletedIndices to the front of the column, and resizes to only include those
     * values.
     */
    template <typename T>
    void applyDeletedIndices(ColumnVector<T>& column);

    template <typename T>
    void applyDeletedVec(ColumnVector<T>& column);
};

template <ColumnVectorPtr Col>
void TombstoneFilter::filter(Col column) {
    if (!column) {
        return;
    }
    this->onePassApplyFilter(*column);
}

template <ColumnVectorPtr... Cols>
    requires(sizeof...(Cols) > 1)
void TombstoneFilter::filter(Cols... columns) {
    bool hasNodes = _tombstones.hasNodes();
    bool hasEdges = _tombstones.hasEdges();

    // Helper to add indexes to be deleted if the column is an ID column
    [[maybe_unused]] const auto populateIfID = [this, hasNodes, hasEdges]<typename T>(ColumnVector<T>* col) {
        if (!col) {
            return;
        }
        if constexpr (std::is_same_v<T, NodeID>) {
            if (hasNodes) {
                this->populateDeletedIndices(*col);
            }
        }
        if constexpr (std::is_same_v<T, EdgeID>) {
            if (hasEdges) {
                this->populateDeletedIndices(*col);
            }
        }
    };

    // Helper to apply a filter to only non-nullptr columns
    const auto applyFilter = [this]<typename T>(ColumnVector<T>* col) {
        if (!col) {
            return;
        }
        // this->applyDeletedIndices(*col);
        this->applyDeletedVec(*col);
    };

    // (populateIfID(columns), ...); // Populate @ref _deletedIndices for ID columns
    std::visit([this](auto& base) { populateDeletedIndices(*base); }, _baseCol);
    if (_deletedIndices.empty()) { // Nothing to filter -> exit early
        return;
    }

    (applyFilter(columns), ...);  // Apply filter to all columns
}

template <typename T>
void TombstoneFilter::applyDeletedIndices(ColumnVector<T>& column) {
    size_t initialSize = column.size();
    std::vector<T>& raw = column.getRaw();

    // 2 pointer approach: traverse the vector with @ref readPointer and
    // @ref writePointer. Overwrite values at @ref writePointer with the value at
    // @ref readPointer if the index of @ref readPointer is not to be deleted.
    // Always increment read, only increment write if the value isn't deleted
    size_t writePointer = 0;
    for (size_t readPointer = 0; readPointer < initialSize; readPointer++) {
        if (!_deletedIndices.contains(readPointer)) {
            raw[writePointer] = raw[readPointer];
            writePointer++;
            continue;
        }
    }
    raw.resize(writePointer);
}

template <typename T>
void TombstoneFilter::applyDeletedVec(ColumnVector<T>& column) {
    bioassert(!_delVec.empty());

    if (column.empty()) {
        return;
    }

    // We should be able to omit these if we only generated filtered indices on a "base
    // column" (e.g. the edge column in GetOutEdges)
    std::ranges::sort(_delVec);
    auto [newEnd, oldEnd] = std::ranges::unique(_delVec);
    _delVec.erase(newEnd, oldEnd);

    // Unique and sorted
    bioassert(std::ranges::adjacent_find(_delVec) == _delVec.end());
    bioassert(std::ranges::is_sorted(_delVec));

    std::vector<T>& raw = column.getRaw();
    size_t initialSize = raw.size();

    // Track the index in the column we are reading from and writing to
    size_t readPtr {0};
    size_t writePtr {0};
    for (size_t i = 0; i < _delVec.size();) {
        // Compute the range [start, end] that is to be deleted
        size_t deletedRangeStart = _delVec[i];
        size_t deletedRangeEnd = deletedRangeStart;

        // Find a contiguous block of deleted indices
        while (i + 1 < _delVec.size() && _delVec[i + 1] == _delVec[i] + 1) {
            i++;
            deletedRangeEnd++;
        }

        // Calculate the size of the block to be deleted based on the indexes of the
        // column
        size_t deletedBlockSize = deletedRangeStart - readPtr;

        std::memmove(raw.data() + writePtr, // Write to the current point in the column
                     raw.data() + readPtr,  // From where we are reading the in the col
                     deletedBlockSize * sizeof(T));

        // Increment so the next place we write is after the block we just moved
        writePtr += deletedBlockSize;

        // Do not read the deleted block that we just skipped
        readPtr = deletedRangeEnd + 1;
        i++; // Look at the next index to be deleted
    }

    // There may be an interval [_delVec.back(), column.size()]. Move that down
    if (readPtr < initialSize) {
        size_t remainingSize = initialSize - readPtr;
        std::memmove(raw.data() + writePtr,
                     raw.data() + readPtr,
                     remainingSize * sizeof(T));
        writePtr += remainingSize;
    }

    // We potentially shifted elements down, so the end of the vector needs to be
    // truncated
    raw.resize(writePtr);
}

}

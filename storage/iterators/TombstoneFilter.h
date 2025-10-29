#pragma once

#include <type_traits>
#include <unordered_set>

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

    const Tombstones& _tombstones;
    DeletedIndices _deletedIndices;

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
    const auto populateIfID = [this, hasNodes, hasEdges]<typename T>(ColumnVector<T>* col) {
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
        this->applyDeletedIndices(*col);
    };

    (populateIfID(columns), ...); // Populate @ref _deletedIndices for ID columns
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
}

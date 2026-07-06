#pragma once

#include <cstddef>
#include <cstdint>
#include <span>
#include <vector>

#include <parquet/types.h>

#include "ParquetImportVisitor.h"

#include "ID.h"

namespace parquet {
class FileMetaData;
class RowGroupMetaData;
}

namespace db {

class DataPartBuilder;

/**
 * @brief Node parser of the @ref ParquetImporter
 *
 * @detail Schema: Required columns:
 *     - @ref _nodeFile:
 *       Required columns:
 *           - __id : INT64
 *           - __labels : BYTE_ARRAY [ BYTE_ARRAY] (max nesting: 1)
 *       Any other columns are interpreted as properties
 */
class ParquetNodeVisitor final : public ParquetImportVisitor {
public:
    using ParquetImportVisitor::ParquetImportVisitor;

    bool onFileStart(const parquet::FileMetaData& metadata) final;

    bool onRowGroupStart(size_t rowGroupIndex, const parquet::RowGroupMetaData& metadata) final;

    bool onLevels(size_t columnIndex,
                  std::span<const int16_t> repLevels,
                  std::span<const int16_t> defLevels) final;

    bool onInt64Values(size_t columnIndex, std::span<const int64_t> values) final;
    bool onByteArrayValues(size_t columnIndex, std::span<const parquet::ByteArray> values) final;

    bool onChunkEnd(size_t rowGroupIndex, size_t firstRowInRowGroup, size_t rows) final;

private:
    size_t _nodeColIdx {INVALID_COL_IDX};
    size_t _lblColIdx {INVALID_COL_IDX};

    // For row X in the labels column: deflevel(X) == maxdeflevel => row X is non-null.
    int16_t _lblMaxDefLevel {0};

    NodeIDs _chunkNodeIds;
    std::vector<std::vector<LabelID>> _chunkNodeLabels;

    // Representation/definition levels for label byte arrays
    std::span<const int16_t> _chunkLabelRepLevels;
    std::span<const int16_t> _chunkLabelDefLevels;

    void fillLabels(std::span<const parquet::ByteArray> labels);

    void createNodes(DataPartBuilder* builder);

    void applyNodeProperties(size_t numRows);

    void addNodeProperty(NodeID id,
                         const PropertyColumn& prop,
                         size_t columnIndex,
                         size_t valueIndex);

    void resetChunk();
};

}

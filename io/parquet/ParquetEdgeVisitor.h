#pragma once

#include <cstddef>
#include <cstdint>
#include <span>
#include <vector>

#include <parquet/types.h>

#include "ParquetImportVisitor.h"

#include "ID.h"
#include "datapart/EdgeRecord.h"

namespace parquet {
class FileMetaData;
class RowGroupMetaData;
}

namespace db {

class DataPartBuilder;

/**
 * @brief Edge parser of the @ref ParquetImporter
 *
 * @detail Schema: Required columns:
 *           - __source : INT64
 *           - __target : INT64
 *           - __type : BYTE_ARRAY (max nesting: 0)
 *       Any other columns are interpreted as properties
 */
class ParquetEdgeVisitor final : public ParquetImportVisitor {
public:
    using ParquetImportVisitor::ParquetImportVisitor;

    /**
    * @brief Parses column schema, extracts relevant columns
    * @throws If missing any required columns
    */
    bool onFileStart(const parquet::FileMetaData& metadata) final;

    bool onRowGroupStart(size_t rowGroupIndex, const parquet::RowGroupMetaData& metadata) final;

    bool onLevels(size_t columnIndex,
                  std::span<const int16_t> repLevels,
                  std::span<const int16_t> defLevels) final;

    bool onInt64Values(size_t columnIndex, std::span<const int64_t> values) final;

    bool onByteArrayValues(size_t columnIndex, std::span<const parquet::ByteArray> values) final;

    bool onChunkEnd(size_t rowGroupIndex, size_t firstRowInRowGroup, size_t rows) final;

private:
    void fillEdgeTypes(std::span<const parquet::ByteArray> types);

    void createEdges(DataPartBuilder* builder);

    void applyEdgeProperties();

    void addEdgeProperty(const EdgeRecord& edge,
                         const PropertyColumn& prop,
                         size_t columnIndex,
                         size_t valueIndex);
    void resetChunk();

    size_t _srcColIdx {INVALID_COL_IDX};
    size_t _tgtColIdx {INVALID_COL_IDX};
    size_t _edgetypeColIdx {INVALID_COL_IDX};

    // For row X in column Y : deflevel(X) == maxdeflevel(Y) => row X is non-null.
    int16_t _srcIdMaxDefLevel {0};

    NodeIDs _chunkSrcIds;
    NodeIDs _chunkTgtIds;
    std::vector<EdgeTypeID> _chunkEdgeTypes;
    std::vector<EdgeRecord> _chunkEdgeRecords;

    std::vector<int16_t> _chunkSrcIdDefLevels;
};

}

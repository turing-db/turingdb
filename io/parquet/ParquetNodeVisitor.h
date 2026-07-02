#pragma once

#include <cstddef>
#include <cstdint>
#include <span>
#include <vector>

#include <parquet/types.h>

#include "ParquetNeo4jVisitor.h"

#include "ID.h"

namespace parquet {
class FileMetaData;
class RowGroupMetaData;
}

namespace db {

class DataPartBuilder;

// Imports the node file of a Neo4j split export: reads `__id` and
// `__labels.list.element`, creates the nodes, and records the Neo4j-ID -> NodeID
// mapping that the edge visitor later consumes.
class ParquetNodeVisitor final : public ParquetNeo4jVisitor {
public:
    using ParquetNeo4jVisitor::ParquetNeo4jVisitor;

    bool onFileStart(const parquet::FileMetaData& metadata) final;

    bool onRowGroupStart(size_t rowGroupIndex, const parquet::RowGroupMetaData& metadata) final;

    bool onLevels(size_t columnIndex,
                  std::span<const int16_t> repLevels,
                  std::span<const int16_t> defLevels) final;

    bool onInt64Values(size_t columnIndex, std::span<const int64_t> values) final;
    bool onByteArrayValues(size_t columnIndex, std::span<const parquet::ByteArray> values) final;

    bool onChunkEnd(size_t rowGroupIndex, size_t firstRowInRowGroup, size_t rows) final;

private:
    void fillLabels(std::span<const parquet::ByteArray> labels);
    void createNodes(DataPartBuilder* builder);
    void applyNodeProperties();
    void addNodeProperty(NodeID id,
                         const PropertyColumn& prop,
                         size_t columnIndex,
                         size_t valueIndex);
    void resetChunk();

    size_t _nodeColIdx {INVALID_COL_IDX};
    size_t _lblColIdx {INVALID_COL_IDX};

    // For row X in column Y : deflevel(X) == maxdeflevel(Y) => row X is non-null.
    int16_t _nodeIdMaxDefLevel {0};
    int16_t _lblMaxDefLevel {0};

    NodeIDs _chunkNodeIds;
    std::vector<std::vector<LabelID>> _chunkNodeLabels;
    std::vector<int16_t> _chunkNodeIdDefLevels;

    // Representation/definition levels for label byte arrays
    std::span<const int16_t> _chunkLabelRepLevels;
    std::span<const int16_t> _chunkLabelDefLevels;
};

}

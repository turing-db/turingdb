#pragma once

#include <cstdint>
#include <limits>
#include <string_view>
#include <unordered_map>
#include <vector>

#include <parquet/types.h>

#include "ParquetReader.h"

#include "ID.h"

namespace parquet {
class FileMetaData;
}

namespace db {

class ChangeAccessor;
class CommitBuilder;

class ParquetNeo4jVisitor final : public ParquetSaxVisitor {
public:
    using NodeIDs = std::span<const int64_t>;
    using NodeLabels = std::vector<std::vector<LabelID>>;
    using EdgeTypes = std::vector<EdgeTypeID>;

    ParquetNeo4jVisitor(CommitBuilder* builder)
        : _builder(builder)
    {
    }

    bool onRowGroupStart(size_t rowGroupIndex, const parquet::RowGroupMetaData& metadata) final;

    /**
     * @brief Inspects the file schema, populating column index members of @ref _loader
     */
    bool onFileStart(const parquet::FileMetaData& metadata) final;

    bool onLevels(size_t columnIndex,
                  std::span<const int16_t> repLevels,
                  std::span<const int16_t> defLevels) final;

    bool onInt64Values(size_t columnIndex, std::span<const int64_t> values) final;

    bool onByteArrayValues(size_t columnIndex, std::span<const parquet::ByteArray> values) final;

    bool onChunkEnd(size_t rowGroupIndex, size_t firstRowInRowGroup, size_t rows) final;

    NodeIDs nodes() const { return _chunkNodeIds; }

private:
    friend class Neo4jParquetImporter;
    using IDMap = std::unordered_map<int64_t, NodeID>;

    static constexpr size_t INVALID_COL_IDX = std::numeric_limits<size_t>::max();

    CommitBuilder* _builder;

    size_t _nodeColIdx {INVALID_COL_IDX};
    size_t _lblColIdx {INVALID_COL_IDX};

    size_t _srcColIdx {INVALID_COL_IDX};
    size_t _tgtColIdx {INVALID_COL_IDX};
    size_t _edgetypeColIdx {INVALID_COL_IDX};

    std::vector<size_t> _propCols;

    // Mapping Neo4j IDs to TuringDB IDs as defined by the DataPartBuilder
    IDMap _nodeIDs;

    // Per chunk reference stores
    NodeIDs _chunkNodeIds;
    NodeLabels _chunkNodeLabels;

    NodeIDs _chunkSrcIds;
    NodeIDs _chunkTgtIds;
    EdgeTypes _chunkEdgeTypes;

    int16_t _ageMaxDefLevel {0};
    std::span<int64_t> _chunkAgeVals;
    std::span<const int16_t> _chunkAgeRepLevels;
    std::span<const int16_t> _chunkAgeDefLevels;

    size_t _ageColIdx {INVALID_COL_IDX};
    int16_t _lblMaxDefLevel {0};
    std::span<const int16_t> _chunkLabelRepLevels;
    std::span<const int16_t> _chunkLabelDefLevels;

    void fillLabels(std::span<const parquet::ByteArray> labels);
    void fillEdgeTypes(std::span<const parquet::ByteArray> types);

    static constexpr std::string_view NEO4J_NODE_COL_PATH = "__id";
    static constexpr std::string_view NEO4J_LBLS_COL_PATH = "__labels.list.element";
    static constexpr std::string_view NEO4J_ETYPE_COL_PATH = "__type";
    static constexpr std::string_view NEO4J_SRC_COL_PATH = "__source_id";
    static constexpr std::string_view NEO4J_TGT_COL_PATH = "__target_id";

    static constexpr parquet::Type::type NEO4J_NODE_COL_TYPE = parquet::Type::INT64;
    static constexpr parquet::Type::type NEO4J_LBLS_COL_TYPE = parquet::Type::BYTE_ARRAY;
    static constexpr parquet::Type::type NEO4J_ETYPE_COL_TYPE = parquet::Type::BYTE_ARRAY;
};

}

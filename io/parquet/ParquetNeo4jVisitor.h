#pragma once

#include <cstdint>
#include <limits>
#include <span>
#include <string>
#include <string_view>
#include <unordered_map>
#include <vector>

#include <parquet/types.h>

#include "ParquetReader.h"

#include "ID.h"
#include "datapart/EdgeRecord.h"
#include "metadata/PropertyType.h"
#include "writers/DataPartBuilder.h"

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
    bool onFileStart(const parquet::FileMetaData& metadata) final;

    bool onLevels(size_t columnIndex,
                  std::span<const int16_t> repLevels,
                  std::span<const int16_t> defLevels) final;

    bool onInt32Values(size_t columnIndex, std::span<const int32_t> values) final;
    bool onInt64Values(size_t columnIndex, std::span<const int64_t> values) final;
    bool onDoubleValues(size_t columnIndex, std::span<const double> values) final;
    bool onBoolValues(size_t columnIndex, std::span<const bool> values) final;
    bool onByteArrayValues(size_t columnIndex, std::span<const parquet::ByteArray> values) final;

    bool onChunkEnd(size_t rowGroupIndex, size_t firstRowInRowGroup, size_t rows) final;

    NodeIDs nodes() const { return _chunkNodeIds; }

private:
    friend class Neo4jParquetImporter;
    using IDMap = std::unordered_map<int64_t, NodeID>;

    struct PropertyColumn {
        std::string name;
        ValueType valueType;
        PropertyTypeID propertyTypeID;
        int16_t maxDefLevel {0};
    };

    static constexpr size_t INVALID_COL_IDX = std::numeric_limits<size_t>::max();

    CommitBuilder* _builder;

    size_t _nodeColIdx {INVALID_COL_IDX};
    size_t _lblColIdx {INVALID_COL_IDX};
    size_t _srcColIdx {INVALID_COL_IDX};
    size_t _tgtColIdx {INVALID_COL_IDX};
    size_t _edgetypeColIdx {INVALID_COL_IDX};

    int16_t _nodeIdMaxDefLevel {0};
    int16_t _srcIdMaxDefLevel {0};
    int16_t _lblMaxDefLevel {0};

    // Mapping Neo4j IDs to TuringDB IDs as defined by the DataPartBuilder
    IDMap _nodeIDs;

    // Per-chunk stores for nodes
    NodeIDs _chunkNodeIds;
    NodeLabels _chunkNodeLabels;

    // Per-chunk stores for edges
    NodeIDs _chunkSrcIds;
    NodeIDs _chunkTgtIds;
    EdgeTypes _chunkEdgeTypes;
    std::vector<EdgeRecord> _chunkEdgeRecords;

    // Def levels for row-type classification, copied from shared scratch each sub-batch
    std::vector<int16_t> _chunkNodeIdDefLevels;
    std::vector<int16_t> _chunkSrcIdDefLevels;

    // Representation/definition levels for label byte arrays
    std::span<const int16_t> _chunkLabelRepLevels;
    std::span<const int16_t> _chunkLabelDefLevels;

    std::unordered_map<size_t, PropertyColumn> _propertyColumns;

    // Maps a property column index => values of that column
    std::unordered_map<size_t, std::span<const int64_t>> _propInt64Vals;
    std::unordered_map<size_t, std::span<const double>> _propDoubleVals;
    std::unordered_map<size_t, std::span<const bool>> _propBoolVals;
    std::unordered_map<size_t, std::span<const parquet::ByteArray>> _propByteArrayVals;

    // Maps a property column index => definition levels of that column
    // Used to encode null/nans for simple types
    std::unordered_map<size_t, std::vector<int16_t>> _propDefLevels;

    void fillLabels(std::span<const parquet::ByteArray> labels);

    void fillEdgeTypes(std::span<const parquet::ByteArray> types);

    void createNodes(DataPartBuilder* builder);
    void createEdges(DataPartBuilder* builder);
    void applyProperties(DataPartBuilder* builder);

    void chunkReset();

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

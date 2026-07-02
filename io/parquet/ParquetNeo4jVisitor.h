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
#include "metadata/PropertyType.h"

namespace parquet {
class FileMetaData;
}

namespace db {

class CommitBuilder;

// Base for the Neo4j-format parquet visitors. Owns the machinery shared between
// node and edge parsing: property-column discovery, per-chunk value capture, and
// the mapping from Neo4j IDs to the TuringDB NodeIDs assigned by the builder.
// The concrete @ref ParquetNodeVisitor and @ref ParquetEdgeVisitor handle the
// entity-specific columns (`__id`/`__labels` and `__source_id`/`__target_id`/
// `__type` respectively).
class ParquetNeo4jVisitor : public ParquetSaxVisitor {
public:
    using NodeIDs = std::span<const int64_t>;
    using IDMap = std::unordered_map<int64_t, NodeID>;

    ParquetNeo4jVisitor(CommitBuilder* builder, IDMap& nodeIDs)
        : _builder(builder),
        _nodeIDs(nodeIDs)
    {
    }

    // Property columns produce doubles/bools/int32s that no entity column claims,
    // so the base captures them directly. The derived visitors handle int64 and
    // byte-array columns because those also carry the entity-specific columns.
    bool onInt32Values(size_t columnIndex, std::span<const int32_t> values) override;
    bool onDoubleValues(size_t columnIndex, std::span<const double> values) override;
    bool onBoolValues(size_t columnIndex, std::span<const bool> values) override;

protected:
    struct PropertyColumn {
        std::string name;
        ValueType valueType;
        PropertyTypeID propertyTypeID;
        int16_t maxDefLevel {0};
    };

    static constexpr size_t INVALID_COL_IDX = std::numeric_limits<size_t>::max();

    static constexpr std::string_view NEO4J_NODE_COL_PATH = "__id";
    static constexpr std::string_view NEO4J_LBLS_COL_PATH = "__labels.list.element";
    static constexpr std::string_view NEO4J_ETYPE_COL_PATH = "__type";
    static constexpr std::string_view NEO4J_SRC_COL_PATH = "__source";
    static constexpr std::string_view NEO4J_TGT_COL_PATH = "__target";

    static constexpr parquet::Type::type NEO4J_NODE_COL_TYPE = parquet::Type::INT64;
    static constexpr parquet::Type::type NEO4J_LBLS_COL_TYPE = parquet::Type::BYTE_ARRAY;
    static constexpr parquet::Type::type NEO4J_ETYPE_COL_TYPE = parquet::Type::BYTE_ARRAY;

    // A column that is not an id/label/type column is treated as a property.
    // Infers its value type and registers the property type with the builder.
    void discoverPropertyColumn(size_t columnIndex,
                                const std::string& path,
                                parquet::Type::type physicalType,
                                int16_t maxDefLevel);

    // Capture helpers for property columns, called by the derived value callbacks
    // once they have handled their entity-specific columns.
    void capturePropertyLevels(size_t columnIndex, std::span<const int16_t> defLevels);
    void capturePropertyInt64(size_t columnIndex, std::span<const int64_t> values);
    void capturePropertyByteArray(size_t columnIndex, std::span<const parquet::ByteArray> values);

    void resetPropertyChunk();

    CommitBuilder* _builder {nullptr};

    // Mapping Neo4j IDs to TuringDB IDs as defined by the DataPartBuilder. Owned by
    // the importer: filled by the node visitor, read by the edge visitor.
    IDMap& _nodeIDs;

    std::unordered_map<size_t, PropertyColumn> _propertyColumns;

    // Maps a property column index => values of that column
    std::unordered_map<size_t, std::span<const int64_t>> _propInt64Vals;
    std::unordered_map<size_t, std::span<const double>> _propDoubleVals;
    std::unordered_map<size_t, std::span<const bool>> _propBoolVals;
    std::unordered_map<size_t, std::span<const parquet::ByteArray>> _propByteArrayVals;

    // Maps a property column index => definition levels of that column.
    // Used to encode null/nans for simple types.
    std::unordered_map<size_t, std::vector<int16_t>> _propDefLevels;
};

}

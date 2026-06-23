#pragma once

#include <limits>
#include <string_view>
#include <vector>

#include <parquet/types.h>

#include "ParquetReader.h"

namespace parquet {
class FileMetaData;
}

namespace db {

class ChangeAccessor;
class CommitBuilder;

class ParquetNeo4jVisitor final : public ParquetSaxVisitor {
public:

    ParquetNeo4jVisitor(CommitBuilder* builder)
        : _builder(builder)
    {
    }

    /**
     * @brief Inspects the file schema, populating column index members of @ref _loader
     */
    bool onFileStart(const parquet::FileMetaData& metadata) final;

    bool onInt64Values(size_t columnIndex, std::span<const int64_t> values) final;

private:
    friend class Neo4jParquetImporter;

    static constexpr size_t INVALID_COL_IDX = std::numeric_limits<size_t>::max();

    CommitBuilder* _builder;

    size_t _nodeColIdx {INVALID_COL_IDX};
    size_t _lblColIdx {INVALID_COL_IDX};

    size_t _srcColIdx {INVALID_COL_IDX};
    size_t _tgtColIdx {INVALID_COL_IDX};
    size_t _edgetypeColIdx {INVALID_COL_IDX};

    std::vector<size_t> _propCols;

    static constexpr std::string_view NEO4J_NODE_COL_NAME = "__id";
    static constexpr std::string_view NEO4J_LBLS_COL_NAME = "__labels";
    static constexpr std::string_view NEO4J_ETYPE_COL_NAME = "__type";
    static constexpr std::string_view NEO4J_SRC_COL_NAME = "__source_id";
    static constexpr std::string_view NEO4J_TGT_COL_NAME = "__target_id";

    static constexpr parquet::Type::type NEO4J_NODE_COL_TYPE = parquet::Type::INT64;
    static constexpr parquet::Type::type NEO4J_LBLS_COL_TYPE = parquet::Type::BYTE_ARRAY;
    static constexpr parquet::Type::type NEO4J_ETYPE_COL_TYPE = parquet::Type::BYTE_ARRAY;
};

}

#pragma once

#include <parquet/types.h>
#include <string_view>

#include "ParquetNeo4jDumpLoader.h"

#include "ParquetReader.h"

namespace parquet {
class FileMetaData;
}

namespace db {

class ChangeAccessor;

class ParquetNeo4jVisitor final : public ParquetSaxVisitor {
public:
    ParquetNeo4jVisitor(ParquetNeo4jDumpLoader* loader)
    : _loader(loader)
    {
    }

    /**
     * @brief Inspects the file schema, populating column index members of @ref _loader
     */
    bool onFileStart(const parquet::FileMetaData& metadata) final;

private:
    ParquetNeo4jDumpLoader* _loader {nullptr};
    ChangeAccessor* _change {nullptr};

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

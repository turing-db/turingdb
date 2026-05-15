#pragma once

#include "ParquetReader.h"

namespace parquet {
namespace schema {
class Node;
}
}

namespace db {

class ParquetSchema;
class ParquetSchemaField;

// SAX visitor that populates a ParquetSchema from a Parquet file's metadata.
// onFileStart fills the schema and returns false to stop the read after the
// metadata callback — no row groups are decoded.
class ParquetSchemaExtractor : public ParquetSaxVisitor {
public:
    explicit ParquetSchemaExtractor(ParquetSchema& schema);
    ~ParquetSchemaExtractor() override;

    ParquetSchemaExtractor(const ParquetSchemaExtractor&) = delete;
    ParquetSchemaExtractor(ParquetSchemaExtractor&&) = delete;
    ParquetSchemaExtractor& operator=(const ParquetSchemaExtractor&) = delete;
    ParquetSchemaExtractor& operator=(ParquetSchemaExtractor&&) = delete;

    bool onFileStart(const parquet::FileMetaData& metadata) override;

private:
    ParquetSchema& _schema;

    static void extractField(const parquet::schema::Node& node, ParquetSchemaField& out);
};

}

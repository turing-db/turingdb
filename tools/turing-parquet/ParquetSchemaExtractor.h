#pragma once

#include "ParquetReader.h"

#include "ParquetSchema.h"

namespace parquet {
namespace schema {
class Node;
}
}

namespace db {

// SAX visitor that builds a ParquetSchema from a Parquet file's metadata.
// onFileStart populates the schema and returns false to stop the read after
// the metadata callback — no row groups are decoded.
class ParquetSchemaExtractor : public ParquetSaxVisitor {
public:
    ParquetSchemaExtractor();
    ~ParquetSchemaExtractor() override;

    ParquetSchemaExtractor(const ParquetSchemaExtractor&) = delete;
    ParquetSchemaExtractor(ParquetSchemaExtractor&&) = delete;
    ParquetSchemaExtractor& operator=(const ParquetSchemaExtractor&) = delete;
    ParquetSchemaExtractor& operator=(ParquetSchemaExtractor&&) = delete;

    bool onFileStart(const parquet::FileMetaData& metadata) override;

    const ParquetSchema& getSchema() const { return _schema; }

private:
    ParquetSchema _schema;

    static void extractField(const parquet::schema::Node& node, ParquetSchemaField& out);
};

}

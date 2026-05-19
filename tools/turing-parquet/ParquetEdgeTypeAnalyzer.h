#pragma once

#include <stddef.h>

#include <string>

#include "ParquetReader.h"

namespace db {

class ParquetSchema;
class ParquetEdgeTypeAnalysis;

// SAX visitor that records every value of a single BYTE_ARRAY column as an
// edge-type observation into the supplied ParquetEdgeTypeAnalysis. The
// constructor checks that the named column exists in the schema, is a
// BYTE_ARRAY primitive, and is at the top level; on failure it throws a
// TuringException. Multiple files can accumulate into the same analysis by
// constructing a fresh analyzer per file with the same column name and
// analysis reference.
class ParquetEdgeTypeAnalyzer : public ParquetSaxVisitor {
public:
    ParquetEdgeTypeAnalyzer(const ParquetSchema& schema,
                            const std::string& columnName,
                            ParquetEdgeTypeAnalysis& analysis);
    ~ParquetEdgeTypeAnalyzer() override;

    ParquetEdgeTypeAnalyzer(const ParquetEdgeTypeAnalyzer&) = delete;
    ParquetEdgeTypeAnalyzer(ParquetEdgeTypeAnalyzer&&) = delete;
    ParquetEdgeTypeAnalyzer& operator=(const ParquetEdgeTypeAnalyzer&) = delete;
    ParquetEdgeTypeAnalyzer& operator=(ParquetEdgeTypeAnalyzer&&) = delete;

    bool onByteArrayValues(size_t columnIndex,
                           std::span<const parquet::ByteArray> values) override;

private:
    const std::string _columnName;
    ParquetEdgeTypeAnalysis& _analysis;
    size_t _targetColumnIndex {0};
};

}

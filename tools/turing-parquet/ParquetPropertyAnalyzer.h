#pragma once

#include <stddef.h>

#include <string>

#include "ParquetReader.h"

namespace db {

class ParquetSchema;
class ParquetPropertyAnalysis;

// SAX visitor that analyzes a single key-value JSON BYTE_ARRAY column,
// recording every JSON value's type into the supplied ParquetPropertyAnalysis
// and retaining a small set of previews for array and object values.
//
// The constructor checks that the named column exists in the schema, is a
// BYTE_ARRAY primitive, and was classified as KEY_VALUE JSON by an earlier
// content-sampling pass (see ParquetJsonDetector). If any check fails it
// throws a TuringException. Top-level columns only.
class ParquetPropertyAnalyzer : public ParquetSaxVisitor {
public:
    ParquetPropertyAnalyzer(const ParquetSchema& schema,
                            const std::string& columnName,
                            ParquetPropertyAnalysis& analysis);
    ~ParquetPropertyAnalyzer() override;

    ParquetPropertyAnalyzer(const ParquetPropertyAnalyzer&) = delete;
    ParquetPropertyAnalyzer(ParquetPropertyAnalyzer&&) = delete;
    ParquetPropertyAnalyzer& operator=(const ParquetPropertyAnalyzer&) = delete;
    ParquetPropertyAnalyzer& operator=(ParquetPropertyAnalyzer&&) = delete;

    bool onByteArrayValues(size_t columnIndex,
                           std::span<const parquet::ByteArray> values) override;

private:
    const std::string _columnName;
    ParquetPropertyAnalysis& _analysis;
    size_t _targetColumnIndex {0};
};

}

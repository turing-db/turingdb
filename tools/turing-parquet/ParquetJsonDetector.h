#pragma once

#include <stddef.h>

#include <unordered_map>

#include "ParquetReader.h"

namespace db {

class ParquetSchema;
class ParquetSchemaField;

// SAX visitor that samples values from BYTE_ARRAY columns and marks the
// corresponding fields in the supplied ParquetSchema as likely-JSON when all
// sampled values parse as JSON. Reading stops as soon as every BYTE_ARRAY
// column has hit the sample target.
//
// The schema must already be populated (e.g., by ParquetSchemaExtractor) when
// onFileStart fires; the visitor relies on its leaf order matching the
// Parquet file's column index order.
class ParquetJsonDetector : public ParquetSaxVisitor {
public:
    static constexpr size_t DEFAULT_SAMPLE_TARGET = 16;

    explicit ParquetJsonDetector(ParquetSchema& schema,
                                 size_t sampleTarget = DEFAULT_SAMPLE_TARGET);
    ~ParquetJsonDetector() override;

    ParquetJsonDetector(const ParquetJsonDetector&) = delete;
    ParquetJsonDetector(ParquetJsonDetector&&) = delete;
    ParquetJsonDetector& operator=(const ParquetJsonDetector&) = delete;
    ParquetJsonDetector& operator=(ParquetJsonDetector&&) = delete;

    bool onFileStart(const parquet::FileMetaData& metadata) override;
    bool onByteArrayValues(size_t columnIndex,
                           std::span<const parquet::ByteArray> values) override;
    bool onChunkEnd(size_t rowGroupIndex,
                    size_t firstRowInRowGroup,
                    size_t rows) override;
    bool onFileEnd() override;

private:
    struct ColumnState {
        ParquetSchemaField* field {nullptr};
        size_t samplesSeen {0};
        size_t jsonLooking {0};
    };

    ParquetSchema& _schema;
    size_t _sampleTarget;
    std::unordered_map<size_t, ColumnState> _byteArrayColumns;

    bool allColumnsSatisfied() const;
    void markJsonFields();
};

}

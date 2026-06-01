#pragma once

#include <stddef.h>
#include <stdint.h>

#include <memory>
#include <span>
#include <string>
#include <string_view>
#include <vector>

#include "Path.h"

namespace arrow {
namespace io {
class FileOutputStream;
}
}

namespace parquet {
class ParquetFileWriter;
class RowGroupWriter;
}

namespace db {

class ParquetWriteSchema;

// Push-style Parquet writer: the inverse of ParquetReader. Owns the Arrow
// output stream and parquet file writer; only primitive spans cross the public
// boundary (no Arrow/Parquet types leak out). Within a row group, columns must
// be written exactly once, in schema order, with the row group's row count of
// values each.
//
// The referenced schema must outlive the writer. Throws TuringException on any
// Parquet/Arrow failure, matching ParquetReader.
class ParquetWriter {
public:
    ParquetWriter(const fs::Path& path, const ParquetWriteSchema& schema);
    ~ParquetWriter();

    ParquetWriter(const ParquetWriter&) = delete;
    ParquetWriter(ParquetWriter&&) = delete;
    ParquetWriter& operator=(const ParquetWriter&) = delete;
    ParquetWriter& operator=(ParquetWriter&&) = delete;

    // Open a row group of rowCount rows. Every schema column must then be
    // written exactly once, in order, with rowCount values, before the next
    // beginRowGroup or finish.
    void beginRowGroup(size_t rowCount);

    // Accepts Int64 and UInt64 columns; for UInt64 the caller reinterprets the
    // unsigned value as int64 (two's-complement, lossless over the full range).
    void writeInt64Column(size_t columnIndex, std::span<const int64_t> values);
    void writeDoubleColumn(size_t columnIndex, std::span<const double> values);
    void writeBoolColumn(size_t columnIndex, std::span<const bool> values);
    void writeStringColumn(size_t columnIndex, std::span<const std::string_view> values);

    // flat holds rowCount * (byteWidth / sizeof(float)) contiguous floats; each
    // consecutive byteWidth bytes form one FIXED_LEN_BYTE_ARRAY value.
    void writeFixedLenColumn(size_t columnIndex,
                             std::span<const float> flat,
                             size_t byteWidth);

    // Attach a file-level key/value metadata entry, written when finish() runs.
    void setMetadata(std::string_view key, std::string_view value);

    // Finalize and flush the file. Must be called exactly once for a complete
    // file; failures surface here as TuringException.
    void finish();

private:
    const ParquetWriteSchema& _schema;
    std::shared_ptr<arrow::io::FileOutputStream> _outputStream;
    std::unique_ptr<parquet::ParquetFileWriter> _fileWriter;
    parquet::RowGroupWriter* _rowGroupWriter {nullptr};
    size_t _currentRowCount {0};
    size_t _nextColumnIndex {0};
    bool _finished {false};
    std::vector<std::string> _metadataKeys;
    std::vector<std::string> _metadataValues;

    void beginColumn(size_t columnIndex, size_t valueCount) const;
};

}

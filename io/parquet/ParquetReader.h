#pragma once

#include <stddef.h>
#include <stdint.h>

#include <memory>
#include <span>
#include <vector>

#include "Path.h"

namespace parquet {
class ByteArray;
class ColumnDescriptor;
class ColumnReader;
class FileMetaData;
class ParquetFileReader;
class RowGroupMetaData;
}

namespace db {

// SAX-style visitor invoked by ParquetReader.  
// Events fire at Parquet's natural chunk granularities: file, 
// row group, column chunk, and one batch of typed values at a time within a column chunk.  
// Return false to stop the execution.
class ParquetSaxVisitor {
public:
    virtual ~ParquetSaxVisitor();

    virtual bool onFileStart(const parquet::FileMetaData& metadata) {
        return true;
    }

    virtual bool onRowGroupStart(size_t rowGroupIndex,
                                 const parquet::RowGroupMetaData& metadata) {
        return true;
    }

    virtual bool onColumnStart(size_t rowGroupIndex,
                               size_t columnIndex,
                               const parquet::ColumnDescriptor& descriptor) {
        return true;
    }

    virtual bool onInt32Values(size_t rowGroupIndex,
                               size_t columnIndex,
                               std::span<const int32_t> values) {
        return true;
    }

    virtual bool onInt64Values(size_t rowGroupIndex,
                               size_t columnIndex,
                               std::span<const int64_t> values) {
        return true;
    }

    virtual bool onFloatValues(size_t rowGroupIndex,
                               size_t columnIndex,
                               std::span<const float> values) {
        return true;
    }

    virtual bool onDoubleValues(size_t rowGroupIndex,
                                size_t columnIndex,
                                std::span<const double> values) {
        return true;
    }

    virtual bool onBoolValues(size_t rowGroupIndex,
                              size_t columnIndex,
                              std::span<const bool> values) {
        return true;
    }

    virtual bool onByteArrayValues(size_t rowGroupIndex,
                                   size_t columnIndex,
                                   std::span<const parquet::ByteArray> values) {
        return true;
    }

    virtual bool onColumnEnd(size_t rowGroupIndex, size_t columnIndex) {
        return true;
    }

    virtual bool onRowGroupEnd(size_t rowGroupIndex) {
        return true;
    }

    virtual bool onFileEnd() { return true; }
};

class ParquetReader {
public:
    static constexpr size_t DEFAULT_BATCH_SIZE = 1024;

    ParquetReader(const fs::Path& path, ParquetSaxVisitor& visitor);
    ~ParquetReader();

    ParquetReader(const ParquetReader&) = delete;
    ParquetReader(ParquetReader&&) = delete;
    ParquetReader& operator=(const ParquetReader&) = delete;
    ParquetReader& operator=(ParquetReader&&) = delete;

    // Restrict decoding to a subset of columns by their index in the
    // file schema. An empty projection (default) walks every column.
    void setColumnProjection(const std::vector<size_t>& columnIndices) {
        _projection = columnIndices;
    }

    // Walks the file and fires SAX callbacks.
    // Throws TuringException on I/O or decode failure.
    void read();

private:
    fs::Path _path;
    ParquetSaxVisitor& _visitor;
    std::vector<size_t> _projection;
    // Scratch buffer reused across all column chunks; sized once to hold
    // DEFAULT_BATCH_SIZE of the widest typed value (parquet::ByteArray).
    std::vector<uint8_t> _scratch;

    template <typename ReaderT, typename ValueT, typename Callback>
    bool decodeAndFire(size_t rowGroupIndex,
                       size_t columnIndex,
                       parquet::ColumnReader& reader,
                       Callback callback);
};

}

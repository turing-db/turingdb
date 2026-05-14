#pragma once

#include <stddef.h>
#include <stdint.h>

#include <memory>
#include <vector>

#include "Path.h"

namespace parquet {
class ColumnDescriptor;
class ColumnReader;
class FileMetaData;
class ParquetFileReader;
class RowGroupMetaData;
}

namespace db {

// SAX-style visitor invoked by ParquetReader as it walks a Parquet file.
// Events are fired at Parquet's natural chunk granularities: per file,
// per row group, per column chunk inside a row group.  Returning false
// from any callback aborts the read cleanly.
//
// Only onColumnChunk is required.  The handler is expected to downcast
// `reader` to the appropriate parquet::TypedColumnReader<...> based on
// `descriptor.physical_type()` and call ReadBatch in its own loop.
class ParquetSaxVisitor {
public:
    virtual ~ParquetSaxVisitor();

    virtual bool onFileStart(const parquet::FileMetaData& metadata) {
        (void)metadata;
        return true;
    }

    virtual bool onRowGroupStart(int rowGroupIndex,
                                 const parquet::RowGroupMetaData& metadata) {
        (void)rowGroupIndex;
        (void)metadata;
        return true;
    }

    virtual bool onColumnChunk(int rowGroupIndex,
                               int columnIndex,
                               const parquet::ColumnDescriptor& descriptor,
                               parquet::ColumnReader& reader) = 0;

    virtual bool onRowGroupEnd(int rowGroupIndex) {
        (void)rowGroupIndex;
        return true;
    }

    virtual bool onFileEnd() { return true; }
};

class ParquetReader {
public:
    ParquetReader(const fs::Path& path, ParquetSaxVisitor& visitor);
    ~ParquetReader();

    ParquetReader(const ParquetReader&) = delete;
    ParquetReader(ParquetReader&&) = delete;
    ParquetReader& operator=(const ParquetReader&) = delete;
    ParquetReader& operator=(ParquetReader&&) = delete;

    // Restrict decoding to a subset of columns by their index in the
    // file schema.  An empty projection (default) walks every column.
    void setColumnProjection(const std::vector<int>& columnIndices) {
        _projection = columnIndices;
    }

    // Walks the file and fires SAX callbacks.  Throws TuringException
    // on I/O or decode failure.
    void read();

private:
    fs::Path _path;
    ParquetSaxVisitor& _visitor;
    std::vector<int> _projection;
};

}

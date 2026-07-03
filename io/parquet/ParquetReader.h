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
class FixedLenByteArray;
class Int96;
class ParquetFileReader;
class RowGroupMetaData;
class RowGroupReader;
}

namespace db {

// Streaming SAX-style visitor driven by ParquetReader::nextChunk.
//
// Lifecycle: onFileStart fires once on the first nextChunk call.
// For each row group, onRowGroupStart fires once, followed by one or more chunks.
// A chunk fires onColumnStart (first time only, per column),
// one callback per projected column in the chunk, then onChunkEnd. 
// When a row group is exhausted, onColumnEnd fires for each projected column 
// followed by onRowGroupEnd.
// onFileEnd fires when nextChunk returns false.
//
// Returning false from any callback aborts further reading.
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

    virtual bool onInt32Values(size_t columnIndex, std::span<const int32_t> values) {
        return true;
    }

    virtual bool onInt64Values(size_t columnIndex, std::span<const int64_t> values) {
        return true;
    }

    virtual bool onFloatValues(size_t columnIndex, std::span<const float> values) {
        return true;
    }

    virtual bool onDoubleValues(size_t columnIndex, std::span<const double> values) {
        return true;
    }

    virtual bool onBoolValues(size_t columnIndex, std::span<const bool> values) {
        return true;
    }

    // max rep levels > 0 => repeated column
    //                    => repLevels[i] == 0 => start of next element
    //
    // max rep level = 0 => non-repeated
    //                      max def level > 0 => optional column
    //                                        => defLevels[i] = max def level => nonnull
    //                                        => defLevels[i] < max def level => null
    virtual bool onLevels(size_t columnIndex,
                          std::span<const int16_t> repLevels,
                          std::span<const int16_t> defLevels) {
        return true;
    }

    virtual bool onByteArrayValues(size_t columnIndex,
                                   std::span<const parquet::ByteArray> values) {
        return true;
    }

    virtual bool onInt96Values(size_t columnIndex,
                               std::span<const parquet::Int96> values) {
        return true;
    }

    // byteWidth is the fixed length of each value, from the column descriptor.
    // Each FixedLenByteArray entry points to byteWidth bytes owned by the reader.
    virtual bool onFixedLenByteArrayValues(size_t columnIndex,
                                           std::span<const parquet::FixedLenByteArray> values,
                                           size_t byteWidth) {
        return true;
    }

    // All projected columns for this chunk have fired their callback.
    // firstRowInRowGroup is the row index within the current row group at
    // which this chunk starts, rows is the chunk's row count.
    virtual bool onChunkEnd(size_t rowGroupIndex,
                            size_t firstRowInRowGroup,
                            size_t rows) {
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
    static constexpr size_t DEFAULT_CHUNK_SIZE = 65536;

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

    // Produce one chunk and fire callbacks.
    // A chunk never crosses a row-group boundary. For files that contain a repeated
    // (list) column, a chunk always spans a whole row group and maxRows is not
    // subdivided, because repeated columns are decoded a full row group at a time
    // and the flat columns must stay aligned with them; for other files the chunk
    // is capped at maxRows.
    // Returns false once the file is exhausted or a callback returned false.
    // Throws TuringException on I/O or decode failure.
    bool nextChunk(size_t maxRows = DEFAULT_CHUNK_SIZE);

    bool ensureFileOpen();

private:
    fs::Path _path;
    ParquetSaxVisitor& _visitor;
    std::vector<size_t> _projection;

    // General ParquetFileReader state
    std::unique_ptr<parquet::ParquetFileReader> _fileReader;
    const parquet::FileMetaData* _fileMetadata {nullptr};
    std::vector<size_t> _columns;
    size_t _numRowGroups {0};
    size_t _currentRowGroup {0};
    bool _fileStarted {false};
    bool _fileEnded {false};
    bool _aborted {false};

    // row-group state
    bool _rowGroupOpen {false};
    std::shared_ptr<parquet::RowGroupReader> _rowGroupReader;
    std::vector<std::shared_ptr<parquet::ColumnReader>> _columnReaders;
    size_t _rowsInRowGroup {0};
    size_t _rowsConsumedInRowGroup {0};

    // One scratch buffer per projected column, sized to hold
    // DEFAULT_CHUNK_SIZE values of the widest type (parquet::ByteArray).
    std::vector<std::vector<uint8_t>> _scratch;

    std::vector<int16_t> _repLevelScratch;
    std::vector<int16_t> _defLevelScratch;

    // True when any projected column is repeated (a list). Repeated columns are
    // decoded a whole row group at a time, so when one is present every chunk spans
    // a full row group to keep flat and repeated columns aligned.
    bool _hasRepeatedColumn {false};

    bool openRowGroup();
    void closeRowGroup();
    bool readColumnSlice(size_t projectionIndex, size_t columnIndex, size_t batchRows);

    // Dispatch a values span to the matching visitor callback for DType.
    template <typename DType>
    bool emitValues(size_t columnIndex,
                    std::span<const typename DType::c_type> values,
                    size_t byteWidth);

    // Read a chunk for one column and dispatch to the matching visitor callback.
    // DType is one of parquet::{Int32,Int64,Float,Double,Boolean,ByteArray,Int96,FLBA}Type.
    // byteWidth is only consulted for FLBAType; it carries the fixed length of each value.
    template <typename DType>
    bool readSlice(parquet::ColumnReader* columnReader,
                   std::vector<uint8_t>& scratch,
                   size_t columnIndex,
                   size_t batchRows,
                   size_t byteWidth = 0);
};

}

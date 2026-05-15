#include "ParquetReader.h"

#include <algorithm>

#include <parquet/column_reader.h>
#include <parquet/exception.h>
#include <parquet/file_reader.h>
#include <parquet/metadata.h>
#include <parquet/schema.h>
#include <parquet/types.h>

#include <spdlog/fmt/fmt.h>

#include "TuringException.h"

using namespace db;

ParquetSaxVisitor::~ParquetSaxVisitor() {
}

ParquetReader::ParquetReader(const fs::Path& path, ParquetSaxVisitor& visitor)
    : _path(path),
    _visitor(visitor)
{
}

ParquetReader::~ParquetReader() {
}

bool ParquetReader::ensureFileOpen() {
    if (_fileStarted) {
        return true;
    }

    try {
        _fileReader = parquet::ParquetFileReader::OpenFile(_path.get());
    } catch (const parquet::ParquetException& e) {
        throw TuringException(fmt::format("Parquet: opening {}: {}", _path.get(), e.what()));
    }

    const auto fileMetadataSharedPtr = _fileReader->metadata();
    _fileMetadata = fileMetadataSharedPtr.get();

    _numRowGroups = static_cast<size_t>(_fileMetadata->num_row_groups());
    const size_t numColumns = static_cast<size_t>(_fileMetadata->num_columns());

    if (_projection.empty()) {
        _columns.reserve(numColumns);
        for (size_t column = 0; column < numColumns; ++column) {
            _columns.push_back(column);
        }
    } else {
        for (const size_t columnIndex : _projection) {
            if (columnIndex >= numColumns) {
                throw TuringException(fmt::format(
                    "Parquet: projection column {} out of range (file has {} columns)",
                    columnIndex, numColumns));
            }
        }
        _columns = _projection;
    }

    _scratch.assign(_columns.size(), {});

    _fileStarted = true;

    if (!_visitor.onFileStart(*_fileMetadata)) {
        _aborted = true;
        return false;
    }

    return true;
}

bool ParquetReader::openRowGroup() {
    _rowGroupReader = _fileReader->RowGroup(static_cast<int>(_currentRowGroup));
    const auto rowGroupMetadata = _fileMetadata->RowGroup(static_cast<int>(_currentRowGroup));

    _rowsInRowGroup = static_cast<size_t>(rowGroupMetadata->num_rows());
    _rowsConsumedInRowGroup = 0;

    if (!_visitor.onRowGroupStart(_currentRowGroup, *rowGroupMetadata)) {
        _aborted = true;
        return false;
    }

    const parquet::SchemaDescriptor* schema = _fileMetadata->schema();
    _columnReaders.clear();
    _columnReaders.reserve(_columns.size());

    for (size_t projectionIndex = 0; projectionIndex < _columns.size(); ++projectionIndex) {
        const size_t columnIndex = _columns[projectionIndex];
        const parquet::ColumnDescriptor* descriptor = schema->Column(static_cast<int>(columnIndex));
        _columnReaders.push_back(_rowGroupReader->Column(static_cast<int>(columnIndex)));

        if (!_visitor.onColumnStart(_currentRowGroup, columnIndex, *descriptor)) {
            _aborted = true;
            return false;
        }
    }

    _rowGroupOpen = true;
    return true;
}

void ParquetReader::closeRowGroup() {
    for (const size_t columnIndex : _columns) {
        if (!_visitor.onColumnEnd(_currentRowGroup, columnIndex)) {
            _aborted = true;
            // Continue to release readers; the abort takes effect on the next
            // nextChunk call which short-circuits on _aborted.
        }
    }

    if (!_aborted) {
        if (!_visitor.onRowGroupEnd(_currentRowGroup)) {
            _aborted = true;
        }
    }

    _columnReaders.clear();
    _rowGroupReader.reset();
    _rowGroupOpen = false;
    ++_currentRowGroup;
}

bool ParquetReader::readInt32Slice(parquet::ColumnReader* columnReader,
                                   std::vector<uint8_t>& scratch,
                                   size_t columnIndex,
                                   int64_t batchRows) {
    auto* typed = static_cast<parquet::Int32Reader*>(columnReader);
    int32_t* buffer = reinterpret_cast<int32_t*>(scratch.data());
    int64_t valuesRead = 0;
    typed->ReadBatch(batchRows, nullptr, nullptr, buffer, &valuesRead);
    const std::span<const int32_t> values(buffer, static_cast<size_t>(valuesRead));
    if (!_visitor.onInt32Values(columnIndex, values)) {
        _aborted = true;
        return false;
    }
    return true;
}

bool ParquetReader::readInt64Slice(parquet::ColumnReader* columnReader,
                                   std::vector<uint8_t>& scratch,
                                   size_t columnIndex,
                                   int64_t batchRows) {
    auto* typed = static_cast<parquet::Int64Reader*>(columnReader);
    int64_t* buffer = reinterpret_cast<int64_t*>(scratch.data());
    int64_t valuesRead = 0;
    typed->ReadBatch(batchRows, nullptr, nullptr, buffer, &valuesRead);
    const std::span<const int64_t> values(buffer, static_cast<size_t>(valuesRead));
    if (!_visitor.onInt64Values(columnIndex, values)) {
        _aborted = true;
        return false;
    }
    return true;
}

bool ParquetReader::readFloatSlice(parquet::ColumnReader* columnReader,
                                   std::vector<uint8_t>& scratch,
                                   size_t columnIndex,
                                   int64_t batchRows) {
    auto* typed = static_cast<parquet::FloatReader*>(columnReader);
    float* buffer = reinterpret_cast<float*>(scratch.data());
    int64_t valuesRead = 0;
    typed->ReadBatch(batchRows, nullptr, nullptr, buffer, &valuesRead);
    const std::span<const float> values(buffer, static_cast<size_t>(valuesRead));
    if (!_visitor.onFloatValues(columnIndex, values)) {
        _aborted = true;
        return false;
    }
    return true;
}

bool ParquetReader::readDoubleSlice(parquet::ColumnReader* columnReader,
                                    std::vector<uint8_t>& scratch,
                                    size_t columnIndex,
                                    int64_t batchRows) {
    auto* typed = static_cast<parquet::DoubleReader*>(columnReader);
    double* buffer = reinterpret_cast<double*>(scratch.data());
    int64_t valuesRead = 0;
    typed->ReadBatch(batchRows, nullptr, nullptr, buffer, &valuesRead);
    const std::span<const double> values(buffer, static_cast<size_t>(valuesRead));
    if (!_visitor.onDoubleValues(columnIndex, values)) {
        _aborted = true;
        return false;
    }
    return true;
}

bool ParquetReader::readBoolSlice(parquet::ColumnReader* columnReader,
                                  std::vector<uint8_t>& scratch,
                                  size_t columnIndex,
                                  int64_t batchRows) {
    auto* typed = static_cast<parquet::BoolReader*>(columnReader);
    bool* buffer = reinterpret_cast<bool*>(scratch.data());
    int64_t valuesRead = 0;
    typed->ReadBatch(batchRows, nullptr, nullptr, buffer, &valuesRead);
    const std::span<const bool> values(buffer, static_cast<size_t>(valuesRead));
    if (!_visitor.onBoolValues(columnIndex, values)) {
        _aborted = true;
        return false;
    }
    return true;
}

bool ParquetReader::readByteArraySlice(parquet::ColumnReader* columnReader,
                                       std::vector<uint8_t>& scratch,
                                       size_t columnIndex,
                                       int64_t batchRows) {
    auto* typed = static_cast<parquet::ByteArrayReader*>(columnReader);
    parquet::ByteArray* buffer = reinterpret_cast<parquet::ByteArray*>(scratch.data());
    int64_t valuesRead = 0;
    typed->ReadBatch(batchRows, nullptr, nullptr, buffer, &valuesRead);
    const std::span<const parquet::ByteArray> values(buffer, static_cast<size_t>(valuesRead));
    if (!_visitor.onByteArrayValues(columnIndex, values)) {
        _aborted = true;
        return false;
    }
    return true;
}

bool ParquetReader::readInt96Slice(parquet::ColumnReader* columnReader,
                                   std::vector<uint8_t>& scratch,
                                   size_t columnIndex,
                                   int64_t batchRows) {
    auto* typed = static_cast<parquet::Int96Reader*>(columnReader);
    parquet::Int96* buffer = reinterpret_cast<parquet::Int96*>(scratch.data());
    int64_t valuesRead = 0;
    typed->ReadBatch(batchRows, nullptr, nullptr, buffer, &valuesRead);
    const std::span<const parquet::Int96> values(buffer, static_cast<size_t>(valuesRead));
    if (!_visitor.onInt96Values(columnIndex, values)) {
        _aborted = true;
        return false;
    }
    return true;
}

bool ParquetReader::readFixedLenByteArraySlice(parquet::ColumnReader* columnReader,
                                               std::vector<uint8_t>& scratch,
                                               size_t columnIndex,
                                               size_t byteWidth,
                                               int64_t batchRows) {
    auto* typed = static_cast<parquet::FixedLenByteArrayReader*>(columnReader);
    parquet::FixedLenByteArray* buffer =
        reinterpret_cast<parquet::FixedLenByteArray*>(scratch.data());
    int64_t valuesRead = 0;
    typed->ReadBatch(batchRows, nullptr, nullptr, buffer, &valuesRead);
    const std::span<const parquet::FixedLenByteArray> values(
        buffer, static_cast<size_t>(valuesRead));
    if (!_visitor.onFixedLenByteArrayValues(columnIndex, values, byteWidth)) {
        _aborted = true;
        return false;
    }
    return true;
}

bool ParquetReader::readColumnSlice(size_t projectionIndex,
                                    size_t columnIndex,
                                    int64_t batchRows) {
    parquet::ColumnReader* columnReader = _columnReaders[projectionIndex].get();
    const parquet::SchemaDescriptor* schema = _fileMetadata->schema();
    const parquet::ColumnDescriptor* descriptor = schema->Column(static_cast<int>(columnIndex));

    std::vector<uint8_t>& scratch = _scratch[projectionIndex];
    const size_t neededBytes = static_cast<size_t>(batchRows) * sizeof(parquet::ByteArray);
    if (scratch.size() < neededBytes) {
        scratch.resize(neededBytes);
    }

    try {
        switch (descriptor->physical_type()) {
            case parquet::Type::INT32:
                return readInt32Slice(columnReader, scratch, columnIndex, batchRows);
            break;
            case parquet::Type::INT64:
                return readInt64Slice(columnReader, scratch, columnIndex, batchRows);
            break;
            case parquet::Type::FLOAT:
                return readFloatSlice(columnReader, scratch, columnIndex, batchRows);
            break;
            case parquet::Type::DOUBLE:
                return readDoubleSlice(columnReader, scratch, columnIndex, batchRows);
            break;
            case parquet::Type::BOOLEAN:
                return readBoolSlice(columnReader, scratch, columnIndex, batchRows);
            break;
            case parquet::Type::BYTE_ARRAY:
                return readByteArraySlice(columnReader, scratch, columnIndex, batchRows);
            break;
            case parquet::Type::INT96:
                return readInt96Slice(columnReader, scratch, columnIndex, batchRows);
            break;
            case parquet::Type::FIXED_LEN_BYTE_ARRAY: {
                const size_t byteWidth = static_cast<size_t>(descriptor->type_length());
                return readFixedLenByteArraySlice(columnReader,
                                                  scratch,
                                                  columnIndex,
                                                  byteWidth,
                                                  batchRows);
            }
            break;
            default:
                throw TuringException(fmt::format(
                    "Parquet: row group {}, column {}: unsupported physical type",
                    _currentRowGroup, columnIndex));
            break;
        }
    } catch (const parquet::ParquetException& e) {
        throw TuringException(fmt::format(
            "Parquet: row group {}, column {}: {}",
            _currentRowGroup, columnIndex, e.what()));
    }

    return true;
}

bool ParquetReader::nextChunk(size_t maxRows) {
    if (_fileEnded || _aborted) {
        return false;
    }

    if (!ensureFileOpen()) {
        return false;
    }

    if (!_rowGroupOpen) {
        if (_currentRowGroup >= _numRowGroups) {
            _fileEnded = true;
            _visitor.onFileEnd();
            return false;
        }

        if (!openRowGroup()) {
            return false;
        }
    }

    const size_t rowsRemaining = _rowsInRowGroup - _rowsConsumedInRowGroup;
    const size_t chunkRows = std::min(maxRows, rowsRemaining);
    const int64_t batchRows = static_cast<int64_t>(chunkRows);
    const size_t firstRowInRowGroup = _rowsConsumedInRowGroup;

    for (size_t projectionIndex = 0; projectionIndex < _columns.size(); ++projectionIndex) {
        const size_t columnIndex = _columns[projectionIndex];
        if (!readColumnSlice(projectionIndex, columnIndex, batchRows)) {
            return false;
        }
    }

    if (!_visitor.onChunkEnd(_currentRowGroup, firstRowInRowGroup, chunkRows)) {
        _aborted = true;
        return false;
    }

    _rowsConsumedInRowGroup += chunkRows;

    if (_rowsConsumedInRowGroup >= _rowsInRowGroup) {
        closeRowGroup();
        if (_aborted) {
            return false;
        }
    }

    return true;
}

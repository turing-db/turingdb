#include "ParquetReader.h"

#include <algorithm>
#include <type_traits>

#include <parquet/column_reader.h>
#include <parquet/exception.h>
#include <parquet/file_reader.h>
#include <parquet/metadata.h>
#include <parquet/schema.h>
#include <parquet/types.h>

#include <spdlog/fmt/fmt.h>

#include "BioAssert.h"
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

template <typename DType>
bool ParquetReader::readSlice(parquet::ColumnReader* columnReader,
                              std::vector<uint8_t>& scratch,
                              size_t columnIndex,
                              size_t batchRows,
                              size_t byteWidth) {
    using T = typename DType::c_type;
    auto* typed = static_cast<parquet::TypedColumnReader<DType>*>(columnReader);

    const bool hasRepLevels = typed->descr()->max_repetition_level() > 0;

    if (hasRepLevels) {
        // For repeated columns, batchRows is the flat row count — not the level
        // count. Each row can contribute multiple levels, so we drain the column
        // with HasNext() rather than counting by batchRows. The scratch buffers
        // are sized to DEFAULT_CHUNK_SIZE per sub-batch.
        const size_t subBatchSize = DEFAULT_CHUNK_SIZE;
        if (_repLevelScratch.size() < subBatchSize) {
            _repLevelScratch.resize(subBatchSize);
        }
        if (_defLevelScratch.size() < subBatchSize) {
            _defLevelScratch.resize(subBatchSize);
        }
        const size_t neededValueBytes = subBatchSize * sizeof(T);
        if (scratch.size() < neededValueBytes) {
            scratch.resize(neededValueBytes);
        }
        T* repeatedBuffer = reinterpret_cast<T*>(scratch.data());

        while (typed->HasNext()) {
            int64_t valuesRead = 0;
            const int64_t levelsRead = typed->ReadBatch(
                static_cast<int64_t>(subBatchSize),
                _defLevelScratch.data(), _repLevelScratch.data(),
                repeatedBuffer, &valuesRead);
            if (levelsRead <= 0) {
                break;
            }

            const std::span<const int16_t> repSpan(_repLevelScratch.data(), static_cast<size_t>(levelsRead));
            const std::span<const int16_t> defSpan(_defLevelScratch.data(), static_cast<size_t>(levelsRead));
            if (!_visitor.onLevels(columnIndex, repSpan, defSpan)) {
                _aborted = true;
                return false;
            }

            const std::span<const T> values(repeatedBuffer, static_cast<size_t>(valuesRead));

            bool visitorOk = true;
            if constexpr (std::is_same_v<DType, parquet::Int32Type>) {
                visitorOk = _visitor.onInt32Values(columnIndex, values);
            } else if constexpr (std::is_same_v<DType, parquet::Int64Type>) {
                visitorOk = _visitor.onInt64Values(columnIndex, values);
            } else if constexpr (std::is_same_v<DType, parquet::FloatType>) {
                visitorOk = _visitor.onFloatValues(columnIndex, values);
            } else if constexpr (std::is_same_v<DType, parquet::DoubleType>) {
                visitorOk = _visitor.onDoubleValues(columnIndex, values);
            } else if constexpr (std::is_same_v<DType, parquet::BooleanType>) {
                visitorOk = _visitor.onBoolValues(columnIndex, values);
            } else if constexpr (std::is_same_v<DType, parquet::ByteArrayType>) {
                visitorOk = _visitor.onByteArrayValues(columnIndex, values);
            } else if constexpr (std::is_same_v<DType, parquet::Int96Type>) {
                visitorOk = _visitor.onInt96Values(columnIndex, values);
            } else if constexpr (std::is_same_v<DType, parquet::FLBAType>) {
                visitorOk = _visitor.onFixedLenByteArrayValues(columnIndex, values, byteWidth);
            } else {
                static_assert(sizeof(DType) == 0, "Unsupported DType in readSlice");
            }

            if (!visitorOk) {
                _aborted = true;
                return false;
            }
        }
        return true;
    }

    // ReadBatch is allowed to stop at page boundaries and return fewer
    // values than requested, especially on BYTE_ARRAY columns with large
    // variable-length values. Loop until we've delivered batchRows values
    // (or the underlying reader reports zero). Invoke the visitor on each
    // sub-batch's values: parquet::ByteArray::ptr points into a page-owned
    // buffer that is invalidated by the next ReadBatch call, so the visitor
    // must consume each sub-batch before we read the next page.
    T* buffer = reinterpret_cast<T*>(scratch.data());
    size_t totalRead = 0;
    while (totalRead < batchRows) {
        int64_t valuesRead = 0;
        const int64_t levelsRead = typed->ReadBatch(
            static_cast<int64_t>(batchRows - totalRead),
            nullptr, nullptr,
            buffer, &valuesRead);
        if (levelsRead <= 0) {
            break;
        }

        const std::span<const T> values(buffer, static_cast<size_t>(valuesRead));

        bool visitorOk = true;
        if constexpr (std::is_same_v<DType, parquet::Int32Type>) {
            visitorOk = _visitor.onInt32Values(columnIndex, values);
        } else if constexpr (std::is_same_v<DType, parquet::Int64Type>) {
            visitorOk = _visitor.onInt64Values(columnIndex, values);
        } else if constexpr (std::is_same_v<DType, parquet::FloatType>) {
            visitorOk = _visitor.onFloatValues(columnIndex, values);
        } else if constexpr (std::is_same_v<DType, parquet::DoubleType>) {
            visitorOk = _visitor.onDoubleValues(columnIndex, values);
        } else if constexpr (std::is_same_v<DType, parquet::BooleanType>) {
            visitorOk = _visitor.onBoolValues(columnIndex, values);
        } else if constexpr (std::is_same_v<DType, parquet::ByteArrayType>) {
            visitorOk = _visitor.onByteArrayValues(columnIndex, values);
        } else if constexpr (std::is_same_v<DType, parquet::Int96Type>) {
            visitorOk = _visitor.onInt96Values(columnIndex, values);
        } else if constexpr (std::is_same_v<DType, parquet::FLBAType>) {
            visitorOk = _visitor.onFixedLenByteArrayValues(columnIndex, values, byteWidth);
        } else {
            static_assert(sizeof(DType) == 0, "Unsupported DType in readSlice");
        }

        if (!visitorOk) {
            _aborted = true;
            return false;
        }

        totalRead += static_cast<size_t>(levelsRead);
    }
    return true;
}

bool ParquetReader::readColumnSlice(size_t projectionIndex,
                                    size_t columnIndex,
                                    size_t batchRows) {
    bioassert(projectionIndex < _columns.size(), "Parquet: projection index out of range");

    parquet::ColumnReader* columnReader = _columnReaders[projectionIndex].get();
    const parquet::SchemaDescriptor* schema = _fileMetadata->schema();
    const parquet::ColumnDescriptor* descriptor = schema->Column(static_cast<int>(columnIndex));

    std::vector<uint8_t>& scratch = _scratch[projectionIndex];
    const size_t neededBytes = batchRows * sizeof(parquet::ByteArray);
    if (scratch.size() < neededBytes) {
        scratch.resize(neededBytes);
    }

    try {
        switch (descriptor->physical_type()) {
            case parquet::Type::INT32:
                return readSlice<parquet::Int32Type>(columnReader, scratch, columnIndex, batchRows);
            break;
            case parquet::Type::INT64:
                return readSlice<parquet::Int64Type>(columnReader, scratch, columnIndex, batchRows);
            break;
            case parquet::Type::FLOAT:
                return readSlice<parquet::FloatType>(columnReader, scratch, columnIndex, batchRows);
            break;
            case parquet::Type::DOUBLE:
                return readSlice<parquet::DoubleType>(columnReader, scratch, columnIndex, batchRows);
            break;
            case parquet::Type::BOOLEAN:
                return readSlice<parquet::BooleanType>(columnReader, scratch, columnIndex, batchRows);
            break;
            case parquet::Type::BYTE_ARRAY:
                return readSlice<parquet::ByteArrayType>(columnReader, scratch, columnIndex, batchRows);
            break;
            case parquet::Type::INT96:
                return readSlice<parquet::Int96Type>(columnReader, scratch, columnIndex, batchRows);
            break;
            case parquet::Type::FIXED_LEN_BYTE_ARRAY: {
                const size_t byteWidth = static_cast<size_t>(descriptor->type_length());
                return readSlice<parquet::FLBAType>(columnReader,
                                                    scratch,
                                                    columnIndex,
                                                    batchRows,
                                                    byteWidth);
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
    const size_t firstRowInRowGroup = _rowsConsumedInRowGroup;

    for (size_t projectionIndex = 0; projectionIndex < _columns.size(); ++projectionIndex) {
        const size_t columnIndex = _columns[projectionIndex];
        if (!readColumnSlice(projectionIndex, columnIndex, chunkRows)) {
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

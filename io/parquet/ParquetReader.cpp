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

    const parquet::SchemaDescriptor* schema = _fileMetadata->schema();
    for (const size_t columnIndex : _columns) {
        if (schema->Column(static_cast<int>(columnIndex))->max_repetition_level() > 0) {
            _hasRepeatedColumn = true;
            break;
        }
    }

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
bool ParquetReader::emitValues(size_t columnIndex,
                               std::span<const typename DType::c_type> values,
                               size_t byteWidth) {
    if constexpr (std::is_same_v<DType, parquet::Int32Type>) {
        return _visitor.onInt32Values(columnIndex, values);
    } else if constexpr (std::is_same_v<DType, parquet::Int64Type>) {
        return _visitor.onInt64Values(columnIndex, values);
    } else if constexpr (std::is_same_v<DType, parquet::FloatType>) {
        return _visitor.onFloatValues(columnIndex, values);
    } else if constexpr (std::is_same_v<DType, parquet::DoubleType>) {
        return _visitor.onDoubleValues(columnIndex, values);
    } else if constexpr (std::is_same_v<DType, parquet::BooleanType>) {
        return _visitor.onBoolValues(columnIndex, values);
    } else if constexpr (std::is_same_v<DType, parquet::ByteArrayType>) {
        return _visitor.onByteArrayValues(columnIndex, values);
    } else if constexpr (std::is_same_v<DType, parquet::Int96Type>) {
        return _visitor.onInt96Values(columnIndex, values);
    } else if constexpr (std::is_same_v<DType, parquet::FLBAType>) {
        return _visitor.onFixedLenByteArrayValues(columnIndex, values, byteWidth);
    } else {
        static_assert(sizeof(DType) == 0, "Unsupported DType in emitValues");
    }
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
        // Repeated columns are decoded a whole row group at a time: batchRows is a
        // flat row count, but each row contributes a variable number of levels, so
        // we drain the column with HasNext() (the caller sizes the chunk to a full
        // row group whenever a repeated column is present). Values are delivered per
        // sub-batch because parquet::ByteArray::ptr points into page-owned memory
        // that the next ReadBatch invalidates.
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
            if (!emitValues<DType>(columnIndex, values, byteWidth)) {
                _aborted = true;
                return false;
            }
        }
        return true;
    }

    const bool hasDefLevels = typed->descr()->max_definition_level() > 0;
    if (hasDefLevels && _defLevelScratch.size() < batchRows) {
        _defLevelScratch.resize(batchRows);
    }

    // ReadBatch may stop at a page boundary and return fewer values than requested.
    // ByteArray/FLBA values point into page-owned memory that the next ReadBatch
    // invalidates, so those must be delivered per sub-batch and consumed immediately.
    constexpr bool isPointerValue =
        std::is_same_v<DType, parquet::ByteArrayType> || std::is_same_v<DType, parquet::FLBAType>;

    if constexpr (isPointerValue) {
        T* buffer = reinterpret_cast<T*>(scratch.data());
        size_t totalRead = 0;
        while (totalRead < batchRows) {
            int64_t valuesRead = 0;
            int16_t* const defBuf = hasDefLevels ? _defLevelScratch.data() : nullptr;
            const int64_t levelsRead = typed->ReadBatch(
                static_cast<int64_t>(batchRows - totalRead),
                defBuf, nullptr,
                buffer, &valuesRead);
            if (levelsRead <= 0) {
                break;
            }

            if (hasDefLevels) {
                const std::span<const int16_t> defSpan(_defLevelScratch.data(), static_cast<size_t>(levelsRead));
                if (!_visitor.onLevels(columnIndex, {}, defSpan)) {
                    _aborted = true;
                    return false;
                }
            }

            const std::span<const T> values(buffer, static_cast<size_t>(valuesRead));
            if (!emitValues<DType>(columnIndex, values, byteWidth)) {
                _aborted = true;
                return false;
            }

            totalRead += static_cast<size_t>(levelsRead);
        }
        return true;
    } else {
        // Scalar values are copied into our buffer, so accumulate the whole slice
        // across sub-batches and deliver a single onLevels + values callback: the
        // graph visitors assign these spans and read them at chunk end, so they must
        // see the complete column at once rather than only the last sub-batch.
        if (scratch.size() < batchRows * sizeof(T)) {
            scratch.resize(batchRows * sizeof(T));
        }
        T* buffer = reinterpret_cast<T*>(scratch.data());

        size_t totalLevels = 0;
        size_t totalValues = 0;
        while (totalLevels < batchRows) {
            int64_t valuesRead = 0;
            int16_t* const defBuf = hasDefLevels ? (_defLevelScratch.data() + totalLevels) : nullptr;
            const int64_t levelsRead = typed->ReadBatch(
                static_cast<int64_t>(batchRows - totalLevels),
                defBuf, nullptr,
                buffer + totalValues, &valuesRead);
            if (levelsRead <= 0) {
                break;
            }

            totalLevels += static_cast<size_t>(levelsRead);
            totalValues += static_cast<size_t>(valuesRead);
        }

        if (hasDefLevels) {
            const std::span<const int16_t> defSpan(_defLevelScratch.data(), totalLevels);
            if (!_visitor.onLevels(columnIndex, {}, defSpan)) {
                _aborted = true;
                return false;
            }
        }

        const std::span<const T> values(buffer, totalValues);
        if (!emitValues<DType>(columnIndex, values, byteWidth)) {
            _aborted = true;
            return false;
        }
        return true;
    }
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
    // Repeated columns are decoded a whole row group at a time, so when one is
    // present the chunk must span the full row group; otherwise honour maxRows.
    const size_t chunkRows = _hasRepeatedColumn ? rowsRemaining : std::min(maxRows, rowsRemaining);
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

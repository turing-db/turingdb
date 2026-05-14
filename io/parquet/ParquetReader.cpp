#include "ParquetReader.h"

#include <functional>

#include <parquet/column_reader.h>
#include <parquet/exception.h>
#include <parquet/file_reader.h>
#include <parquet/metadata.h>
#include <parquet/schema.h>
#include <parquet/types.h>

#include <spdlog/fmt/fmt.h>

#include "TuringException.h"

using namespace db;

ParquetSaxVisitor::~ParquetSaxVisitor()
{
}

ParquetReader::ParquetReader(const fs::Path& path, ParquetSaxVisitor& visitor)
    : _path(path),
    _visitor(visitor)
{
    _scratch.resize(DEFAULT_BATCH_SIZE * sizeof(parquet::ByteArray));
}

ParquetReader::~ParquetReader() {
}

template <typename ReaderT, typename ValueT, typename Callback>
bool ParquetReader::decodeAndFire(size_t rowGroupIndex,
                                  size_t columnIndex,
                                  parquet::ColumnReader& reader,
                                  Callback callback) {
    ReaderT* typed = static_cast<ReaderT*>(&reader);
    ValueT* buf = reinterpret_cast<ValueT*>(_scratch.data());
    constexpr int64_t batchSize = static_cast<int64_t>(DEFAULT_BATCH_SIZE);
    int64_t valuesRead = 0;

    auto fire = std::bind_front(callback, &_visitor);

    while (typed->HasNext()) {
        typed->ReadBatch(batchSize, nullptr, nullptr, buf, &valuesRead);
        const std::span<const ValueT> values(buf, static_cast<size_t>(valuesRead));
        if (!fire(rowGroupIndex, columnIndex, values)) {
            return false;
        }
    }

    return true;
}

void ParquetReader::read() {
    std::unique_ptr<parquet::ParquetFileReader> reader;
    try {
        reader = parquet::ParquetFileReader::OpenFile(_path.get());
    } catch (const parquet::ParquetException& e) {
        throw TuringException(fmt::format("Parquet: opening {}: {}", _path.get(), e.what()));
    }

    // metadata() returns shared_ptr<FileMetaData>
    // bind locally and use a raw pointer for the rest of the function.
    const auto fileMetadataSharedPtr = reader->metadata();
    const parquet::FileMetaData* fileMetadata = fileMetadataSharedPtr.get();

    if (!_visitor.onFileStart(*fileMetadata)) {
        return;
    }

    const size_t numRowGroups = static_cast<size_t>(fileMetadata->num_row_groups());
    const size_t numColumns = static_cast<size_t>(fileMetadata->num_columns());
    const parquet::SchemaDescriptor* schema = fileMetadata->schema();

    std::vector<size_t> columns;
    if (_projection.empty()) {
        columns.reserve(numColumns);
        for (size_t c = 0; c < numColumns; ++c) {
            columns.push_back(c);
        }
    } else {
        columns = _projection;
    }

    for (size_t rg = 0; rg < numRowGroups; ++rg) {
        const auto rgReaderPtr = reader->RowGroup(static_cast<int>(rg));
        parquet::RowGroupReader* rgReader = rgReaderPtr.get();
        const auto rgMetadata = fileMetadata->RowGroup(static_cast<int>(rg));

        if (!_visitor.onRowGroupStart(rg, *rgMetadata)) {
            return;
        }

        for (const size_t colIdx : columns) {
            if (colIdx >= numColumns) {
                throw TuringException(fmt::format(
                    "Parquet: row group {}: projection column {} out of range", rg, colIdx));
            }

            const parquet::ColumnDescriptor* descriptor =
                schema->Column(static_cast<int>(colIdx));
            const auto colReaderPtr = rgReader->Column(static_cast<int>(colIdx));
            parquet::ColumnReader* colReader = colReaderPtr.get();

            if (!_visitor.onColumnStart(rg, colIdx, *descriptor)) {
                return;
            }

            bool keepGoing = true;
            try {
                switch (descriptor->physical_type()) {
                    case parquet::Type::INT32: {
                        keepGoing = decodeAndFire<parquet::Int32Reader, int32_t>(rg,
                                                                                 colIdx,
                                                                                 *colReader,
                                                                                 &ParquetSaxVisitor::onInt32Values);
                    }
                    break;
                    case parquet::Type::INT64: {
                        keepGoing = decodeAndFire<parquet::Int64Reader, int64_t>(rg,
                                                                                 colIdx,
                                                                                 *colReader,
                                                                                 &ParquetSaxVisitor::onInt64Values);
                    }
                    break;
                    case parquet::Type::FLOAT: {
                        keepGoing = decodeAndFire<parquet::FloatReader, float>(rg,
                                                                               colIdx,
                                                                               *colReader,
                                                                               &ParquetSaxVisitor::onFloatValues);
                    }
                    break;
                    case parquet::Type::DOUBLE: {
                        keepGoing = decodeAndFire<parquet::DoubleReader, double>(rg,
                                                                                 colIdx,
                                                                                 *colReader,
                                                                                 &ParquetSaxVisitor::onDoubleValues);
                    }
                    break;
                    case parquet::Type::BOOLEAN: {
                        keepGoing = decodeAndFire<parquet::BoolReader, bool>(rg,
                                                                             colIdx,
                                                                             *colReader,
                                                                             &ParquetSaxVisitor::onBoolValues);
                    }
                    break;
                    case parquet::Type::BYTE_ARRAY: {
                        keepGoing = decodeAndFire<parquet::ByteArrayReader, parquet::ByteArray>(rg,
                                                                                                colIdx,
                                                                                                *colReader,
                                                                                                &ParquetSaxVisitor::onByteArrayValues);
                    }
                    break;
                    default: {
                        // INT96 and FIXED_LEN_BYTE_ARRAY not yet supported;
                        // onColumnStart still fired so users can detect.
                    }
                    break;
                }
            } catch (const parquet::ParquetException& e) {
                throw TuringException(fmt::format(
                    "Parquet: row group {}, column {}: {}", rg, colIdx, e.what()));
            }

            if (!keepGoing) {
                return;
            }

            if (!_visitor.onColumnEnd(rg, colIdx)) {
                return;
            }
        }

        if (!_visitor.onRowGroupEnd(rg)) {
            return;
        }
    }

    _visitor.onFileEnd();
}

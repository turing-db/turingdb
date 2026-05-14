#include "ParquetReader.h"

#include <parquet/column_reader.h>
#include <parquet/exception.h>
#include <parquet/file_reader.h>
#include <parquet/metadata.h>
#include <parquet/schema.h>

#include <spdlog/fmt/fmt.h>

#include "TuringException.h"

using namespace db;

ParquetSaxVisitor::~ParquetSaxVisitor() = default;

ParquetReader::ParquetReader(const fs::Path& path, ParquetSaxVisitor& visitor)
    : _path(path),
    _visitor(visitor)
{
}

ParquetReader::~ParquetReader() = default;

void ParquetReader::read() {
    std::unique_ptr<parquet::ParquetFileReader> reader;
    try {
        reader = parquet::ParquetFileReader::OpenFile(_path.get());
    } catch (const parquet::ParquetException& e) {
        throw TuringException(fmt::format("Parquet: opening {}: {}", _path.get(), e.what()));
    }

    // metadata() returns shared_ptr<FileMetaData>; bind locally and
    // use a raw pointer for the rest of the function.
    const auto fileMetadataSp = reader->metadata();
    const parquet::FileMetaData* fileMetadata = fileMetadataSp.get();

    if (!_visitor.onFileStart(*fileMetadata)) {
        return;
    }

    const int numRowGroups = fileMetadata->num_row_groups();
    const int numColumns = fileMetadata->num_columns();
    const parquet::SchemaDescriptor* schema = fileMetadata->schema();

    std::vector<int> columns;
    if (_projection.empty()) {
        columns.reserve(numColumns);
        for (int c = 0; c < numColumns; ++c) {
            columns.push_back(c);
        }
    } else {
        columns = _projection;
    }

    for (int rg = 0; rg < numRowGroups; ++rg) {
        // RowGroup(i) is shared_ptr in the parquet API; keep the local
        // binding tight and only pass the raw pointer further.
        const auto rgReaderSp = reader->RowGroup(rg);
        parquet::RowGroupReader* rgReader = rgReaderSp.get();
        const auto rgMetadata = fileMetadata->RowGroup(rg);

        if (!_visitor.onRowGroupStart(rg, *rgMetadata)) {
            return;
        }

        for (const int colIdx : columns) {
            if (colIdx < 0 || colIdx >= numColumns) {
                throw TuringException(fmt::format(
                    "Parquet: row group {}: projection column {} out of range", rg, colIdx));
            }

            const parquet::ColumnDescriptor* descriptor = schema->Column(colIdx);
            const auto colReaderSp = rgReader->Column(colIdx);
            parquet::ColumnReader* colReader = colReaderSp.get();

            try {
                if (!_visitor.onColumnChunk(rg, colIdx, *descriptor, *colReader)) {
                    return;
                }
            } catch (const parquet::ParquetException& e) {
                throw TuringException(fmt::format(
                    "Parquet: row group {}, column {}: {}", rg, colIdx, e.what()));
            }
        }

        if (!_visitor.onRowGroupEnd(rg)) {
            return;
        }
    }

    _visitor.onFileEnd();
}

#include "ParquetWriter.h"

#include <stddef.h>
#include <stdint.h>

#include <limits>
#include <memory>
#include <string>
#include <vector>

#include <arrow/io/file.h>
#include <arrow/result.h>
#include <arrow/status.h>
#include <arrow/util/key_value_metadata.h>
#include <parquet/column_writer.h>
#include <parquet/exception.h>
#include <parquet/file_writer.h>
#include <parquet/properties.h>
#include <parquet/schema.h>
#include <parquet/types.h>

#include <spdlog/fmt/fmt.h>

#include "ParquetWriteSchema.h"

#include "BioAssert.h"
#include "TuringException.h"

using namespace db;

namespace {

// Translate one ParquetWriteSchema column into the matching parquet schema node:
// a REQUIRED primitive node carrying the column name, physical type, and (for
// the annotated / fixed-length kinds) its converted type and byte width.
parquet::schema::NodePtr buildSchemaNode(const ParquetWriteSchema& schema, size_t index) {
    const std::string& name = schema.getColumnName(index);

    switch (schema.getColumnType(index)) {
        case ParquetColumnType::Int64:
            return parquet::schema::PrimitiveNode::Make(name,
                                                        parquet::Repetition::REQUIRED,
                                                        parquet::Type::INT64);
        break;
        case ParquetColumnType::UInt64:
            return parquet::schema::PrimitiveNode::Make(name,
                                                        parquet::Repetition::REQUIRED,
                                                        parquet::Type::INT64,
                                                        parquet::ConvertedType::UINT_64);
        break;
        case ParquetColumnType::Double:
            return parquet::schema::PrimitiveNode::Make(name,
                                                        parquet::Repetition::REQUIRED,
                                                        parquet::Type::DOUBLE);
        break;
        case ParquetColumnType::Bool:
            return parquet::schema::PrimitiveNode::Make(name,
                                                        parquet::Repetition::REQUIRED,
                                                        parquet::Type::BOOLEAN);
        break;
        case ParquetColumnType::String:
            return parquet::schema::PrimitiveNode::Make(name,
                                                        parquet::Repetition::REQUIRED,
                                                        parquet::Type::BYTE_ARRAY,
                                                        parquet::ConvertedType::UTF8);
        break;
        case ParquetColumnType::FixedLenBytes:
            return parquet::schema::PrimitiveNode::Make(name,
                                                        parquet::Repetition::REQUIRED,
                                                        parquet::Type::FIXED_LEN_BYTE_ARRAY,
                                                        parquet::ConvertedType::NONE,
                                                        static_cast<int>(schema.getColumnByteWidth(index)));
        break;
    }

    bioassert(false, "ParquetWriter: unhandled column type");
    return nullptr;
}

}

ParquetWriter::ParquetWriter(const fs::Path& path, const ParquetWriteSchema& schema)
    : _path(path),
    _schema(schema)
{
}

void ParquetWriter::ensureOpen() {
    if (_fileWriter) {
        return;
    }

    parquet::schema::NodeVector fields;
    fields.reserve(_schema.getColumnCount());
    for (size_t index = 0; index < _schema.getColumnCount(); ++index) {
        fields.push_back(buildSchemaNode(_schema, index));
    }

    const auto groupNode = std::static_pointer_cast<parquet::schema::GroupNode>(
        parquet::schema::GroupNode::Make("schema", parquet::Repetition::REQUIRED, fields));

    parquet::WriterProperties::Builder builder;
    builder.compression(parquet::Compression::ZSTD);
    const auto properties = builder.build();

    auto streamResult = arrow::io::FileOutputStream::Open(_path.get());
    if (!streamResult.ok()) {
        throw TuringException(fmt::format(
            "Parquet: opening {} for writing: {}",
            _path.get(), streamResult.status().ToString()));
    }
    _outputStream = *streamResult;

    try {
        _fileWriter = parquet::ParquetFileWriter::Open(_outputStream, groupNode, properties);
    } catch (const parquet::ParquetException& e) {
        throw TuringException(fmt::format(
            "Parquet: opening {} for writing: {}", _path.get(), e.what()));
    }
}

ParquetWriter::~ParquetWriter() {
    if (!_finished && _fileWriter) {
        // Best-effort close so a caller that skipped finish() does not trigger a
        // throwing destructor in the underlying writer. The file is incomplete.
        try {
            _fileWriter->Close();
        } catch (...) {
        }
    }
}

void ParquetWriter::beginColumn(size_t columnIndex, size_t valueCount) const {
    bioassert(!_finished, "ParquetWriter: write after finish");
    bioassert(_rowGroupWriter != nullptr, "ParquetWriter: write before beginRowGroup");
    bioassert(columnIndex < _schema.getColumnCount(),
              "ParquetWriter: column index out of range");
    bioassert(columnIndex == _nextColumnIndex,
              "ParquetWriter: columns must be written in schema order");
    bioassert(valueCount == _currentRowCount,
              "ParquetWriter: column value count does not match the row group row count");
}

void ParquetWriter::beginRowGroup(size_t rowCount) {
    bioassert(!_finished, "ParquetWriter: beginRowGroup after finish");
    bioassert(_nextColumnIndex == 0 || _nextColumnIndex == _schema.getColumnCount(),
              "ParquetWriter: previous row group is missing columns");

    ensureOpen();

    try {
        _rowGroupWriter = _fileWriter->AppendRowGroup();
    } catch (const parquet::ParquetException& e) {
        throw TuringException(fmt::format("Parquet: appending row group: {}", e.what()));
    }

    _currentRowCount = rowCount;
    _nextColumnIndex = 0;
}

void ParquetWriter::writeInt64Column(size_t columnIndex, std::span<const int64_t> values) {
    beginColumn(columnIndex, values.size());

    const ParquetColumnType type = _schema.getColumnType(columnIndex);
    const bool isInteger = (type == ParquetColumnType::Int64)
                        || (type == ParquetColumnType::UInt64);
    bioassert(isInteger, "ParquetWriter: writeInt64Column on a non-integer column");

    try {
        auto* writer = static_cast<parquet::Int64Writer*>(_rowGroupWriter->NextColumn());
        writer->WriteBatch(static_cast<int64_t>(values.size()), nullptr, nullptr, values.data());
    } catch (const parquet::ParquetException& e) {
        throw TuringException(fmt::format(
            "Parquet: writing int64 column {}: {}", columnIndex, e.what()));
    }

    ++_nextColumnIndex;
}

void ParquetWriter::writeDoubleColumn(size_t columnIndex, std::span<const double> values) {
    beginColumn(columnIndex, values.size());
    bioassert(_schema.getColumnType(columnIndex) == ParquetColumnType::Double,
              "ParquetWriter: writeDoubleColumn on a non-double column");

    try {
        auto* writer = static_cast<parquet::DoubleWriter*>(_rowGroupWriter->NextColumn());
        writer->WriteBatch(static_cast<int64_t>(values.size()), nullptr, nullptr, values.data());
    } catch (const parquet::ParquetException& e) {
        throw TuringException(fmt::format(
            "Parquet: writing double column {}: {}", columnIndex, e.what()));
    }

    ++_nextColumnIndex;
}

void ParquetWriter::writeBoolColumn(size_t columnIndex, std::span<const bool> values) {
    beginColumn(columnIndex, values.size());
    bioassert(_schema.getColumnType(columnIndex) == ParquetColumnType::Bool,
              "ParquetWriter: writeBoolColumn on a non-bool column");

    try {
        auto* writer = static_cast<parquet::BoolWriter*>(_rowGroupWriter->NextColumn());
        writer->WriteBatch(static_cast<int64_t>(values.size()), nullptr, nullptr, values.data());
    } catch (const parquet::ParquetException& e) {
        throw TuringException(fmt::format(
            "Parquet: writing bool column {}: {}", columnIndex, e.what()));
    }

    ++_nextColumnIndex;
}

void ParquetWriter::writeStringColumn(size_t columnIndex,
                                      std::span<const std::string_view> values) {
    beginColumn(columnIndex, values.size());
    bioassert(_schema.getColumnType(columnIndex) == ParquetColumnType::String,
              "ParquetWriter: writeStringColumn on a non-string column");

    _byteArrayScratch.clear();
    _byteArrayScratch.reserve(values.size());
    for (const std::string_view value : values) {
        // parquet::ByteArray carries a 32-bit length; refuse rather than truncate.
        if (value.size() > std::numeric_limits<uint32_t>::max()) {
            throw TuringException(fmt::format(
                "Parquet: writing string column {}: a {}-byte value exceeds the ByteArray limit",
                columnIndex, value.size()));
        }

        _byteArrayScratch.emplace_back(static_cast<uint32_t>(value.size()),
                                       reinterpret_cast<const uint8_t*>(value.data()));
    }

    try {
        auto* writer = static_cast<parquet::ByteArrayWriter*>(_rowGroupWriter->NextColumn());
        writer->WriteBatch(static_cast<int64_t>(_byteArrayScratch.size()),
                           nullptr,
                           nullptr,
                           _byteArrayScratch.data());
    } catch (const parquet::ParquetException& e) {
        throw TuringException(fmt::format(
            "Parquet: writing string column {}: {}", columnIndex, e.what()));
    }

    ++_nextColumnIndex;
}

void ParquetWriter::writeFixedLenColumn(size_t columnIndex,
                                        std::span<const float> flat,
                                        size_t byteWidth) {
    bioassert(byteWidth % sizeof(float) == 0,
              "ParquetWriter: FixedLenBytes byteWidth must be a multiple of sizeof(float)");
    const size_t floatsPerValue = byteWidth / sizeof(float);
    bioassert(floatsPerValue > 0, "ParquetWriter: FixedLenBytes byteWidth must be positive");
    bioassert(flat.size() % floatsPerValue == 0,
              "ParquetWriter: flat float count is not a multiple of the value width");
    const size_t valueCount = flat.size() / floatsPerValue;

    beginColumn(columnIndex, valueCount);
    bioassert(_schema.getColumnType(columnIndex) == ParquetColumnType::FixedLenBytes,
              "ParquetWriter: writeFixedLenColumn on a non-FixedLenBytes column");
    bioassert(byteWidth == _schema.getColumnByteWidth(columnIndex),
              "ParquetWriter: byteWidth does not match the schema column");

    const auto* base = reinterpret_cast<const uint8_t*>(flat.data());
    _fixedLenByteArrayScratch.clear();
    _fixedLenByteArrayScratch.reserve(valueCount);
    for (size_t value = 0; value < valueCount; ++value) {
        _fixedLenByteArrayScratch.emplace_back(base + value * byteWidth);
    }

    try {
        auto* writer = static_cast<parquet::FixedLenByteArrayWriter*>(
            _rowGroupWriter->NextColumn());
        writer->WriteBatch(static_cast<int64_t>(_fixedLenByteArrayScratch.size()),
                           nullptr,
                           nullptr,
                           _fixedLenByteArrayScratch.data());
    } catch (const parquet::ParquetException& e) {
        throw TuringException(fmt::format(
            "Parquet: writing fixed-length column {}: {}", columnIndex, e.what()));
    }

    ++_nextColumnIndex;
}

void ParquetWriter::setMetadata(std::string_view key, std::string_view value) {
    bioassert(!_finished, "ParquetWriter: setMetadata after finish");
    _metadataKeys.emplace_back(key);
    _metadataValues.emplace_back(value);
}

void ParquetWriter::finish() {
    bioassert(!_finished, "ParquetWriter: finish called twice");
    bioassert(_nextColumnIndex == 0 || _nextColumnIndex == _schema.getColumnCount(),
              "ParquetWriter: final row group is missing columns");

    // A writer that never opened a row group still produces a valid empty file.
    ensureOpen();

    try {
        if (!_metadataKeys.empty()) {
            const auto metadata = std::make_shared<arrow::KeyValueMetadata>(
                _metadataKeys, _metadataValues);
            _fileWriter->AddKeyValueMetadata(metadata);
        }
        _fileWriter->Close();
    } catch (const parquet::ParquetException& e) {
        throw TuringException(fmt::format("Parquet: finalizing file: {}", e.what()));
    }

    const arrow::Status status = _outputStream->Close();
    if (!status.ok()) {
        throw TuringException(fmt::format(
            "Parquet: closing output stream: {}", status.ToString()));
    }

    _finished = true;
}

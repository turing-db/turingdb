#include "ParquetImportVisitor.h"

#include <string>
#include <utility>

#include <parquet/types.h>

#include "metadata/PropertyType.h"
#include "versioning/CommitBuilder.h"
#include "writers/MetadataBuilder.h"

#include "TuringException.h"

using namespace db;

bool ParquetImportVisitor::onInt32Values(size_t, std::span<const int32_t>) {
    throw TuringException("INT32 columns are not supported. Please use INT64.");
}

bool ParquetImportVisitor::onFloatValues(size_t, std::span<const float>) {
    throw TuringException("FLOAT columns are not supported. Please use DOUBLE.");
}

bool ParquetImportVisitor::onDoubleValues(size_t columnIndex, std::span<const double> values) {
    if (_propertyColumns.contains(columnIndex)) {
        _propDoubleVals[columnIndex] = values;
    }
    return true;
}

bool ParquetImportVisitor::onBoolValues(size_t columnIndex, std::span<const bool> values) {
    if (_propertyColumns.contains(columnIndex)) {
        _propBoolVals[columnIndex] = values;
    }
    return true;
}

void ParquetImportVisitor::discoverPropertyColumn(size_t columnIndex,
                                                  const std::string& path,
                                                  parquet::Type::type physicalType,
                                                  int16_t maxDefLevel) {
    MetadataBuilder& metadataBuilder = _builder->metadata();

    ValueType valueType = ValueType::Invalid;
    // FIXME: Byte arrays always strings. Check for lists, etc.
    switch (physicalType) {
        case parquet::Type::INT64:
            valueType = ValueType::Int64;
        break;
        case parquet::Type::BYTE_ARRAY:
            valueType = ValueType::String;
        break;
        case parquet::Type::BOOLEAN:
            valueType = ValueType::Bool;
        break;

        case parquet::Type::DOUBLE:
            valueType = ValueType::Double;
        break;

        case parquet::Type::FLOAT:
        case parquet::Type::INT32:
        case parquet::Type::INT96:
        case parquet::Type::FIXED_LEN_BYTE_ARRAY:
        case parquet::Type::UNDEFINED:
            throw TuringException(
                fmt::format("Unsupported column type (parquet::Type::{}).",
                            std::to_underlying(physicalType)));
            break;
    }

    const PropertyType propType = metadataBuilder.getOrCreatePropertyType(path, valueType);

    PropertyColumn col {.name = path,
                        .valueType = valueType,
                        .propertyTypeID = propType._id,
                        .maxDefLevel = maxDefLevel};

    _propertyColumns[columnIndex] = std::move(col);
}

void ParquetImportVisitor::capturePropertyLevels(size_t columnIndex,
                                                std::span<const int16_t> defLevels) {
    if (!_propertyColumns.contains(columnIndex)) {
        return;
    }

    std::vector<int16_t>& levels = _propDefLevels[columnIndex];
    levels.insert(end(levels), begin(defLevels), end(defLevels));
}

void ParquetImportVisitor::capturePropertyInt64(size_t columnIndex,
                                               std::span<const int64_t> values) {
    if (_propertyColumns.contains(columnIndex)) {
        _propInt64Vals[columnIndex] = values;
    }
}

void ParquetImportVisitor::capturePropertyByteArray(size_t columnIndex,
                                                   std::span<const parquet::ByteArray> values) {
    if (!_propertyColumns.contains(columnIndex)) {
        return;
    }

    // Each ByteArray::ptr points into page-owned memory that the next ReadBatch
    // invalidates, and a string column can span several data pages within one chunk
    // (readSlice delivers ByteArrays one page at a time). Copy the bytes into owned
    // storage now and accumulate across pages; they are read back at onChunkEnd.
    std::vector<std::string>& strings = _propByteArrayVals[columnIndex];
    strings.reserve(strings.size() + values.size());
    for (const parquet::ByteArray& bytes : values) {
        if (!bytes.ptr || bytes.len == 0) {
            strings.emplace_back();
        } else {
            strings.emplace_back(reinterpret_cast<const char*>(bytes.ptr), bytes.len);
        }
    }
}

void ParquetImportVisitor::resetPropertyChunk() {
    for (auto& [_, levels] : _propDefLevels) {
        levels.clear();
    }

    // Cleared, not erased, so the per-column buffers keep their capacity for reuse.
    for (auto& [_, strings] : _propByteArrayVals) {
        strings.clear();
    }
}

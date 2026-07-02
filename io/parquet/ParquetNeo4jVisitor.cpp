#include "ParquetNeo4jVisitor.h"

#include <string>

#include <parquet/types.h>
#include <utility>

#include "TuringException.h"
#include "metadata/PropertyType.h"
#include "versioning/CommitBuilder.h"
#include "writers/MetadataBuilder.h"

#include "FatalException.h"

using namespace db;

bool ParquetNeo4jVisitor::onInt32Values(size_t columnIndex, std::span<const int32_t> values) {
    return true;
}

bool ParquetNeo4jVisitor::onDoubleValues(size_t columnIndex, std::span<const double> values) {
    if (_propertyColumns.contains(columnIndex)) {
        _propDoubleVals[columnIndex] = values;
    }
    return true;
}

bool ParquetNeo4jVisitor::onBoolValues(size_t columnIndex, std::span<const bool> values) {
    if (_propertyColumns.contains(columnIndex)) {
        _propBoolVals[columnIndex] = values;
    }
    return true;
}

void ParquetNeo4jVisitor::discoverPropertyColumn(size_t columnIndex,
                                                 const std::string& path,
                                                 parquet::Type::type physicalType,
                                                 int16_t maxDefLevel) {
    MetadataBuilder& metadataBuilder = _builder->metadata();

    ValueType valueType = ValueType::Invalid;
    // FIXME: Byte arrays always strings. Check for lists, etc.
    switch (physicalType) {
        case parquet::Type::INT64:
        case parquet::Type::INT32:
            valueType = ValueType::Int64;
        break;
        case parquet::Type::BYTE_ARRAY:
            valueType = ValueType::String;
        break;
        case parquet::Type::BOOLEAN:
            valueType = ValueType::Bool;
        break;

        case parquet::Type::DOUBLE:
        case parquet::Type::FLOAT:
            valueType = ValueType::Double;
        break;

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

void ParquetNeo4jVisitor::capturePropertyLevels(size_t columnIndex,
                                                std::span<const int16_t> defLevels) {
    if (!_propertyColumns.contains(columnIndex)) {
        return;
    }

    std::vector<int16_t>& levels = _propDefLevels[columnIndex];
    levels.insert(end(levels), begin(defLevels), end(defLevels));
}

void ParquetNeo4jVisitor::capturePropertyInt64(size_t columnIndex,
                                               std::span<const int64_t> values) {
    if (_propertyColumns.contains(columnIndex)) {
        _propInt64Vals[columnIndex] = values;
    }
}

void ParquetNeo4jVisitor::capturePropertyByteArray(size_t columnIndex,
                                                   std::span<const parquet::ByteArray> values) {
    if (_propertyColumns.contains(columnIndex)) {
        _propByteArrayVals[columnIndex] = values;
    }
}

void ParquetNeo4jVisitor::resetPropertyChunk() {
    for (auto& [columnIndex, levels] : _propDefLevels) {
        levels.clear();
    }
}

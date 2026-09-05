#include "ParquetImportVisitor.h"

#include <string>
#include <utility>

#include <parquet/types.h>

#include "list/ListBuffer.h"
#include "metadata/PropertyType.h"
#include "versioning/CommitBuilder.h"
#include "writers/MetadataBuilder.h"

#include "BioAssert.h"
#include "TuringException.h"

using namespace db;

namespace {

// Parquet nests a LIST's values under synthetic group names, so the leaf path carries a
// suffix the property must not be named after.
std::string_view listPropertyName(const std::string& path) {
    static constexpr std::string_view suffixes[] = {".list.element", ".list.item", ".array"};

    for (const std::string_view suffix : suffixes) {
        if (path.ends_with(suffix)) {
            return std::string_view {path}.substr(0, path.size() - suffix.size());
        }
    }

    return path;
}

}

bool ParquetImportVisitor::onInt32Values(size_t, std::span<const int32_t>) {
    throw TuringException("INT32 columns are not supported. Please use INT64.");
}

bool ParquetImportVisitor::onFloatValues(size_t, std::span<const float>) {
    throw TuringException("FLOAT columns are not supported. Please use DOUBLE.");
}

bool ParquetImportVisitor::onDoubleValues(size_t columnIndex, std::span<const double> values) {
    const auto it = _propertyColumns.find(columnIndex);
    if (it == end(_propertyColumns)) {
        return true;
    }

    if (it->second.valueType == ValueType::List) {
        std::vector<double>& captured = _propListDoubleVals[columnIndex];
        captured.insert(end(captured), begin(values), end(values));
    } else {
        _propDoubleVals[columnIndex] = values;
    }

    return true;
}

bool ParquetImportVisitor::onBoolValues(size_t columnIndex, std::span<const bool> values) {
    const auto it = _propertyColumns.find(columnIndex);
    if (it == end(_propertyColumns)) {
        return true;
    }

    if (it->second.valueType == ValueType::List) {
        std::vector<uint8_t>& captured = _propListBoolVals[columnIndex];
        captured.insert(end(captured), begin(values), end(values));
    } else {
        _propBoolVals[columnIndex] = values;
    }

    return true;
}

void ParquetImportVisitor::discoverPropertyColumn(size_t columnIndex,
                                                  const std::string& path,
                                                  parquet::Type::type physicalType,
                                                  int16_t maxDefLevel,
                                                  int16_t maxRepLevel) {
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

    const bool isList = maxRepLevel > 0;
    if (isList) {
        valueType = ValueType::List;
    }

    const std::string name {isList ? listPropertyName(path) : std::string_view {path}};
    const PropertyType propType = metadataBuilder.getOrCreatePropertyType(name, valueType);

    PropertyColumn col {.name = name,
                        .valueType = valueType,
                        .propertyTypeID = propType._id,
                        .physicalType = physicalType,
                        .maxDefLevel = maxDefLevel,
                        .maxRepLevel = maxRepLevel};

    _propertyColumns[columnIndex] = std::move(col);
}

void ParquetImportVisitor::capturePropertyLevels(size_t columnIndex,
                                                std::span<const int16_t> repLevels,
                                                std::span<const int16_t> defLevels) {
    const auto it = _propertyColumns.find(columnIndex);
    if (it == end(_propertyColumns)) {
        return;
    }

    std::vector<int16_t>& levels = _propDefLevels[columnIndex];
    levels.insert(end(levels), begin(defLevels), end(defLevels));

    if (it->second.valueType == ValueType::List) {
        std::vector<int16_t>& repeats = _propRepLevels[columnIndex];
        repeats.insert(end(repeats), begin(repLevels), end(repLevels));
    }
}

void ParquetImportVisitor::capturePropertyInt64(size_t columnIndex,
                                               std::span<const int64_t> values) {
    const auto it = _propertyColumns.find(columnIndex);
    if (it == end(_propertyColumns)) {
        return;
    }

    if (it->second.valueType == ValueType::List) {
        std::vector<int64_t>& captured = _propListInt64Vals[columnIndex];
        captured.insert(end(captured), begin(values), end(values));
    } else {
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

void ParquetImportVisitor::buildListProperties(size_t columnIndex,
                                               const PropertyColumn& prop,
                                               size_t numRows) {
    _chunkLists.assign(numRows, std::nullopt);

    const std::vector<int16_t>& repLevels = _propRepLevels[columnIndex];
    const std::vector<int16_t>& defLevels = _propDefLevels[columnIndex];
    bioassert(repLevels.size() == defLevels.size(), "List property: rep and def level counts differ");

    // A leaf under a LIST is one level deeper than the list itself, so an entry one below
    // the maximum is a null element while anything shallower carries no element at all -
    // an empty list, or, at level zero, a row with no value for the column.
    const int16_t elementDefLevel = prop.maxDefLevel;
    const int16_t nullElementDefLevel = static_cast<int16_t>(prop.maxDefLevel - 1);

    std::vector<ListContainer::ListItemVariant> elements;
    size_t valueIndex = 0;
    size_t row = 0;
    bool rowHasValue = false;

    const auto storeRow = [&]() {
        if (row > 0 && rowHasValue) {
            _chunkLists[row - 1] = _listScratch.insert(elements);
        }
        elements.clear();
    };

    for (size_t i = 0; i < repLevels.size(); i++) {
        const bool nextRow = repLevels[i] == 0;
        if (nextRow) {
            storeRow();

            if (row == numRows) {
                throw TuringException(fmt::format(
                    "List property '{}': more lists than rows in the chunk.", prop.name));
            }

            row++;
            rowHasValue = defLevels[i] != 0;
        }

        if (defLevels[i] == elementDefLevel) {
            elements.push_back(listElement(prop, columnIndex, valueIndex));
            valueIndex++;
        } else if (defLevels[i] == nullElementDefLevel) {
            elements.push_back(PropertyNull {});
        }
    }

    storeRow();
}

ListContainer::ListItemVariant ParquetImportVisitor::listElement(const PropertyColumn& prop,
                                                                 size_t columnIndex,
                                                                 size_t valueIndex) {
    switch (prop.physicalType) {
        case parquet::Type::INT64:
            return _propListInt64Vals.at(columnIndex).at(valueIndex);
        break;
        case parquet::Type::DOUBLE:
            return _propListDoubleVals.at(columnIndex).at(valueIndex);
        break;
        case parquet::Type::BOOLEAN:
            return types::Bool::Primitive(_propListBoolVals.at(columnIndex).at(valueIndex) != 0);
        break;
        case parquet::Type::BYTE_ARRAY:
            return types::String::Primitive {_propByteArrayVals.at(columnIndex).at(valueIndex)};
        break;

        case parquet::Type::FLOAT:
        case parquet::Type::INT32:
        case parquet::Type::INT96:
        case parquet::Type::FIXED_LEN_BYTE_ARRAY:
        case parquet::Type::UNDEFINED:
        break;
    }

    throw TuringException(fmt::format("List property '{}': unsupported element type.", prop.name));
}

void ParquetImportVisitor::resetPropertyChunk() {
    for (auto& [_, levels] : _propDefLevels) {
        levels.clear();
    }

    for (auto& [_, levels] : _propRepLevels) {
        levels.clear();
    }

    // Cleared, not erased, so the per-column buffers keep their capacity for reuse.
    for (auto& [_, strings] : _propByteArrayVals) {
        strings.clear();
    }

    for (auto& [_, values] : _propListInt64Vals) {
        values.clear();
    }

    for (auto& [_, values] : _propListDoubleVals) {
        values.clear();
    }

    for (auto& [_, values] : _propListBoolVals) {
        values.clear();
    }

    _listScratch.clear();
}

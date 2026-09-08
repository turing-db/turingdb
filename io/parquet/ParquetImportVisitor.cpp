#include "ParquetImportVisitor.h"

#include <ranges>
#include <string>
#include <utility>
#include <vector>

#include <parquet/schema.h>
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
// suffix the property must not be named after - one per level of nesting, so a list of
// lists carries the suffix twice.
std::string_view listPropertyName(const std::string& path) {
    static constexpr std::string_view suffixes[] = {".list.element", ".list.item", ".array"};

    std::string_view name {path};

    bool stripped = true;
    while (stripped) {
        stripped = false;

        for (const std::string_view suffix : suffixes) {
            if (name.ends_with(suffix)) {
                name = name.substr(0, name.size() - suffix.size());
                stripped = true;
                break;
            }
        }
    }

    return name;
}

// The definition level at which the list at each repetition depth holds an element.
// Walking the schema path root to leaf, a repeated node opens the next depth and both it
// and an optional node deepen the level a value has to reach to be present at all.
void collectListDefLevels(const parquet::ColumnDescriptor& descriptor,
                          std::vector<int16_t>& listDefLevels) {
    std::vector<const parquet::schema::Node*> ancestors;
    for (const parquet::schema::Node* node = descriptor.schema_node().get();
         node != nullptr && node->parent() != nullptr;
         node = node->parent()) {
        ancestors.push_back(node);
    }

    listDefLevels.assign(descriptor.max_repetition_level() + 1, 0);

    int16_t definition = 0;
    int16_t repetition = 0;
    for (const parquet::schema::Node* node : ancestors | std::views::reverse) {
        if (node->is_repeated()) {
            repetition++;
            definition++;
            listDefLevels[repetition] = definition;
        } else if (node->is_optional()) {
            definition++;
        }
    }
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
                                                  const parquet::ColumnDescriptor& descriptor) {
    MetadataBuilder& metadataBuilder = _builder->metadata();

    const parquet::Type::type physicalType = descriptor.physical_type();
    const int16_t maxDefLevel = descriptor.max_definition_level();
    const int16_t maxRepLevel = descriptor.max_repetition_level();

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

    if (isList) {
        collectListDefLevels(descriptor, col.listDefLevels);
    }

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

    const size_t maxDepth = static_cast<size_t>(prop.maxRepLevel);
    const int16_t rowPresentDefLevel = static_cast<int16_t>(prop.listDefLevels[1] - 1);

    // One accumulator per repetition depth; a deeper one is closed into its parent
    // before that parent moves on, so a list is built before the list holding it.
    std::vector<std::vector<ListContainer::ListItemVariant>> openLists(maxDepth + 1);
    size_t openDepth = 0;
    size_t valueIndex = 0;
    size_t row = 0;
    bool rowHasValue = false;
    ListView rowList;

    const auto closeDepth = [&](size_t depth) {
        const ListView list = _listScratch.insert(openLists[depth]);
        openLists[depth].clear();

        if (depth == 1) {
            rowList = list;
        } else {
            openLists[depth - 1].push_back(list);
        }
    };

    const auto storeRow = [&]() {
        if (row == 0 || !rowHasValue) {
            return;
        }

        while (openDepth > 1) {
            closeDepth(openDepth);
            openDepth--;
        }

        closeDepth(1);
        _chunkLists[row - 1] = rowList;
    };

    for (size_t i = 0; i < repLevels.size(); i++) {
        const int16_t repetition = repLevels[i];
        const int16_t definition = defLevels[i];

        if (repetition == 0) {
            storeRow();

            if (row == numRows) {
                throw TuringException(fmt::format(
                    "List property '{}': more lists than rows in the chunk.", prop.name));
            }

            row++;
            rowHasValue = definition >= rowPresentDefLevel;
            openDepth = rowHasValue ? 1 : 0;
            openLists[1].clear();
        } else {
            while (openDepth > static_cast<size_t>(repetition)) {
                closeDepth(openDepth);
                openDepth--;
            }
        }

        if (!rowHasValue) {
            continue;
        }

        size_t presentDepth = 0;
        while (presentDepth < maxDepth && prop.listDefLevels[presentDepth + 1] <= definition) {
            presentDepth++;
        }

        while (openDepth < presentDepth) {
            openDepth++;
            openLists[openDepth].clear();
        }

        if (presentDepth == maxDepth) {
            if (definition == prop.maxDefLevel) {
                openLists[maxDepth].push_back(listElement(prop, columnIndex, valueIndex));
                valueIndex++;
            } else {
                openLists[maxDepth].push_back(PropertyNull {});
            }
        } else if (presentDepth > 0) {
            // The entry stops short of a leaf, so what sits at the next depth is the
            // whole element: an empty list where the level reaches that list, a null
            // where it does not.
            const bool emptyList =
                definition == static_cast<int16_t>(prop.listDefLevels[presentDepth + 1] - 1);

            if (emptyList) {
                openLists[presentDepth].push_back(_listScratch.insert({}));
            } else {
                openLists[presentDepth].push_back(PropertyNull {});
            }
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

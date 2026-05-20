#include "ParquetPropertyAnalyzer.h"

#include <stddef.h>

#include <string_view>

#include <parquet/types.h>

#include <nlohmann/json.hpp>
#include <spdlog/fmt/fmt.h>

#include "ParquetPropertyAnalysis.h"
#include "ParquetSchema.h"

#include "TuringException.h"

using namespace db;

namespace {

constexpr size_t MAX_PREVIEW_LENGTH = 120;

// Count the total number of leaf (non-group) fields under a schema field, recursively.
size_t countLeaves(const ParquetSchemaField& field) {
    if (!field.isGroup()) {
        return 1;
    }

    size_t total = 0;
    const size_t childCount = field.getChildCount();
    for (size_t childIndex = 0; childIndex < childCount; ++childIndex) {
        total += countLeaves(field.getChild(childIndex));
    }
    return total;
}

// Map a `nlohmann::json` value's runtime tag to a `ParquetJsonValueType`.
ParquetJsonValueType jsonTypeOf(const nlohmann::json& value) {
    switch (value.type()) {
        case nlohmann::json::value_t::null:
            return ParquetJsonValueType::NIL;
        break;
        case nlohmann::json::value_t::boolean:
            return ParquetJsonValueType::BOOLEAN;
        break;
        case nlohmann::json::value_t::number_integer:
        case nlohmann::json::value_t::number_unsigned:
            return ParquetJsonValueType::INTEGER;
        break;
        case nlohmann::json::value_t::number_float:
            return ParquetJsonValueType::FLOAT;
        break;
        case nlohmann::json::value_t::string:
            return ParquetJsonValueType::STRING;
        break;
        case nlohmann::json::value_t::array:
            return ParquetJsonValueType::ARRAY;
        break;
        case nlohmann::json::value_t::object:
            return ParquetJsonValueType::OBJECT;
        break;
        default:
            return ParquetJsonValueType::NIL;
        break;
    }
}

// Build a `"key": value` preview string, truncated with an ellipsis when longer than MAX_PREVIEW_LENGTH.
std::string buildPreview(const std::string& key, const nlohmann::json& value) {
    std::string preview = "\"" + key + "\": " + value.dump();
    if (preview.size() > MAX_PREVIEW_LENGTH) {
        preview.resize(MAX_PREVIEW_LENGTH);
        preview += "...";
    }
    return preview;
}

// Walk a JSON value and record types into `propertyType`, recursing into object sub-properties and array elements.
void recordJsonRecursive(ParquetPropertyType& propertyType, const nlohmann::json& value) {
    const ParquetJsonValueType type = jsonTypeOf(value);
    propertyType.recordValue(type);

    if (type == ParquetJsonValueType::OBJECT) {
        for (const auto& entry : value.items()) {
            recordJsonRecursive(propertyType.getOrCreateSubProperty(entry.key()),
                                entry.value());
        }
    } else if (type == ParquetJsonValueType::ARRAY) {
        if (value.empty()) {
            return;
        }
        ParquetPropertyType& elementType = propertyType.getOrCreateElementType();
        for (const auto& element : value) {
            recordJsonRecursive(elementType, element);
        }
    }
}

}

ParquetPropertyAnalyzer::ParquetPropertyAnalyzer(const ParquetSchema& schema,
                                                 const std::string& columnName,
                                                 ParquetPropertyAnalysis& analysis)
    : _columnName(columnName),
    _analysis(analysis)
{
    const ParquetSchemaField& root = schema.getRoot();
    const size_t childCount = root.getChildCount();

    size_t leafIndex = 0;
    const ParquetSchemaField* target = nullptr;
    for (size_t childIndex = 0; childIndex < childCount; ++childIndex) {
        const ParquetSchemaField& child = root.getChild(childIndex);
        if (child.getName() == columnName) {
            target = &child;
            _targetColumnIndex = leafIndex;
            break;
        }
        leafIndex += countLeaves(child);
    }

    if (target == nullptr) {
        throw TuringException(fmt::format("Column '{}' not found in the schema",
                                          columnName));
    } else if (target->isGroup()) {
        throw TuringException(fmt::format("Column '{}' is a group, expected a BYTE_ARRAY primitive",
                                          columnName));
    } else if (target->getPrimitiveType() != ParquetPrimitiveType::BYTE_ARRAY) {
        throw TuringException(fmt::format("Column '{}' is not a BYTE_ARRAY column",
                                          columnName));
    } else if (target->getJsonShape() != ParquetJsonShape::KEY_VALUE) {
        throw TuringException(fmt::format("Column '{}' is not key-value JSON",
                                          columnName));
    }
}

ParquetPropertyAnalyzer::~ParquetPropertyAnalyzer() {
}

bool ParquetPropertyAnalyzer::onByteArrayValues(size_t columnIndex,
                                                 std::span<const parquet::ByteArray> values) {
    if (columnIndex != _targetColumnIndex) {
        return true;
    }

    for (const parquet::ByteArray& value : values) {
        if (value.len == 0) {
            continue;
        }

        const std::string_view text(reinterpret_cast<const char*>(value.ptr),
                                    static_cast<size_t>(value.len));
        const auto json = nlohmann::json::parse(text, nullptr, /*allow_exceptions=*/false);
        if (json.is_discarded() || !json.is_object()) {
            continue;
        }

        for (const auto& entry : json.items()) {
            const ParquetJsonValueType type = jsonTypeOf(entry.value());
            _analysis.recordValue(type);
            recordJsonRecursive(_analysis.getOrCreatePropertyType(entry.key()),
                                entry.value());

            if (type == ParquetJsonValueType::ARRAY) {
                _analysis.recordArrayPreview(buildPreview(entry.key(), entry.value()));
            } else if (type == ParquetJsonValueType::OBJECT) {
                _analysis.recordObjectPreview(buildPreview(entry.key(), entry.value()));
            }
        }
    }

    return true;
}

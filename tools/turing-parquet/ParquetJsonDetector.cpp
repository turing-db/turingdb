#include "ParquetJsonDetector.h"

#include <string_view>
#include <vector>

#include <parquet/metadata.h>
#include <parquet/schema.h>
#include <parquet/types.h>

#include <nlohmann/json.hpp>

#include "ParquetSchema.h"

using namespace db;

namespace {

void collectLeaves(ParquetSchemaField& field,
                   std::vector<ParquetSchemaField*>& outLeaves) {
    if (field.isGroup()) {
        const size_t childCount = field.getChildCount();
        for (size_t childIndex = 0; childIndex < childCount; ++childIndex) {
            collectLeaves(field.getChild(childIndex), outLeaves);
        }
    } else {
        outLeaves.push_back(&field);
    }
}

ParquetJsonShape classifyJson(const parquet::ByteArray& value) {
    if (value.len == 0) {
        return ParquetJsonShape::NONE;
    }

    const std::string_view text(reinterpret_cast<const char*>(value.ptr),
                                static_cast<size_t>(value.len));
    const auto json = nlohmann::json::parse(text, nullptr, /*allow_exceptions=*/false);
    if (json.is_discarded()) {
        return ParquetJsonShape::NONE;
    }

    if (json.is_object()) {
        return ParquetJsonShape::KEY_VALUE;
    }
    return ParquetJsonShape::GENERAL;
}

}

ParquetJsonDetector::ParquetJsonDetector(ParquetSchema& schema, size_t sampleTarget)
    : _schema(schema),
    _sampleTarget(sampleTarget)
{
}

ParquetJsonDetector::~ParquetJsonDetector() {
}

bool ParquetJsonDetector::onFileStart(const parquet::FileMetaData& metadata) {
    std::vector<ParquetSchemaField*> leaves;
    collectLeaves(_schema.getRoot(), leaves);

    const parquet::SchemaDescriptor* schemaDescriptor = metadata.schema();
    const size_t columnCount = static_cast<size_t>(schemaDescriptor->num_columns());

    for (size_t columnIndex = 0; columnIndex < columnCount; ++columnIndex) {
        const parquet::ColumnDescriptor* column =
            schemaDescriptor->Column(static_cast<int>(columnIndex));
        if (column->physical_type() != parquet::Type::BYTE_ARRAY) {
            continue;
        }

        ColumnState state;
        if (columnIndex < leaves.size()) {
            state.field = leaves[columnIndex];
        }
        _byteArrayColumns.emplace(columnIndex, state);
    }

    // Nothing to inspect when the file has no BYTE_ARRAY columns.
    return !_byteArrayColumns.empty();
}

bool ParquetJsonDetector::onByteArrayValues(size_t columnIndex,
                                            std::span<const parquet::ByteArray> values) {
    const auto it = _byteArrayColumns.find(columnIndex);
    if (it == _byteArrayColumns.end()) {
        return true;
    }

    ColumnState& state = it->second;
    for (const parquet::ByteArray& value : values) {
        if (state.samplesSeen >= _sampleTarget) {
            break;
        }
        if (value.len == 0) {
            continue;
        }

        ++state.samplesSeen;
        const ParquetJsonShape shape = classifyJson(value);
        if (shape == ParquetJsonShape::KEY_VALUE) {
            ++state.keyValueLooking;
            ++state.jsonLooking;
        } else if (shape == ParquetJsonShape::GENERAL) {
            ++state.jsonLooking;
        }
    }

    return true;
}

bool ParquetJsonDetector::onChunkEnd(size_t rowGroupIndex,
                                     size_t firstRowInRowGroup,
                                     size_t rows) {
    if (allColumnsSatisfied()) {
        markJsonFields();
        return false;
    }
    return true;
}

bool ParquetJsonDetector::onFileEnd() {
    markJsonFields();
    return true;
}

bool ParquetJsonDetector::allColumnsSatisfied() const {
    for (const auto& [columnIndex, state] : _byteArrayColumns) {
        if (state.samplesSeen < _sampleTarget) {
            return false;
        }
    }
    return true;
}

void ParquetJsonDetector::markJsonFields() {
    for (auto& [columnIndex, state] : _byteArrayColumns) {
        if (state.field == nullptr) {
            continue;
        }
        if (state.samplesSeen == 0) {
            continue;
        }
        if (state.jsonLooking != state.samplesSeen) {
            continue;
        }

        if (state.keyValueLooking == state.samplesSeen) {
            state.field->setJsonShape(ParquetJsonShape::KEY_VALUE);
        } else {
            state.field->setJsonShape(ParquetJsonShape::GENERAL);
        }
    }
}

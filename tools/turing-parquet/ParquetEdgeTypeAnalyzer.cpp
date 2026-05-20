#include "ParquetEdgeTypeAnalyzer.h"

#include <stddef.h>

#include <string_view>

#include <parquet/types.h>

#include <spdlog/fmt/fmt.h>

#include "ParquetEdgeTypeAnalysis.h"
#include "ParquetSchema.h"

#include "TuringException.h"

using namespace db;

namespace {

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

}

ParquetEdgeTypeAnalyzer::ParquetEdgeTypeAnalyzer(const ParquetSchema& schema,
                                                 const std::string& columnName,
                                                 ParquetEdgeTypeAnalysis& analysis)
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
    }
}

ParquetEdgeTypeAnalyzer::~ParquetEdgeTypeAnalyzer() {
}

bool ParquetEdgeTypeAnalyzer::onByteArrayValues(size_t columnIndex,
                                                std::span<const parquet::ByteArray> values) {
    if (columnIndex != _targetColumnIndex) {
        return true;
    }

    for (const parquet::ByteArray& value : values) {
        const std::string_view text(reinterpret_cast<const char*>(value.ptr),
                                    static_cast<size_t>(value.len));
        _analysis.recordType(text);
    }

    return true;
}

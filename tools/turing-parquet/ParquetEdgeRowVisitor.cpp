#include "ParquetEdgeRowVisitor.h"

#include <string_view>

#include <parquet/types.h>

#include <spdlog/fmt/fmt.h>

#include "ParquetGraphImporter.h"
#include "ParquetSchema.h"

#include "TuringException.h"

using namespace db;

namespace {

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

// Return the zero-based leaf-column index of a named top-level field, throwing if it is absent.
size_t resolveLeafIndex(const ParquetSchema& schema, const std::string& name) {
    const ParquetSchemaField& root = schema.getRoot();
    const size_t childCount = root.getChildCount();
    size_t leafIndex = 0;
    for (size_t childIndex = 0; childIndex < childCount; ++childIndex) {
        const ParquetSchemaField& child = root.getChild(childIndex);
        if (child.getName() == name) {
            return leafIndex;
        }
        leafIndex += countLeaves(child);
    }
    throw TuringException(fmt::format("Column '{}' not found in the schema", name));
}

}

ParquetEdgeRowVisitor::ParquetEdgeRowVisitor(const ParquetSchema& schema,
                                             const std::string& edgeTypeColumn,
                                             const std::string& propertyColumn,
                                             ParquetGraphImporter& importer)
    : _importer(importer),
    _fromColumnIndex(resolveLeafIndex(schema, "from")),
    _toColumnIndex(resolveLeafIndex(schema, "to")),
    _edgeTypeColumnIndex(resolveLeafIndex(schema, edgeTypeColumn)),
    _propertiesColumnIndex(resolveLeafIndex(schema, propertyColumn))
{
}

ParquetEdgeRowVisitor::~ParquetEdgeRowVisitor() {
}

bool ParquetEdgeRowVisitor::onByteArrayValues(size_t columnIndex,
                                              std::span<const parquet::ByteArray> values) {
    std::vector<std::string>* target = nullptr;
    if (columnIndex == _fromColumnIndex) {
        target = &_fromIds;
    } else if (columnIndex == _toColumnIndex) {
        target = &_toIds;
    } else if (columnIndex == _edgeTypeColumnIndex) {
        target = &_edgeTypes;
    } else if (columnIndex == _propertiesColumnIndex) {
        target = &_properties;
    } else {
        return true;
    }

    target->reserve(target->size() + values.size());
    for (const parquet::ByteArray& value : values) {
        target->emplace_back(reinterpret_cast<const char*>(value.ptr),
                             static_cast<size_t>(value.len));
    }
    return true;
}

bool ParquetEdgeRowVisitor::onChunkEnd(size_t rowGroupIndex,
                                       size_t firstRowInRowGroup,
                                       size_t rows) {
    for (size_t rowIndex = 0; rowIndex < rows; ++rowIndex) {
        const std::string_view fromId = (rowIndex < _fromIds.size()) ? std::string_view(_fromIds[rowIndex]) : std::string_view();
        const std::string_view toId = (rowIndex < _toIds.size()) ? std::string_view(_toIds[rowIndex]) : std::string_view();
        const std::string_view edgeType = (rowIndex < _edgeTypes.size()) ? std::string_view(_edgeTypes[rowIndex]) : std::string_view();
        const std::string_view properties = (rowIndex < _properties.size()) ? std::string_view(_properties[rowIndex]) : std::string_view();
        _importer.onEdgeRow(fromId, toId, edgeType, properties, /*undirected=*/false);
    }

    _fromIds.clear();
    _toIds.clear();
    _edgeTypes.clear();
    _properties.clear();
    return true;
}

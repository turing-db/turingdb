#include "ParquetNodeRowVisitor.h"

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

ParquetNodeRowVisitor::ParquetNodeRowVisitor(const ParquetSchema& schema,
                                             const std::string& propertyColumn,
                                             ParquetGraphImporter& importer)
    : _importer(importer),
    _idColumnIndex(resolveLeafIndex(schema, "id")),
    _labelColumnIndex(resolveLeafIndex(schema, "label")),
    _propertiesColumnIndex(resolveLeafIndex(schema, propertyColumn))
{
}

ParquetNodeRowVisitor::~ParquetNodeRowVisitor() {
}

bool ParquetNodeRowVisitor::onByteArrayValues(size_t columnIndex,
                                              std::span<const parquet::ByteArray> values) {
    std::vector<std::string>* target = nullptr;
    if (columnIndex == _idColumnIndex) {
        target = &_ids;
    } else if (columnIndex == _labelColumnIndex) {
        target = &_labels;
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

bool ParquetNodeRowVisitor::onChunkEnd(size_t rowGroupIndex,
                                       size_t firstRowInRowGroup,
                                       size_t rows) {
    for (size_t rowIndex = 0; rowIndex < rows; ++rowIndex) {
        const std::string_view id = (rowIndex < _ids.size()) ? std::string_view(_ids[rowIndex]) : std::string_view();
        const std::string_view label = (rowIndex < _labels.size()) ? std::string_view(_labels[rowIndex]) : std::string_view();
        const std::string_view properties = (rowIndex < _properties.size()) ? std::string_view(_properties[rowIndex]) : std::string_view();
        _importer.onNodeRow(id, label, properties);
    }

    _ids.clear();
    _labels.clear();
    _properties.clear();
    return true;
}

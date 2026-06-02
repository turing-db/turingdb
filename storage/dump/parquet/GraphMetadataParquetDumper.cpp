#include "GraphMetadataParquetDumper.h"

#include <stddef.h>
#include <stdint.h>

#include <array>
#include <string_view>
#include <vector>

#include "ParquetWriteSchema.h"
#include "ParquetWriter.h"

#include "CommitParquetLayout.h"
#include "metadata/EdgeTypeMap.h"
#include "metadata/GraphMetadata.h"
#include "metadata/LabelMap.h"
#include "metadata/LabelSet.h"
#include "metadata/LabelSetMap.h"
#include "metadata/PropertyType.h"
#include "metadata/PropertyTypeMap.h"
#include "Path.h"

using namespace db;

namespace {

constexpr std::string_view LABEL_ID_COLUMN = "label_id";
constexpr std::string_view EDGE_TYPE_ID_COLUMN = "edge_type_id";
constexpr std::string_view PROPERTY_TYPE_ID_COLUMN = "property_type_id";
constexpr std::string_view LABELSET_ID_COLUMN = "labelset_id";
constexpr std::string_view NAME_COLUMN = "name";
constexpr std::string_view VALUE_TYPE_COLUMN = "value_type";

constexpr std::array<std::string_view, 4> LABELSET_INTEGER_COLUMNS = {
    "integer_0", "integer_1", "integer_2", "integer_3"};

void dumpLabels(const LabelMap& labels, const fs::Path& path) {
    ParquetWriteSchema schema;
    schema.addColumn(LABEL_ID_COLUMN, ParquetColumnType::UInt64);
    schema.addColumn(NAME_COLUMN, ParquetColumnType::String);

    ParquetWriter writer(path, schema);

    const size_t count = labels.getCount();
    if (count > 0) {
        std::vector<int64_t> ids;
        std::vector<std::string_view> names;
        ids.reserve(count);
        names.reserve(count);

        for (const auto& [id, name] : labels) {
            ids.push_back(static_cast<int64_t>(id.getValue()));
            names.push_back(*name);
        }

        writer.beginRowGroup(count);
        writer.writeInt64Column(0, ids);
        writer.writeStringColumn(1, names);
    }

    writer.finish();
}

void dumpEdgeTypes(const EdgeTypeMap& edgeTypes, const fs::Path& path) {
    ParquetWriteSchema schema;
    schema.addColumn(EDGE_TYPE_ID_COLUMN, ParquetColumnType::UInt64);
    schema.addColumn(NAME_COLUMN, ParquetColumnType::String);

    ParquetWriter writer(path, schema);

    const size_t count = edgeTypes.getCount();
    if (count > 0) {
        std::vector<int64_t> ids;
        std::vector<std::string_view> names;
        ids.reserve(count);
        names.reserve(count);

        for (const auto& [id, name] : edgeTypes) {
            ids.push_back(static_cast<int64_t>(id.getValue()));
            names.push_back(*name);
        }

        writer.beginRowGroup(count);
        writer.writeInt64Column(0, ids);
        writer.writeStringColumn(1, names);
    }

    writer.finish();
}

void dumpPropertyTypes(const PropertyTypeMap& propTypes, const fs::Path& path) {
    ParquetWriteSchema schema;
    schema.addColumn(PROPERTY_TYPE_ID_COLUMN, ParquetColumnType::UInt64);
    schema.addColumn(VALUE_TYPE_COLUMN, ParquetColumnType::UInt64);
    schema.addColumn(NAME_COLUMN, ParquetColumnType::String);

    ParquetWriter writer(path, schema);

    const size_t count = propTypes.getCount();
    if (count > 0) {
        std::vector<int64_t> ids;
        std::vector<int64_t> valueTypes;
        std::vector<std::string_view> names;
        ids.reserve(count);
        valueTypes.reserve(count);
        names.reserve(count);

        for (const auto& [propertyType, name] : propTypes) {
            ids.push_back(static_cast<int64_t>(propertyType._id.getValue()));
            valueTypes.push_back(static_cast<int64_t>(static_cast<uint8_t>(propertyType._valueType)));
            names.push_back(*name);
        }

        writer.beginRowGroup(count);
        writer.writeInt64Column(0, ids);
        writer.writeInt64Column(1, valueTypes);
        writer.writeStringColumn(2, names);
    }

    writer.finish();
}

void dumpLabelsets(const LabelSetMap& labelsets, const fs::Path& path) {
    static_assert(LabelSet::IntegerCount == 4,
                  "GraphMetadataParquetDumper assumes a 4-integer LabelSet");

    ParquetWriteSchema schema;
    schema.addColumn(LABELSET_ID_COLUMN, ParquetColumnType::UInt64);
    for (size_t column = 0; column < LabelSet::IntegerCount; ++column) {
        schema.addColumn(LABELSET_INTEGER_COLUMNS[column], ParquetColumnType::UInt64);
    }

    ParquetWriter writer(path, schema);

    const size_t count = labelsets.getCount();
    if (count > 0) {
        std::vector<int64_t> ids;
        std::array<std::vector<int64_t>, LabelSet::IntegerCount> integers;
        ids.reserve(count);
        for (auto& column : integers) {
            column.reserve(count);
        }

        for (const auto& [id, labelset] : labelsets) {
            ids.push_back(static_cast<int64_t>(id.getValue()));
            const LabelSet::IntegerType* data = labelset->data();
            for (size_t column = 0; column < LabelSet::IntegerCount; ++column) {
                integers[column].push_back(static_cast<int64_t>(data[column]));
            }
        }

        writer.beginRowGroup(count);
        writer.writeInt64Column(0, ids);
        for (size_t column = 0; column < LabelSet::IntegerCount; ++column) {
            writer.writeInt64Column(column + 1, integers[column]);
        }
    }

    writer.finish();
}

}

void GraphMetadataParquetDumper::dump(const GraphMetadata& metadata, const fs::Path& commitDir) {
    dumpLabels(metadata.labels(), commitParquetLayout::labels(commitDir));
    dumpEdgeTypes(metadata.edgeTypes(), commitParquetLayout::edgeTypes(commitDir));
    dumpPropertyTypes(metadata.propTypes(), commitParquetLayout::propertyTypes(commitDir));
    dumpLabelsets(metadata.labelsets(), commitParquetLayout::labelsets(commitDir));
}

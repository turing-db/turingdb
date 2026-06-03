#include "GraphMetadataParquetLoader.h"

#include <stddef.h>
#include <stdint.h>

#include <array>
#include <span>
#include <string>
#include <string_view>
#include <vector>

#include <parquet/types.h>

#include <spdlog/fmt/fmt.h>

#include "ParquetReader.h"
#include "ParquetWriteSchema.h"

#include "CommitParquetLayout.h"
#include "metadata/EdgeTypeMap.h"
#include "metadata/GraphMetadata.h"
#include "metadata/LabelMap.h"
#include "metadata/LabelSet.h"
#include "metadata/LabelSetHandle.h"
#include "metadata/LabelSetMap.h"
#include "metadata/PropertyType.h"
#include "metadata/PropertyTypeMap.h"
#include "Path.h"

#include "FatalException.h"

using namespace db;

namespace layout = commitParquetLayout;

namespace {

// labels / edge-types: column 0 the id (INT64), column 1 the name (BYTE_ARRAY).
class NameTableVisitor : public ParquetSaxVisitor {
public:
    std::vector<int64_t> _ids;
    std::vector<std::string> _names;

    bool onInt64Values(size_t columnIndex, std::span<const int64_t> values) override {
        for (const int64_t value : values) {
            _ids.push_back(value);
        }
        return true;
    }

    bool onByteArrayValues(size_t columnIndex,
                           std::span<const parquet::ByteArray> values) override {
        for (const parquet::ByteArray& byteArray : values) {
            _names.emplace_back(reinterpret_cast<const char*>(byteArray.ptr), byteArray.len);
        }
        return true;
    }
};

// property-types: column 0 the id, column 1 the value type (both INT64), column 2 the
// name (BYTE_ARRAY).
class PropertyTypesVisitor : public ParquetSaxVisitor {
public:
    std::vector<int64_t> _ids;
    std::vector<int64_t> _valueTypes;
    std::vector<std::string> _names;

    bool onInt64Values(size_t columnIndex, std::span<const int64_t> values) override {
        std::vector<int64_t>& target = (columnIndex == 0) ? _ids : _valueTypes;
        for (const int64_t value : values) {
            target.push_back(value);
        }
        return true;
    }

    bool onByteArrayValues(size_t columnIndex,
                           std::span<const parquet::ByteArray> values) override {
        for (const parquet::ByteArray& byteArray : values) {
            _names.emplace_back(reinterpret_cast<const char*>(byteArray.ptr), byteArray.len);
        }
        return true;
    }
};

// labelsets: column 0 the id, columns 1..IntegerCount the LabelSet integers (all INT64).
class LabelsetsVisitor : public ParquetSaxVisitor {
public:
    std::vector<int64_t> _ids;
    std::array<std::vector<int64_t>, LabelSet::IntegerCount> _integers;

    bool onInt64Values(size_t columnIndex, std::span<const int64_t> values) override {
        std::vector<int64_t>& target = (columnIndex == 0) ? _ids : _integers[columnIndex - 1];
        for (const int64_t value : values) {
            target.push_back(value);
        }
        return true;
    }
};

// Every per-row loop below indexes the secondary columns by the id column's length, so
// a truncated or corrupt file must fail here rather than read out of bounds.
void checkColumnsAgree(bool columnsAgree, std::string_view table) {
    if (!columnsAgree) {
        throw FatalException(fmt::format(
            "GraphMetadataParquetLoader: {} columns have mismatched lengths", table));
    }
}

void readNameTable(const fs::Path& path,
                   std::string_view idColumnName,
                   std::string_view table,
                   NameTableVisitor& visitor) {
    ParquetWriteSchema expectedSchema;
    expectedSchema.addColumn(idColumnName, ParquetColumnType::UInt64);
    expectedSchema.addColumn(layout::NAME_COLUMN, ParquetColumnType::String);

    ParquetReader reader(path, visitor);
    reader.setExpectedSchema(expectedSchema);
    while (reader.nextChunk()) {
    }

    checkColumnsAgree(visitor._names.size() == visitor._ids.size(), table);
}

void loadLabels(const fs::Path& path, LabelMap& labels) {
    NameTableVisitor visitor;
    readNameTable(path, layout::LABEL_ID_COLUMN, "labels", visitor);

    for (size_t i = 0; i < visitor._ids.size(); ++i) {
        const LabelID id = labels.getOrCreate(visitor._names[i]);
        if (id.getValue() != static_cast<LabelID::Type>(visitor._ids[i])) {
            throw FatalException("GraphMetadataParquetLoader: label id mismatch on load");
        }
    }
}

void loadEdgeTypes(const fs::Path& path, EdgeTypeMap& edgeTypes) {
    NameTableVisitor visitor;
    readNameTable(path, layout::EDGE_TYPE_ID_COLUMN, "edge-types", visitor);

    for (size_t i = 0; i < visitor._ids.size(); ++i) {
        const EdgeTypeID id = edgeTypes.getOrCreate(visitor._names[i]);
        if (id.getValue() != static_cast<EdgeTypeID::Type>(visitor._ids[i])) {
            throw FatalException("GraphMetadataParquetLoader: edge type id mismatch on load");
        }
    }
}

void loadPropertyTypes(const fs::Path& path, PropertyTypeMap& propTypes) {
    PropertyTypesVisitor visitor;
    {
        ParquetWriteSchema expectedSchema;
        expectedSchema.addColumn(layout::PROPERTY_TYPE_ID_COLUMN, ParquetColumnType::UInt64);
        expectedSchema.addColumn(layout::VALUE_TYPE_COLUMN, ParquetColumnType::UInt64);
        expectedSchema.addColumn(layout::NAME_COLUMN, ParquetColumnType::String);

        ParquetReader reader(path, visitor);
        reader.setExpectedSchema(expectedSchema);
        while (reader.nextChunk()) {
        }
    }

    checkColumnsAgree(visitor._valueTypes.size() == visitor._ids.size()
                          && visitor._names.size() == visitor._ids.size(),
                      "property-types");

    for (size_t i = 0; i < visitor._ids.size(); ++i) {
        const ValueType valueType =
            static_cast<ValueType>(static_cast<uint8_t>(visitor._valueTypes[i]));
        const PropertyType propertyType = propTypes.getOrCreate(visitor._names[i], valueType);
        if (propertyType._id.getValue() != static_cast<PropertyTypeID::Type>(visitor._ids[i])) {
            throw FatalException("GraphMetadataParquetLoader: property type id mismatch on load");
        }
    }
}

void loadLabelsets(const fs::Path& path, LabelSetMap& labelsets) {
    static_assert(LabelSet::IntegerCount == 4,
                  "GraphMetadataParquetLoader assumes a 4-integer LabelSet");

    LabelsetsVisitor visitor;
    {
        ParquetWriteSchema expectedSchema;
        expectedSchema.addColumn(layout::LABELSET_ID_COLUMN, ParquetColumnType::UInt64);
        for (size_t column = 0; column < LabelSet::IntegerCount; ++column) {
            expectedSchema.addColumn(layout::LABELSET_INTEGER_COLUMNS[column], ParquetColumnType::UInt64);
        }

        ParquetReader reader(path, visitor);
        reader.setExpectedSchema(expectedSchema);
        while (reader.nextChunk()) {
        }
    }

    bool integerColumnsAgree = true;
    for (const std::vector<int64_t>& integerColumn : visitor._integers) {
        integerColumnsAgree = integerColumnsAgree && integerColumn.size() == visitor._ids.size();
    }
    checkColumnsAgree(integerColumnsAgree, "labelsets");

    for (size_t i = 0; i < visitor._ids.size(); ++i) {
        std::array<LabelSet::IntegerType, LabelSet::IntegerCount> integers;
        for (size_t column = 0; column < LabelSet::IntegerCount; ++column) {
            integers[column] = static_cast<LabelSet::IntegerType>(visitor._integers[column][i]);
        }

        const LabelSet labelset = LabelSet::fromIntegers(integers);
        const LabelSetHandle handle = labelsets.getOrCreate(labelset);
        if (handle.getID().getValue() != static_cast<LabelSetID::Type>(visitor._ids[i])) {
            throw FatalException("GraphMetadataParquetLoader: labelset id mismatch on load");
        }
    }
}

}

void GraphMetadataParquetLoader::load(const fs::Path& commitDir, GraphMetadata& out) {
    loadLabels(layout::labels(commitDir), out._labelMap);
    loadEdgeTypes(layout::edgeTypes(commitDir), out._edgeTypeMap);
    loadPropertyTypes(layout::propertyTypes(commitDir), out._propTypeMap);
    loadLabelsets(layout::labelsets(commitDir), out._labelsetMap);
}

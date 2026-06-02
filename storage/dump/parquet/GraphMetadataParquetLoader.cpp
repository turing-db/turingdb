#include "GraphMetadataParquetLoader.h"

#include <stddef.h>
#include <stdint.h>

#include <array>
#include <span>
#include <string>
#include <vector>

#include <parquet/types.h>

#include "ParquetReader.h"

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

void loadLabels(const fs::Path& path, LabelMap& labels) {
    NameTableVisitor visitor;
    {
        ParquetReader reader(path, visitor);
        while (reader.nextChunk()) {
        }
    }

    for (size_t i = 0; i < visitor._ids.size(); ++i) {
        const LabelID id = labels.getOrCreate(visitor._names[i]);
        if (id.getValue() != static_cast<LabelID::Type>(visitor._ids[i])) {
            throw FatalException("GraphMetadataParquetLoader: label id mismatch on load");
        }
    }
}

void loadEdgeTypes(const fs::Path& path, EdgeTypeMap& edgeTypes) {
    NameTableVisitor visitor;
    {
        ParquetReader reader(path, visitor);
        while (reader.nextChunk()) {
        }
    }

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
        ParquetReader reader(path, visitor);
        while (reader.nextChunk()) {
        }
    }

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
        ParquetReader reader(path, visitor);
        while (reader.nextChunk()) {
        }
    }

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
    loadLabels(commitParquetLayout::labels(commitDir), out._labelMap);
    loadEdgeTypes(commitParquetLayout::edgeTypes(commitDir), out._edgeTypeMap);
    loadPropertyTypes(commitParquetLayout::propertyTypes(commitDir), out._propTypeMap);
    loadLabelsets(commitParquetLayout::labelsets(commitDir), out._labelsetMap);
}

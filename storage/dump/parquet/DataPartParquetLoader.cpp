#include "DataPartParquetLoader.h"

#include <stddef.h>
#include <stdint.h>

#include <charconv>
#include <memory>
#include <optional>
#include <span>
#include <string_view>
#include <vector>

#include "ParquetReader.h"
#include "ParquetWriteSchema.h"

#include "DataPartParquetLayout.h"
#include "NodeContainerParquetLoader.h"
#include "EdgeContainerParquetLoader.h"
#include "EdgeIndexerParquetLoader.h"
#include "PropertyIndexerParquetLoader.h"
#include "PropertyContainerParquetLoader.h"
#include "StringPropertyIndexerParquetLoader.h"

#include "datapart/DataPart.h"
#include "datapart/NodeContainer.h"
#include "datapart/EdgeContainer.h"
#include "indexers/EdgeIndexer.h"
#include "indexers/StringPropertyIndexer.h"
#include "properties/PropertyManager.h"
#include "properties/PropertyContainer.h"
#include "metadata/PropertyType.h"
#include "versioning/DataPartID.h"
#include "Path.h"

#include "FatalException.h"

using namespace db;

namespace {

// Info file: three INT64 columns (data_part_id, first_node_id, first_edge_id), one row.
class InfoVisitor : public ParquetSaxVisitor {
public:
    uint64_t _dataPartId {0};
    uint64_t _firstNodeId {0};
    uint64_t _firstEdgeId {0};

    bool onInt64Values(size_t columnIndex, std::span<const int64_t> values) override {
        if (values.empty()) {
            return true;
        }
        const uint64_t value = static_cast<uint64_t>(values[0]);
        if (columnIndex == 0) {
            _dataPartId = value;
        } else if (columnIndex == 1) {
            _firstNodeId = value;
        } else {
            _firstEdgeId = value;
        }
        return true;
    }
};

InfoVisitor readInfo(const fs::Path& infoPath) {
    namespace layout = dataPartParquetLayout;

    ParquetWriteSchema expectedSchema;
    expectedSchema.addColumn(layout::DATA_PART_ID_COLUMN, ParquetColumnType::UInt64);
    expectedSchema.addColumn(layout::FIRST_NODE_ID_COLUMN, ParquetColumnType::UInt64);
    expectedSchema.addColumn(layout::FIRST_EDGE_ID_COLUMN, ParquetColumnType::UInt64);

    InfoVisitor infoVisitor;
    ParquetReader reader(infoPath, infoVisitor);
    reader.setExpectedSchema(expectedSchema);
    while (reader.nextChunk()) {
    }
    return infoVisitor;
}

// Matches "<prefix><digits>.parquet" and returns the property type id.
std::optional<uint64_t> parsePropertyTypeId(std::string_view filename, std::string_view prefix) {
    namespace layout = dataPartParquetLayout;
    if (!filename.starts_with(prefix) || !filename.ends_with(layout::PARQUET_SUFFIX)) {
        return std::nullopt;
    }

    const size_t start = prefix.size();
    const size_t end = filename.size() - layout::PARQUET_SUFFIX.size();
    if (end <= start) {
        return std::nullopt;
    }

    const std::string_view digits = filename.substr(start, end - start);
    uint64_t value = 0;
    const auto result = std::from_chars(digits.data(), digits.data() + digits.size(), value);
    if (result.ec != std::errc() || result.ptr != digits.data() + digits.size()) {
        return std::nullopt;
    }
    return value;
}

}

void DataPartParquetLoader::insertContainer(PropertyManager& manager,
                                            PropertyTypeID propertyTypeID,
                                            std::unique_ptr<PropertyContainer> container) {
    PropertyContainer* ptr = container.release();
    manager._map.emplace(propertyTypeID, ptr);

    switch (ptr->getValueType()) {
        case ValueType::UInt64:
            manager._uint64s.emplace(propertyTypeID, ptr);
        break;
        case ValueType::Int64:
            manager._int64s.emplace(propertyTypeID, ptr);
        break;
        case ValueType::Double:
            manager._doubles.emplace(propertyTypeID, ptr);
        break;
        case ValueType::String:
            manager._strings.emplace(propertyTypeID, ptr);
        break;
        case ValueType::Bool:
            manager._bools.emplace(propertyTypeID, ptr);
        break;
        case ValueType::Embedding:
            manager._embeddings.emplace(propertyTypeID, ptr);
        break;
        case ValueType::Invalid:
        case ValueType::_SIZE:
            throw FatalException("DataPartParquetLoader: invalid property value type");
        break;
    }
}

void DataPartParquetLoader::loadPropertyManager(const fs::Path& partDir,
                                                const LabelSetMap& labelsets,
                                                const fs::Path& indexerPath,
                                                std::string_view propsPrefix,
                                                PropertyManager& manager) {
    PropertyIndexerParquetLoader::load(indexerPath, labelsets, manager._indexers);

    const auto entries = partDir.listDir();
    if (!entries) {
        throw FatalException("DataPartParquetLoader: cannot list part directory");
    }

    for (const fs::Path& entry : entries.value()) {
        const std::optional<uint64_t> propertyTypeId = parsePropertyTypeId(entry.filename(), propsPrefix);
        if (!propertyTypeId) {
            continue;
        }

        std::unique_ptr<PropertyContainer> container = PropertyContainerParquetLoader::load(entry);
        insertContainer(manager,
                        PropertyTypeID {static_cast<PropertyTypeID::Type>(propertyTypeId.value())},
                        std::move(container));
    }
}

void DataPartParquetLoader::fillContainers(DataPart& part,
                                           const fs::Path& partDir,
                                           const LabelSetMap& labelsets) {
    namespace layout = dataPartParquetLayout;

    part._nodes = NodeContainerParquetLoader::load(layout::nodeRanges(partDir),
                                                   layout::nodeRecords(partDir),
                                                   labelsets);

    part._edges = EdgeContainerParquetLoader::load(layout::edgesOut(partDir),
                                                   layout::edgesIn(partDir));

    part._edgeIndexer = EdgeIndexerParquetLoader::load(layout::edgeIndexerNodeData(partDir),
                                                       layout::edgeIndexerOutSpans(partDir),
                                                       layout::edgeIndexerInSpans(partDir),
                                                       labelsets,
                                                       *part._edges);

    part._nodeProperties = std::make_unique<PropertyManager>();
    loadPropertyManager(partDir,
                        labelsets,
                        layout::nodePropIndexer(partDir),
                        layout::NODE_PROPS_PREFIX,
                        *part._nodeProperties);

    part._edgeProperties = std::make_unique<PropertyManager>();
    loadPropertyManager(partDir,
                        labelsets,
                        layout::edgePropIndexer(partDir),
                        layout::EDGE_PROPS_PREFIX,
                        *part._edgeProperties);

    part._nodeStrPropIdx = StringPropertyIndexerParquetLoader::load(layout::nodeStringIndexes(partDir),
                                                                    layout::nodeStringChildren(partDir),
                                                                    layout::nodeStringOwners(partDir));

    part._edgeStrPropIdx = StringPropertyIndexerParquetLoader::load(layout::edgeStringIndexes(partDir),
                                                                    layout::edgeStringChildren(partDir),
                                                                    layout::edgeStringOwners(partDir));

    part._initialized = true;
}

std::unique_ptr<DataPart> DataPartParquetLoader::load(const fs::Path& partDir,
                                                      const LabelSetMap& labelsets) {
    namespace layout = dataPartParquetLayout;

    const InfoVisitor info = readInfo(layout::info(partDir));

    auto part = std::make_unique<DataPart>(NodeID {info._firstNodeId},
                                           EdgeID {info._firstEdgeId},
                                           DataPartID {info._dataPartId});

    fillContainers(*part, partDir, labelsets);
    return part;
}

void DataPartParquetLoader::load(DataPart& part,
                                 const fs::Path& partDir,
                                 const LabelSetMap& labelsets) {
    namespace layout = dataPartParquetLayout;

    const InfoVisitor info = readInfo(layout::info(partDir));

    if (part.getID().get() != info._dataPartId) {
        throw FatalException("DataPartParquetLoader: part id does not match info.parquet");
    }

    part._firstNodeID = NodeID {info._firstNodeId};
    part._firstEdgeID = EdgeID {info._firstEdgeId};

    fillContainers(part, partDir, labelsets);
}

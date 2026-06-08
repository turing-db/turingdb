#include "NodeContainerParquetLoader.h"

#include <stddef.h>
#include <stdint.h>

#include <memory>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <vector>

#include <arrow/util/key_value_metadata.h>
#include <parquet/metadata.h>

#include "ParquetReader.h"
#include "ParquetWriteSchema.h"

#include "NodeContainerParquetLayout.h"
#include "ParquetMetadataParsing.h"

#include "datapart/NodeContainer.h"
#include "datapart/NodeRange.h"
#include "datapart/NodeRecord.h"
#include "metadata/LabelSetMap.h"
#include "Path.h"

#include "FatalException.h"

using namespace db;

namespace layout = nodeContainerParquetLayout;

namespace {

// Ranges file: three INT64 columns (labelset_id, first_node_id, count), one row per range.
struct RangesData {
    std::vector<int64_t> _labelsetIds;
    std::vector<int64_t> _firstNodeIds;
    std::vector<int64_t> _counts;
};

// Fills the caller-owned RangesData, routing each column by its index.
class RangesVisitor : public ParquetSaxVisitor {
public:
    explicit RangesVisitor(RangesData& data)
        : _data(data) {
    }

    bool onInt64Values(size_t columnIndex, std::span<const int64_t> values) override {
        if (columnIndex == 0) {
            append(_data._labelsetIds, values);
        } else if (columnIndex == 1) {
            append(_data._firstNodeIds, values);
        } else if (columnIndex == 2) {
            append(_data._counts, values);
        } else {
            throw FatalException("NodeContainerParquetLoader: unexpected ranges column");
        }
        return true;
    }

private:
    RangesData& _data;

    static void append(std::vector<int64_t>& target, std::span<const int64_t> values) {
        for (const int64_t value : values) {
            target.push_back(value);
        }
    }
};

// Records file: one INT64 labelset_id per node, plus the first node id in metadata.
struct RecordsData {
    uint64_t _firstNodeID {0};
    std::vector<int64_t> _labelsetIds;
};

// Fills the caller-owned RecordsData from the labelset_id column and the file metadata.
class RecordsVisitor : public ParquetSaxVisitor {
public:
    explicit RecordsVisitor(RecordsData& data)
        : _data(data) {
    }

    bool onFileStart(const parquet::FileMetaData& metadata) override {
        const auto& keyValueMetadata = metadata.key_value_metadata();
        bool hasFirstNodeID = false;
        if (keyValueMetadata) {
            for (int64_t i = 0; i < keyValueMetadata->size(); ++i) {
                if (keyValueMetadata->key(i) == layout::FIRST_NODE_ID_KEY) {
                    _data._firstNodeID = parseMetadataUint64(keyValueMetadata->key(i),
                                                             keyValueMetadata->value(i));
                    hasFirstNodeID = true;
                }
            }
        }

        if (!hasFirstNodeID) {
            throw FatalException("NodeContainerParquetLoader: missing first-node-id metadata");
        }
        return true;
    }

    bool onInt64Values(size_t columnIndex, std::span<const int64_t> values) override {
        for (const int64_t value : values) {
            _data._labelsetIds.push_back(value);
        }
        return true;
    }

private:
    RecordsData& _data;
};

LabelSetHandle resolveHandle(const LabelSetMap& labelsets, int64_t labelsetId) {
    const std::optional<LabelSetHandle> handle =
        labelsets.getValue(static_cast<LabelSetID::Type>(labelsetId));
    if (!handle) {
        throw FatalException("NodeContainerParquetLoader: unknown labelset id");
    }
    return handle.value();
}

}

std::unique_ptr<NodeContainer> NodeContainerParquetLoader::load(const fs::Path& rangesPath,
                                                               const fs::Path& recordsPath,
                                                               const LabelSetMap& labelsets) {
    RangesData ranges;
    {
        RangesVisitor visitor(ranges);

        ParquetWriteSchema expectedSchema;
        expectedSchema.addColumn(layout::LABELSET_ID_COLUMN, ParquetColumnType::UInt64);
        expectedSchema.addColumn(layout::FIRST_NODE_ID_COLUMN, ParquetColumnType::UInt64);
        expectedSchema.addColumn(layout::COUNT_COLUMN, ParquetColumnType::UInt64);

        ParquetReader reader(rangesPath, visitor);
        reader.setExpectedSchema(expectedSchema);
        while (reader.nextChunk()) {
        }
    }

    RecordsData records;
    {
        RecordsVisitor visitor(records);

        ParquetWriteSchema expectedSchema;
        expectedSchema.addColumn(layout::LABELSET_ID_COLUMN, ParquetColumnType::UInt64);

        ParquetReader reader(recordsPath, visitor);
        reader.setExpectedSchema(expectedSchema);
        while (reader.nextChunk()) {
        }
    }

    const bool rangeColumnsAgree = ranges._firstNodeIds.size() == ranges._labelsetIds.size()
                                   && ranges._counts.size() == ranges._labelsetIds.size();
    if (!rangeColumnsAgree) {
        throw FatalException("NodeContainerParquetLoader: ranges columns have mismatched lengths");
    }

    const uint64_t firstID = records._firstNodeID;
    const size_t nodeCount = records._labelsetIds.size();

    NodeContainer* container = new NodeContainer {firstID, nodeCount};

    for (size_t i = 0; i < ranges._labelsetIds.size(); ++i) {
        const LabelSetHandle handle = resolveHandle(labelsets, ranges._labelsetIds[i]);
        NodeRange& range = container->_ranges[handle];
        range._first = NodeID {static_cast<uint64_t>(ranges._firstNodeIds[i])};
        range._count = static_cast<size_t>(ranges._counts[i]);
    }

    container->_nodes.resize(nodeCount);
    for (size_t i = 0; i < nodeCount; ++i) {
        container->_nodes[i]._labelset = resolveHandle(labelsets, records._labelsetIds[i]);
    }

    return std::unique_ptr<NodeContainer>(container);
}

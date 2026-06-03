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

#include "datapart/NodeContainer.h"
#include "datapart/NodeRange.h"
#include "datapart/NodeRecord.h"
#include "metadata/LabelSetMap.h"
#include "Path.h"

#include "FatalException.h"

using namespace db;

namespace layout = nodeContainerParquetLayout;

namespace {

// Ranges file: three INT64 columns (labelset_id, first_node_id, count), one row
// per range, distinguished by column index.
class RangesVisitor : public ParquetSaxVisitor {
public:
    std::vector<int64_t> _labelsetIds;
    std::vector<int64_t> _firstNodeIds;
    std::vector<int64_t> _counts;

    bool onInt64Values(size_t columnIndex, std::span<const int64_t> values) override {
        if (columnIndex == 0) {
            append(_labelsetIds, values);
        } else if (columnIndex == 1) {
            append(_firstNodeIds, values);
        } else if (columnIndex == 2) {
            append(_counts, values);
        } else {
            throw FatalException("NodeContainerParquetLoader: unexpected ranges column");
        }
        return true;
    }

private:
    static void append(std::vector<int64_t>& target, std::span<const int64_t> values) {
        for (const int64_t value : values) {
            target.push_back(value);
        }
    }
};

// Records file: one INT64 labelset_id per node, plus the first node id in metadata.
class RecordsVisitor : public ParquetSaxVisitor {
public:
    uint64_t _firstNodeID {0};
    std::vector<int64_t> _labelsetIds;

    bool onFileStart(const parquet::FileMetaData& metadata) override {
        const auto& keyValueMetadata = metadata.key_value_metadata();
        bool hasFirstNodeID = false;
        if (keyValueMetadata) {
            for (int64_t i = 0; i < keyValueMetadata->size(); ++i) {
                if (keyValueMetadata->key(i) == layout::FIRST_NODE_ID_KEY) {
                    _firstNodeID = static_cast<uint64_t>(std::stoull(keyValueMetadata->value(i)));
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
            _labelsetIds.push_back(value);
        }
        return true;
    }
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
    RangesVisitor rangesVisitor;
    {
        ParquetWriteSchema expectedSchema;
        expectedSchema.addColumn(layout::LABELSET_ID_COLUMN, ParquetColumnType::UInt64);
        expectedSchema.addColumn(layout::FIRST_NODE_ID_COLUMN, ParquetColumnType::UInt64);
        expectedSchema.addColumn(layout::COUNT_COLUMN, ParquetColumnType::UInt64);

        ParquetReader reader(rangesPath, rangesVisitor);
        reader.setExpectedSchema(expectedSchema);
        while (reader.nextChunk()) {
        }
    }

    RecordsVisitor recordsVisitor;
    {
        ParquetWriteSchema expectedSchema;
        expectedSchema.addColumn(layout::LABELSET_ID_COLUMN, ParquetColumnType::UInt64);

        ParquetReader reader(recordsPath, recordsVisitor);
        reader.setExpectedSchema(expectedSchema);
        while (reader.nextChunk()) {
        }
    }

    const bool rangeColumnsAgree = rangesVisitor._firstNodeIds.size() == rangesVisitor._labelsetIds.size()
                                   && rangesVisitor._counts.size() == rangesVisitor._labelsetIds.size();
    if (!rangeColumnsAgree) {
        throw FatalException("NodeContainerParquetLoader: ranges columns have mismatched lengths");
    }

    const uint64_t firstID = recordsVisitor._firstNodeID;
    const size_t nodeCount = recordsVisitor._labelsetIds.size();

    NodeContainer* container = new NodeContainer {firstID, nodeCount};

    for (size_t i = 0; i < rangesVisitor._labelsetIds.size(); ++i) {
        const LabelSetHandle handle = resolveHandle(labelsets, rangesVisitor._labelsetIds[i]);
        NodeRange& range = container->_ranges[handle];
        range._first = NodeID {static_cast<uint64_t>(rangesVisitor._firstNodeIds[i])};
        range._count = static_cast<size_t>(rangesVisitor._counts[i]);
    }

    container->_nodes.resize(nodeCount);
    for (size_t i = 0; i < nodeCount; ++i) {
        container->_nodes[i]._labelset = resolveHandle(labelsets, recordsVisitor._labelsetIds[i]);
    }

    return std::unique_ptr<NodeContainer>(container);
}

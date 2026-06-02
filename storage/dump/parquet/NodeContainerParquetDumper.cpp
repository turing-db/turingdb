#include "NodeContainerParquetDumper.h"

#include <stddef.h>
#include <stdint.h>

#include <span>
#include <string_view>
#include <vector>

#include <spdlog/fmt/fmt.h>

#include "ParquetWriteSchema.h"
#include "ParquetWriter.h"

#include "datapart/NodeContainer.h"
#include "Path.h"

using namespace db;

namespace {

constexpr std::string_view LABELSET_ID_COLUMN = "labelset_id";
constexpr std::string_view FIRST_NODE_ID_COLUMN = "first_node_id";
constexpr std::string_view COUNT_COLUMN = "count";
constexpr std::string_view FIRST_NODE_ID_KEY = "turing.first_node_id";

}

void NodeContainerParquetDumper::dump(const NodeContainer& nodes,
                                      const fs::Path& rangesPath,
                                      const fs::Path& recordsPath) {
    // Ranges: one row per (labelset -> contiguous NodeID range).
    {
        const LabelSetIndexer<NodeRange>& indexer = nodes.getLabelSetIndexer();
        const size_t rangeCount = indexer.size();

        ParquetWriteSchema schema;
        schema.addColumn(LABELSET_ID_COLUMN, ParquetColumnType::UInt64);
        schema.addColumn(FIRST_NODE_ID_COLUMN, ParquetColumnType::UInt64);
        schema.addColumn(COUNT_COLUMN, ParquetColumnType::UInt64);

        ParquetWriter writer(rangesPath, schema);

        if (rangeCount > 0) {
            std::vector<int64_t> labelsetIds;
            std::vector<int64_t> firstNodeIds;
            std::vector<int64_t> counts;
            labelsetIds.reserve(rangeCount);
            firstNodeIds.reserve(rangeCount);
            counts.reserve(rangeCount);

            for (const auto& [labelset, range] : indexer) {
                labelsetIds.push_back(static_cast<int64_t>(labelset.getID().getValue()));
                firstNodeIds.push_back(static_cast<int64_t>(range._first.getValue()));
                counts.push_back(static_cast<int64_t>(range._count));
            }

            writer.beginRowGroup(rangeCount);
            writer.writeInt64Column(0, labelsetIds);
            writer.writeInt64Column(1, firstNodeIds);
            writer.writeInt64Column(2, counts);
        }

        writer.finish();
    }

    // Records: one labelset id per node, in node-id order.
    {
        const NodeContainer::NodeRecords& records = nodes.records();
        const size_t nodeCount = records.size();

        ParquetWriteSchema schema;
        schema.addColumn(LABELSET_ID_COLUMN, ParquetColumnType::UInt64);

        ParquetWriter writer(recordsPath, schema);
        writer.setMetadata(FIRST_NODE_ID_KEY,
                           fmt::format("{}", nodes.getFirstNodeID().getValue()));

        if (nodeCount > 0) {
            std::vector<int64_t> labelsetIds;
            labelsetIds.reserve(nodeCount);
            for (const NodeRecord& record : records) {
                labelsetIds.push_back(static_cast<int64_t>(record._labelset.getID().getValue()));
            }

            writer.beginRowGroup(nodeCount);
            writer.writeInt64Column(0, labelsetIds);
        }

        writer.finish();
    }
}

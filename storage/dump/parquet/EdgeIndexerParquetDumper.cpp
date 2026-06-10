#include "EdgeIndexerParquetDumper.h"

#include <stddef.h>
#include <stdint.h>

#include <span>
#include <string_view>
#include <vector>

#include <spdlog/fmt/fmt.h>

#include "ParquetWriteSchema.h"
#include "ParquetWriter.h"

#include "EdgeIndexerParquetLayout.h"

#include "indexers/EdgeIndexer.h"
#include "indexers/LabelSetIndexer.h"
#include "datapart/EdgeContainer.h"
#include "datapart/EdgeRecord.h"
#include "datapart/NodeEdgeData.h"
#include "Path.h"

using namespace db;

namespace layout = edgeIndexerParquetLayout;

namespace {

void writeSpansFile(const LabelSetIndexer<EdgeIndexer::EdgeSpans>& spansByLabelSet,
                    std::span<const EdgeRecord> edges,
                    const fs::Path& path) {
    size_t totalSpans = 0;
    for (const auto& [labelset, spans] : spansByLabelSet) {
        totalSpans += spans.size();
    }

    ParquetWriteSchema schema;
    schema.addColumn(layout::LABELSET_ID_COLUMN, ParquetColumnType::UInt64);
    schema.addColumn(layout::SPAN_OFFSET_COLUMN, ParquetColumnType::UInt64);
    schema.addColumn(layout::SPAN_COUNT_COLUMN, ParquetColumnType::UInt64);

    ParquetWriter writer(path, schema);

    if (totalSpans > 0) {
        std::vector<int64_t> labelsetIds;
        std::vector<int64_t> offsets;
        std::vector<int64_t> counts;
        labelsetIds.reserve(totalSpans);
        offsets.reserve(totalSpans);
        counts.reserve(totalSpans);

        for (const auto& [labelset, spans] : spansByLabelSet) {
            for (const EdgeIndexer::EdgeSpan& span : spans) {
                labelsetIds.push_back(static_cast<int64_t>(labelset.getID().getValue()));
                offsets.push_back(static_cast<int64_t>(span.data() - edges.data()));
                counts.push_back(static_cast<int64_t>(span.size()));
            }
        }

        writer.beginRowGroup(totalSpans);
        writer.writeInt64Column(0, labelsetIds);
        writer.writeInt64Column(1, offsets);
        writer.writeInt64Column(2, counts);
    }

    writer.finish();
}

}

void EdgeIndexerParquetDumper::dump(const EdgeIndexer& indexer,
                                    const fs::Path& nodeDataPath,
                                    const fs::Path& outSpansPath,
                                    const fs::Path& inSpansPath) {
    const std::span<const NodeEdgeData> nodeData = indexer.getNodeData();
    const size_t nodeCount = nodeData.size();

    // Per-node out/in ranges.
    {
        ParquetWriteSchema schema;
        schema.addColumn(layout::OUT_FIRST_COLUMN, ParquetColumnType::UInt64);
        schema.addColumn(layout::OUT_COUNT_COLUMN, ParquetColumnType::UInt64);
        schema.addColumn(layout::IN_FIRST_COLUMN, ParquetColumnType::UInt64);
        schema.addColumn(layout::IN_COUNT_COLUMN, ParquetColumnType::UInt64);

        ParquetWriter writer(nodeDataPath, schema);
        writer.setMetadata(layout::FIRST_NODE_ID_KEY, fmt::format("{}", indexer.getFirstNodeID().getValue()));
        writer.setMetadata(layout::FIRST_EDGE_ID_KEY, fmt::format("{}", indexer.getFirstEdgeID().getValue()));
        writer.setMetadata(layout::CORE_NODE_COUNT_KEY, fmt::format("{}", indexer.getCoreNodeCount()));
        writer.setMetadata(layout::PATCH_NODE_COUNT_KEY, fmt::format("{}", indexer.getPatchNodeCount()));

        if (nodeCount > 0) {
            std::vector<int64_t> outFirsts;
            std::vector<int64_t> outCounts;
            std::vector<int64_t> inFirsts;
            std::vector<int64_t> inCounts;
            outFirsts.reserve(nodeCount);
            outCounts.reserve(nodeCount);
            inFirsts.reserve(nodeCount);
            inCounts.reserve(nodeCount);

            for (const NodeEdgeData& data : nodeData) {
                outFirsts.push_back(static_cast<int64_t>(data._outRange._first));
                outCounts.push_back(static_cast<int64_t>(data._outRange._count));
                inFirsts.push_back(static_cast<int64_t>(data._inRange._first));
                inCounts.push_back(static_cast<int64_t>(data._inRange._count));
            }

            writer.beginRowGroup(nodeCount);
            writer.writeInt64Column(0, outFirsts);
            writer.writeInt64Column(1, outCounts);
            writer.writeInt64Column(2, inFirsts);
            writer.writeInt64Column(3, inCounts);
        }

        writer.finish();
    }

    // Out / in label-set span tables. _edges supplies the base of each direction's
    // edge array so spans can be stored as (offset, count). _patchNodeOffsets is not
    // written; the loader rebuilds it from the per-node ranges and the edges, as the
    // binary EdgeIndexerLoader does.
    writeSpansFile(indexer.getOutsByLabelSet(), indexer._edges->getOuts(), outSpansPath);
    writeSpansFile(indexer.getInsByLabelSet(), indexer._edges->getIns(), inSpansPath);
}

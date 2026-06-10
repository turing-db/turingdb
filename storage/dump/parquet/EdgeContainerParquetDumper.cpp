#include "EdgeContainerParquetDumper.h"

#include <stddef.h>
#include <stdint.h>

#include <algorithm>
#include <span>
#include <string_view>
#include <vector>

#include <spdlog/fmt/fmt.h>

#include "ParquetWriteSchema.h"
#include "ParquetWriter.h"

#include "EdgeContainerParquetLayout.h"

#include "datapart/EdgeContainer.h"
#include "datapart/EdgeRecord.h"
#include "Path.h"

using namespace db;

namespace layout = edgeContainerParquetLayout;

namespace {

// Edges are written in bounded row groups: materialising all four id columns for the whole
// container at once costs 32 bytes per edge (tens of gigabytes on large graphs), all of it
// transient on top of the already-resident graph. One row group's worth at a time keeps the
// dumper's own footprint flat regardless of edge count.
constexpr size_t edgeScratchBudgetBytes = 512ull * 1024 * 1024;

void writeEdgeFile(std::span<const EdgeRecord> records,
                   const fs::Path& path,
                   uint64_t firstEdgeID,
                   uint64_t firstNodeID) {
    ParquetWriteSchema schema;
    schema.addColumn(layout::EDGE_ID_COLUMN, ParquetColumnType::UInt64);
    schema.addColumn(layout::NODE_ID_COLUMN, ParquetColumnType::UInt64);
    schema.addColumn(layout::OTHER_ID_COLUMN, ParquetColumnType::UInt64);
    schema.addColumn(layout::EDGE_TYPE_ID_COLUMN, ParquetColumnType::UInt64);

    ParquetWriter writer(path, schema);
    writer.setMetadata(layout::FIRST_EDGE_ID_KEY, fmt::format("{}", firstEdgeID));
    writer.setMetadata(layout::FIRST_NODE_ID_KEY, fmt::format("{}", firstNodeID));

    const size_t count = records.size();

    // Four int64 columns, so 32 bytes per edge across the scratch buffers.
    const size_t rowsPerGroup = std::max<size_t>(1, edgeScratchBudgetBytes / (4 * sizeof(int64_t)));
    const size_t reserveRows = std::min(count, rowsPerGroup);

    std::vector<int64_t> edgeIds;
    std::vector<int64_t> nodeIds;
    std::vector<int64_t> otherIds;
    std::vector<int64_t> edgeTypeIds;
    edgeIds.reserve(reserveRows);
    nodeIds.reserve(reserveRows);
    otherIds.reserve(reserveRows);
    edgeTypeIds.reserve(reserveRows);

    for (size_t firstRow = 0; firstRow < count; firstRow += rowsPerGroup) {
        const size_t groupRows = std::min(rowsPerGroup, count - firstRow);

        edgeIds.clear();
        nodeIds.clear();
        otherIds.clear();
        edgeTypeIds.clear();

        for (size_t i = 0; i < groupRows; ++i) {
            const EdgeRecord& record = records[firstRow + i];
            edgeIds.push_back(static_cast<int64_t>(record._edgeID.getValue()));
            nodeIds.push_back(static_cast<int64_t>(record._nodeID.getValue()));
            otherIds.push_back(static_cast<int64_t>(record._otherID.getValue()));
            edgeTypeIds.push_back(static_cast<int64_t>(record._edgeTypeID.getValue()));
        }

        writer.beginRowGroup(groupRows);
        writer.writeInt64Column(0, edgeIds);
        writer.writeInt64Column(1, nodeIds);
        writer.writeInt64Column(2, otherIds);
        writer.writeInt64Column(3, edgeTypeIds);
    }

    writer.finish();
}

}

void EdgeContainerParquetDumper::dump(const EdgeContainer& edges,
                                      const fs::Path& outPath,
                                      const fs::Path& inPath) {
    const uint64_t firstEdgeID = edges.getFirstEdgeID().getValue();
    const uint64_t firstNodeID = edges.getFirstNodeID().getValue();

    writeEdgeFile(edges.getOuts(), outPath, firstEdgeID, firstNodeID);
    writeEdgeFile(edges.getIns(), inPath, firstEdgeID, firstNodeID);
}

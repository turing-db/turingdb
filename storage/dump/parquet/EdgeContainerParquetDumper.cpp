#include "EdgeContainerParquetDumper.h"

#include <stddef.h>
#include <stdint.h>

#include <span>
#include <string_view>
#include <vector>

#include <spdlog/fmt/fmt.h>

#include "ParquetWriteSchema.h"
#include "ParquetWriter.h"

#include "datapart/EdgeContainer.h"
#include "datapart/EdgeRecord.h"
#include "Path.h"

using namespace db;

namespace {

constexpr std::string_view EDGE_ID_COLUMN = "edge_id";
constexpr std::string_view NODE_ID_COLUMN = "node_id";
constexpr std::string_view OTHER_ID_COLUMN = "other_id";
constexpr std::string_view EDGE_TYPE_ID_COLUMN = "edge_type_id";
constexpr std::string_view FIRST_EDGE_ID_KEY = "turing.first_edge_id";
constexpr std::string_view FIRST_NODE_ID_KEY = "turing.first_node_id";

void writeEdgeFile(std::span<const EdgeRecord> records,
                   const fs::Path& path,
                   uint64_t firstEdgeID,
                   uint64_t firstNodeID) {
    ParquetWriteSchema schema;
    schema.addColumn(EDGE_ID_COLUMN, ParquetColumnType::UInt64);
    schema.addColumn(NODE_ID_COLUMN, ParquetColumnType::UInt64);
    schema.addColumn(OTHER_ID_COLUMN, ParquetColumnType::UInt64);
    schema.addColumn(EDGE_TYPE_ID_COLUMN, ParquetColumnType::UInt64);

    ParquetWriter writer(path, schema);
    writer.setMetadata(FIRST_EDGE_ID_KEY, fmt::format("{}", firstEdgeID));
    writer.setMetadata(FIRST_NODE_ID_KEY, fmt::format("{}", firstNodeID));

    const size_t count = records.size();
    if (count > 0) {
        std::vector<int64_t> edgeIds;
        std::vector<int64_t> nodeIds;
        std::vector<int64_t> otherIds;
        std::vector<int64_t> edgeTypeIds;
        edgeIds.reserve(count);
        nodeIds.reserve(count);
        otherIds.reserve(count);
        edgeTypeIds.reserve(count);

        for (const EdgeRecord& record : records) {
            edgeIds.push_back(static_cast<int64_t>(record._edgeID.getValue()));
            nodeIds.push_back(static_cast<int64_t>(record._nodeID.getValue()));
            otherIds.push_back(static_cast<int64_t>(record._otherID.getValue()));
            edgeTypeIds.push_back(static_cast<int64_t>(record._edgeTypeID.getValue()));
        }

        writer.beginRowGroup(count);
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

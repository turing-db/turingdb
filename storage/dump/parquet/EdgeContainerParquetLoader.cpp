#include "EdgeContainerParquetLoader.h"

#include <stddef.h>
#include <stdint.h>

#include <memory>
#include <span>
#include <string>
#include <string_view>
#include <vector>

#include <arrow/util/key_value_metadata.h>
#include <parquet/metadata.h>

#include "ParquetReader.h"

#include "datapart/EdgeContainer.h"
#include "datapart/EdgeRecord.h"
#include "Path.h"

#include "FatalException.h"

using namespace db;

namespace {

constexpr std::string_view FIRST_EDGE_ID_KEY = "turing.first_edge_id";
constexpr std::string_view FIRST_NODE_ID_KEY = "turing.first_node_id";

// One edge file: four INT64 columns (edge_id, node_id, other_id, edge_type_id),
// plus the first edge/node ids in metadata.
class EdgeColumnsVisitor : public ParquetSaxVisitor {
public:
    std::vector<int64_t> _edgeIds;
    std::vector<int64_t> _nodeIds;
    std::vector<int64_t> _otherIds;
    std::vector<int64_t> _edgeTypeIds;
    uint64_t _firstEdgeID {0};
    uint64_t _firstNodeID {0};
    bool _hasFirstEdgeID {false};
    bool _hasFirstNodeID {false};

    bool onFileStart(const parquet::FileMetaData& metadata) override {
        const auto& keyValueMetadata = metadata.key_value_metadata();
        if (keyValueMetadata) {
            for (int64_t i = 0; i < keyValueMetadata->size(); ++i) {
                const std::string& key = keyValueMetadata->key(i);
                if (key == FIRST_EDGE_ID_KEY) {
                    _firstEdgeID = static_cast<uint64_t>(std::stoull(keyValueMetadata->value(i)));
                    _hasFirstEdgeID = true;
                } else if (key == FIRST_NODE_ID_KEY) {
                    _firstNodeID = static_cast<uint64_t>(std::stoull(keyValueMetadata->value(i)));
                    _hasFirstNodeID = true;
                }
            }
        }
        return true;
    }

    bool onInt64Values(size_t columnIndex, std::span<const int64_t> values) override {
        std::vector<int64_t>& target = columnFor(columnIndex);
        for (const int64_t value : values) {
            target.push_back(value);
        }
        return true;
    }

private:
    std::vector<int64_t>& columnFor(size_t columnIndex) {
        if (columnIndex == 0) {
            return _edgeIds;
        } else if (columnIndex == 1) {
            return _nodeIds;
        } else if (columnIndex == 2) {
            return _otherIds;
        } else {
            return _edgeTypeIds;
        }
    }
};

void readEdgeFile(const fs::Path& path, EdgeColumnsVisitor& visitor) {
    ParquetReader reader(path, visitor);
    while (reader.nextChunk()) {
    }
}

void buildRecords(const EdgeColumnsVisitor& visitor, std::vector<EdgeRecord>& out) {
    const size_t count = visitor._edgeIds.size();
    out.resize(count);
    for (size_t i = 0; i < count; ++i) {
        out[i]._edgeID = EdgeID {static_cast<uint64_t>(visitor._edgeIds[i])};
        out[i]._nodeID = NodeID {static_cast<uint64_t>(visitor._nodeIds[i])};
        out[i]._otherID = NodeID {static_cast<uint64_t>(visitor._otherIds[i])};
        out[i]._edgeTypeID = EdgeTypeID {static_cast<uint64_t>(visitor._edgeTypeIds[i])};
    }
}

}

std::unique_ptr<EdgeContainer> EdgeContainerParquetLoader::load(const fs::Path& outPath,
                                                               const fs::Path& inPath) {
    EdgeColumnsVisitor outVisitor;
    readEdgeFile(outPath, outVisitor);

    EdgeColumnsVisitor inVisitor;
    readEdgeFile(inPath, inVisitor);

    if (!outVisitor._hasFirstEdgeID || !outVisitor._hasFirstNodeID) {
        throw FatalException("EdgeContainerParquetLoader: missing first-id metadata");
    }

    std::vector<EdgeRecord> outEdges;
    std::vector<EdgeRecord> inEdges;
    buildRecords(outVisitor, outEdges);
    buildRecords(inVisitor, inEdges);

    EdgeContainer* container = new EdgeContainer {
        outVisitor._firstNodeID,
        outVisitor._firstEdgeID,
        std::move(outEdges),
        std::move(inEdges),
    };

    return std::unique_ptr<EdgeContainer>(container);
}

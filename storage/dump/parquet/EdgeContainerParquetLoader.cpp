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
#include "ParquetWriteSchema.h"

#include "EdgeContainerParquetLayout.h"
#include "ParquetMetadataParsing.h"

#include "datapart/EdgeContainer.h"
#include "datapart/EdgeRecord.h"
#include "Path.h"

#include "FatalException.h"

using namespace db;

namespace layout = edgeContainerParquetLayout;

namespace {

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
                if (key == layout::FIRST_EDGE_ID_KEY) {
                    _firstEdgeID = parseMetadataUint64(key, keyValueMetadata->value(i));
                    _hasFirstEdgeID = true;
                } else if (key == layout::FIRST_NODE_ID_KEY) {
                    _firstNodeID = parseMetadataUint64(key, keyValueMetadata->value(i));
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
        } else if (columnIndex == 3) {
            return _edgeTypeIds;
        } else {
            throw FatalException("EdgeContainerParquetLoader: unexpected edge column");
        }
    }
};

void readEdgeFile(const fs::Path& path, EdgeColumnsVisitor& visitor) {
    ParquetWriteSchema expectedSchema;
    expectedSchema.addColumn(layout::EDGE_ID_COLUMN, ParquetColumnType::UInt64);
    expectedSchema.addColumn(layout::NODE_ID_COLUMN, ParquetColumnType::UInt64);
    expectedSchema.addColumn(layout::OTHER_ID_COLUMN, ParquetColumnType::UInt64);
    expectedSchema.addColumn(layout::EDGE_TYPE_ID_COLUMN, ParquetColumnType::UInt64);

    ParquetReader reader(path, visitor);
    reader.setExpectedSchema(expectedSchema);
    while (reader.nextChunk()) {
    }
}

void buildRecords(const EdgeColumnsVisitor& visitor, std::vector<EdgeRecord>& out) {
    const size_t count = visitor._edgeIds.size();

    const bool columnsAgree = visitor._nodeIds.size() == count
                              && visitor._otherIds.size() == count
                              && visitor._edgeTypeIds.size() == count;
    if (!columnsAgree) {
        throw FatalException("EdgeContainerParquetLoader: edge columns have mismatched lengths");
    }

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

    const bool outHasFirstIds = outVisitor._hasFirstEdgeID && outVisitor._hasFirstNodeID;
    const bool inHasFirstIds = inVisitor._hasFirstEdgeID && inVisitor._hasFirstNodeID;
    if (!outHasFirstIds || !inHasFirstIds) {
        throw FatalException("EdgeContainerParquetLoader: missing first-id metadata");
    }

    // The dumper writes the same first ids to both files; a disagreement means one of
    // them belongs to a different container.
    const bool firstIdsAgree = inVisitor._firstEdgeID == outVisitor._firstEdgeID
                               && inVisitor._firstNodeID == outVisitor._firstNodeID;
    if (!firstIdsAgree) {
        throw FatalException("EdgeContainerParquetLoader: out- and in-edge first ids disagree");
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

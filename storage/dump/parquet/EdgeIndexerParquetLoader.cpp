#include "EdgeIndexerParquetLoader.h"

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

#include "indexers/EdgeIndexer.h"
#include "indexers/LabelSetIndexer.h"
#include "datapart/EdgeContainer.h"
#include "datapart/EdgeRecord.h"
#include "datapart/NodeEdgeData.h"
#include "metadata/LabelSetMap.h"
#include "Path.h"

#include "FatalException.h"

using namespace db;

namespace {

constexpr std::string_view FIRST_NODE_ID_KEY = "turing.first_node_id";
constexpr std::string_view FIRST_EDGE_ID_KEY = "turing.first_edge_id";
constexpr std::string_view CORE_NODE_COUNT_KEY = "turing.core_node_count";
constexpr std::string_view PATCH_NODE_COUNT_KEY = "turing.patch_node_count";

// Per-node out/in ranges: four INT64 columns, plus the scalars in metadata.
class NodeDataVisitor : public ParquetSaxVisitor {
public:
    uint64_t _firstNodeID {0};
    uint64_t _firstEdgeID {0};
    uint64_t _coreNodeCount {0};
    uint64_t _patchNodeCount {0};
    std::vector<int64_t> _outFirsts;
    std::vector<int64_t> _outCounts;
    std::vector<int64_t> _inFirsts;
    std::vector<int64_t> _inCounts;

    bool onFileStart(const parquet::FileMetaData& metadata) override {
        const auto& keyValueMetadata = metadata.key_value_metadata();
        bool hasNode = false;
        bool hasEdge = false;
        bool hasCore = false;
        bool hasPatch = false;
        if (keyValueMetadata) {
            for (int64_t i = 0; i < keyValueMetadata->size(); ++i) {
                const std::string& key = keyValueMetadata->key(i);
                const std::string& value = keyValueMetadata->value(i);
                if (key == FIRST_NODE_ID_KEY) {
                    _firstNodeID = static_cast<uint64_t>(std::stoull(value));
                    hasNode = true;
                } else if (key == FIRST_EDGE_ID_KEY) {
                    _firstEdgeID = static_cast<uint64_t>(std::stoull(value));
                    hasEdge = true;
                } else if (key == CORE_NODE_COUNT_KEY) {
                    _coreNodeCount = static_cast<uint64_t>(std::stoull(value));
                    hasCore = true;
                } else if (key == PATCH_NODE_COUNT_KEY) {
                    _patchNodeCount = static_cast<uint64_t>(std::stoull(value));
                    hasPatch = true;
                }
            }
        }

        if (!hasNode || !hasEdge || !hasCore || !hasPatch) {
            throw FatalException("EdgeIndexerParquetLoader: missing nodedata metadata");
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
            return _outFirsts;
        } else if (columnIndex == 1) {
            return _outCounts;
        } else if (columnIndex == 2) {
            return _inFirsts;
        } else {
            return _inCounts;
        }
    }
};

// Label-set spans: three INT64 columns (labelset_id, offset, count).
class SpansVisitor : public ParquetSaxVisitor {
public:
    std::vector<int64_t> _labelsetIds;
    std::vector<int64_t> _offsets;
    std::vector<int64_t> _counts;

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
            return _labelsetIds;
        } else if (columnIndex == 1) {
            return _offsets;
        } else {
            return _counts;
        }
    }
};

template <typename Visitor>
void readFile(const fs::Path& path, Visitor& visitor) {
    ParquetReader reader(path, visitor);
    while (reader.nextChunk()) {
    }
}

LabelSetHandle resolveHandle(const LabelSetMap& labelsets, int64_t labelsetId) {
    const std::optional<LabelSetHandle> handle =
        labelsets.getValue(static_cast<LabelSetID::Type>(labelsetId));
    if (!handle) {
        throw FatalException("EdgeIndexerParquetLoader: unknown labelset id");
    }
    return handle.value();
}

}

std::unique_ptr<EdgeIndexer> EdgeIndexerParquetLoader::load(const fs::Path& nodeDataPath,
                                                           const fs::Path& outSpansPath,
                                                           const fs::Path& inSpansPath,
                                                           const LabelSetMap& labelsets,
                                                           const EdgeContainer& edges) {
    NodeDataVisitor nodeVisitor;
    readFile(nodeDataPath, nodeVisitor);

    SpansVisitor outSpansVisitor;
    readFile(outSpansPath, outSpansVisitor);

    SpansVisitor inSpansVisitor;
    readFile(inSpansPath, inSpansVisitor);

    EdgeIndexer* indexer = new EdgeIndexer {edges};
    indexer->_firstNodeID = NodeID {nodeVisitor._firstNodeID};
    indexer->_firstEdgeID = EdgeID {nodeVisitor._firstEdgeID};

    const size_t nodeCount = nodeVisitor._outFirsts.size();
    indexer->_nodes.resize(nodeCount);
    for (size_t i = 0; i < nodeCount; ++i) {
        NodeEdgeData& data = indexer->_nodes[i];
        data._outRange._first = static_cast<size_t>(nodeVisitor._outFirsts[i]);
        data._outRange._count = static_cast<size_t>(nodeVisitor._outCounts[i]);
        data._inRange._first = static_cast<size_t>(nodeVisitor._inFirsts[i]);
        data._inRange._count = static_cast<size_t>(nodeVisitor._inCounts[i]);
    }

    // Patch nodes are the prefix of _nodes, core nodes the suffix.
    const size_t patchNodeCount = static_cast<size_t>(nodeVisitor._patchNodeCount);
    const size_t coreNodeCount = static_cast<size_t>(nodeVisitor._coreNodeCount);
    indexer->_patchNodes = std::span<NodeEdgeData>(indexer->_nodes.data(), patchNodeCount);
    indexer->_coreNodes = std::span<NodeEdgeData>(indexer->_nodes.data() + patchNodeCount,
                                                  coreNodeCount);

    const std::span<const EdgeRecord> outs = edges.getOuts();
    const std::span<const EdgeRecord> ins = edges.getIns();

    // Rebuild the patch-node offset map by recovering each patch node's id from its first
    // edge, exactly as the binary EdgeIndexerLoader does — nothing is stored for it.
    for (size_t i = 0; i < patchNodeCount; ++i) {
        const NodeEdgeData& data = indexer->_patchNodes[i];

        const bool hasOutEdge = data._outRange._count != 0;
        const bool hasInEdge = data._inRange._count != 0;

        if (!hasOutEdge && !hasInEdge) {
            // A node recorded as a patch must have at least one edge.
            throw FatalException("EdgeIndexerParquetLoader: patch node has no edges");
        }

        const NodeID nodeID = hasOutEdge ? outs[data._outRange._first]._nodeID
                                         : ins[data._inRange._first]._nodeID;
        indexer->_patchNodeOffsets[nodeID] = i;
    }

    for (size_t i = 0; i < outSpansVisitor._labelsetIds.size(); ++i) {
        const LabelSetHandle handle = resolveHandle(labelsets, outSpansVisitor._labelsetIds[i]);
        const size_t offset = static_cast<size_t>(outSpansVisitor._offsets[i]);
        const size_t count = static_cast<size_t>(outSpansVisitor._counts[i]);
        indexer->_outLabelSetSpans[handle].emplace_back(outs.data() + offset, count);
    }

    for (size_t i = 0; i < inSpansVisitor._labelsetIds.size(); ++i) {
        const LabelSetHandle handle = resolveHandle(labelsets, inSpansVisitor._labelsetIds[i]);
        const size_t offset = static_cast<size_t>(inSpansVisitor._offsets[i]);
        const size_t count = static_cast<size_t>(inSpansVisitor._counts[i]);
        indexer->_inLabelSetSpans[handle].emplace_back(ins.data() + offset, count);
    }

    return std::unique_ptr<EdgeIndexer>(indexer);
}

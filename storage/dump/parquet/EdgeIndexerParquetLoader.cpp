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

#include <spdlog/fmt/fmt.h>

#include "ParquetReader.h"
#include "ParquetWriteSchema.h"

#include "EdgeIndexerParquetLayout.h"
#include "ParquetMetadataParsing.h"

#include "indexers/EdgeIndexer.h"
#include "indexers/LabelSetIndexer.h"
#include "datapart/EdgeContainer.h"
#include "datapart/EdgeRecord.h"
#include "datapart/NodeEdgeData.h"
#include "metadata/LabelSetMap.h"
#include "Path.h"

#include "FatalException.h"

using namespace db;

namespace layout = edgeIndexerParquetLayout;

namespace {

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
                if (key == layout::FIRST_NODE_ID_KEY) {
                    _firstNodeID = parseMetadataUint64(key, value);
                    hasNode = true;
                } else if (key == layout::FIRST_EDGE_ID_KEY) {
                    _firstEdgeID = parseMetadataUint64(key, value);
                    hasEdge = true;
                } else if (key == layout::CORE_NODE_COUNT_KEY) {
                    _coreNodeCount = parseMetadataUint64(key, value);
                    hasCore = true;
                } else if (key == layout::PATCH_NODE_COUNT_KEY) {
                    _patchNodeCount = parseMetadataUint64(key, value);
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
        } else if (columnIndex == 3) {
            return _inCounts;
        } else {
            throw FatalException("EdgeIndexerParquetLoader: unexpected nodedata column");
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
        } else if (columnIndex == 2) {
            return _counts;
        } else {
            throw FatalException("EdgeIndexerParquetLoader: unexpected spans column");
        }
    }
};

void nodeDataSchema(ParquetWriteSchema& schema) {
    schema.addColumn(layout::OUT_FIRST_COLUMN, ParquetColumnType::UInt64);
    schema.addColumn(layout::OUT_COUNT_COLUMN, ParquetColumnType::UInt64);
    schema.addColumn(layout::IN_FIRST_COLUMN, ParquetColumnType::UInt64);
    schema.addColumn(layout::IN_COUNT_COLUMN, ParquetColumnType::UInt64);
}

void spansSchema(ParquetWriteSchema& schema) {
    schema.addColumn(layout::LABELSET_ID_COLUMN, ParquetColumnType::UInt64);
    schema.addColumn(layout::SPAN_OFFSET_COLUMN, ParquetColumnType::UInt64);
    schema.addColumn(layout::SPAN_COUNT_COLUMN, ParquetColumnType::UInt64);
}

template <typename Visitor>
void readFile(const fs::Path& path, Visitor& visitor, const ParquetWriteSchema& expectedSchema) {
    ParquetReader reader(path, visitor);
    reader.setExpectedSchema(expectedSchema);
    while (reader.nextChunk()) {
    }
}

// A (first, count) pair read from the file must address edges inside the direction's
// edge array; anything else would read out of bounds when the indexer is used, or form
// an out-of-range span pointer. The int64-to-size_t casts make negative file values
// enormous, so this check rejects them too.
void checkEdgeRange(size_t first, size_t count, size_t edgeCount, std::string_view what) {
    const bool startsInBounds = first <= edgeCount;
    const bool fitsInBounds = startsInBounds && count <= edgeCount - first;
    if (!fitsInBounds) {
        throw FatalException(fmt::format(
            "EdgeIndexerParquetLoader: {} range [{} +{}) lies outside the {} edges",
            what, first, count, edgeCount));
    }
}

void checkSpanColumns(const SpansVisitor& visitor, std::string_view which) {
    const size_t spanCount = visitor._labelsetIds.size();
    const bool columnsAgree = visitor._offsets.size() == spanCount
                              && visitor._counts.size() == spanCount;
    if (!columnsAgree) {
        throw FatalException(fmt::format(
            "EdgeIndexerParquetLoader: {} spans columns have mismatched lengths", which));
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
    ParquetWriteSchema expectedNodeDataSchema;
    nodeDataSchema(expectedNodeDataSchema);

    ParquetWriteSchema expectedSpansSchema;
    spansSchema(expectedSpansSchema);

    NodeDataVisitor nodeVisitor;
    readFile(nodeDataPath, nodeVisitor, expectedNodeDataSchema);

    SpansVisitor outSpansVisitor;
    readFile(outSpansPath, outSpansVisitor, expectedSpansSchema);

    SpansVisitor inSpansVisitor;
    readFile(inSpansPath, inSpansVisitor, expectedSpansSchema);

    const size_t nodeCount = nodeVisitor._outFirsts.size();

    const bool nodeColumnsAgree = nodeVisitor._outCounts.size() == nodeCount
                                  && nodeVisitor._inFirsts.size() == nodeCount
                                  && nodeVisitor._inCounts.size() == nodeCount;
    if (!nodeColumnsAgree) {
        throw FatalException("EdgeIndexerParquetLoader: nodedata columns have mismatched lengths");
    }

    checkSpanColumns(outSpansVisitor, "out");
    checkSpanColumns(inSpansVisitor, "in");

    // Patch nodes are the prefix of _nodes, core nodes the suffix; the two metadata
    // counts must partition the rows exactly or the spans built below would overrun.
    const size_t patchNodeCount = static_cast<size_t>(nodeVisitor._patchNodeCount);
    const size_t coreNodeCount = static_cast<size_t>(nodeVisitor._coreNodeCount);
    const bool countsPartitionRows = patchNodeCount <= nodeCount
                                     && coreNodeCount == nodeCount - patchNodeCount;
    if (!countsPartitionRows) {
        throw FatalException(fmt::format(
            "EdgeIndexerParquetLoader: patch ({}) + core ({}) node counts do not match the {} rows",
            patchNodeCount, coreNodeCount, nodeCount));
    }

    std::unique_ptr<EdgeIndexer> indexer {new EdgeIndexer {edges}};
    indexer->_firstNodeID = NodeID {nodeVisitor._firstNodeID};
    indexer->_firstEdgeID = EdgeID {nodeVisitor._firstEdgeID};

    const std::span<const EdgeRecord> outs = edges.getOuts();
    const std::span<const EdgeRecord> ins = edges.getIns();

    indexer->_nodes.resize(nodeCount);
    for (size_t i = 0; i < nodeCount; ++i) {
        NodeEdgeData& data = indexer->_nodes[i];
        data._outRange._first = static_cast<size_t>(nodeVisitor._outFirsts[i]);
        data._outRange._count = static_cast<size_t>(nodeVisitor._outCounts[i]);
        data._inRange._first = static_cast<size_t>(nodeVisitor._inFirsts[i]);
        data._inRange._count = static_cast<size_t>(nodeVisitor._inCounts[i]);

        // An empty range is never dereferenced, so only non-empty ranges must lie
        // within their direction's edge array.
        if (data._outRange._count != 0) {
            checkEdgeRange(data._outRange._first, data._outRange._count, outs.size(), "node out");
        }
        if (data._inRange._count != 0) {
            checkEdgeRange(data._inRange._first, data._inRange._count, ins.size(), "node in");
        }
    }

    indexer->_patchNodes = std::span<NodeEdgeData>(indexer->_nodes.data(), patchNodeCount);
    indexer->_coreNodes = std::span<NodeEdgeData>(indexer->_nodes.data() + patchNodeCount,
                                                  coreNodeCount);

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

        // Span pointers are formed from the offset even for empty spans, so the
        // whole range must lie within the edge array.
        checkEdgeRange(offset, count, outs.size(), "out span");
        indexer->_outLabelSetSpans[handle].emplace_back(outs.data() + offset, count);
    }

    for (size_t i = 0; i < inSpansVisitor._labelsetIds.size(); ++i) {
        const LabelSetHandle handle = resolveHandle(labelsets, inSpansVisitor._labelsetIds[i]);
        const size_t offset = static_cast<size_t>(inSpansVisitor._offsets[i]);
        const size_t count = static_cast<size_t>(inSpansVisitor._counts[i]);

        checkEdgeRange(offset, count, ins.size(), "in span");
        indexer->_inLabelSetSpans[handle].emplace_back(ins.data() + offset, count);
    }

    return indexer;
}

#include "TuringTest.h"

#include "ParquetWriteSchema.h"
#include "ParquetWriter.h"

#include "dump/parquet/EdgeIndexerParquetDumper.h"
#include "dump/parquet/EdgeIndexerParquetLoader.h"
#include "dump/parquet/EdgeIndexerParquetLayout.h"
#include "indexers/EdgeIndexer.h"
#include "datapart/EdgeContainer.h"
#include "datapart/EdgeRecord.h"
#include "datapart/NodeContainer.h"
#include "datapart/NodeEdgeData.h"
#include "comparators/EdgeIndexerComparator.h"
#include "metadata/LabelSet.h"
#include "metadata/LabelSetHandle.h"
#include "metadata/LabelSetMap.h"

#include <stddef.h>
#include <stdint.h>

#include <memory>
#include <span>
#include <string>
#include <unordered_map>
#include <vector>

#include "ID.h"
#include "Path.h"
#include "FatalException.h"
#include "TuringException.h"

using namespace db;
using namespace turing::test;

namespace {

// Forge a nodedata file with arbitrary (possibly inconsistent) column contents and
// patch/core counts, bypassing the dumper's invariants.
void writeNodeDataFile(const fs::Path& path,
                       const std::vector<int64_t>& outFirsts,
                       const std::vector<int64_t>& outCounts,
                       const std::vector<int64_t>& inFirsts,
                       const std::vector<int64_t>& inCounts,
                       uint64_t coreNodeCount,
                       uint64_t patchNodeCount) {
    namespace layout = edgeIndexerParquetLayout;

    ParquetWriteSchema schema;
    schema.addColumn(layout::OUT_FIRST_COLUMN, ParquetColumnType::UInt64);
    schema.addColumn(layout::OUT_COUNT_COLUMN, ParquetColumnType::UInt64);
    schema.addColumn(layout::IN_FIRST_COLUMN, ParquetColumnType::UInt64);
    schema.addColumn(layout::IN_COUNT_COLUMN, ParquetColumnType::UInt64);

    ParquetWriter writer(path, schema);
    writer.setMetadata(layout::FIRST_NODE_ID_KEY, "0");
    writer.setMetadata(layout::FIRST_EDGE_ID_KEY, "0");
    writer.setMetadata(layout::CORE_NODE_COUNT_KEY, std::to_string(coreNodeCount));
    writer.setMetadata(layout::PATCH_NODE_COUNT_KEY, std::to_string(patchNodeCount));

    if (!outFirsts.empty()) {
        writer.beginRowGroup(outFirsts.size());
        writer.writeInt64Column(0, outFirsts);
        writer.writeInt64Column(1, outCounts);
        writer.writeInt64Column(2, inFirsts);
        writer.writeInt64Column(3, inCounts);
    }

    writer.finish();
}

void writeSpansFile(const fs::Path& path,
                    const std::vector<int64_t>& labelsetIds,
                    const std::vector<int64_t>& offsets,
                    const std::vector<int64_t>& counts) {
    namespace layout = edgeIndexerParquetLayout;

    ParquetWriteSchema schema;
    schema.addColumn(layout::LABELSET_ID_COLUMN, ParquetColumnType::UInt64);
    schema.addColumn(layout::SPAN_OFFSET_COLUMN, ParquetColumnType::UInt64);
    schema.addColumn(layout::SPAN_COUNT_COLUMN, ParquetColumnType::UInt64);

    ParquetWriter writer(path, schema);

    if (!labelsetIds.empty()) {
        writer.beginRowGroup(labelsetIds.size());
        writer.writeInt64Column(0, labelsetIds);
        writer.writeInt64Column(1, offsets);
        writer.writeInt64Column(2, counts);
    }

    writer.finish();
}

}

class EdgeIndexerParquetTest : public TuringTest {
protected:
    void initialize() override {
        _nodeDataPath = fs::Path(_outDir) / "edge-indexer-nodedata.parquet";
        _outSpansPath = fs::Path(_outDir) / "edge-indexer-out-spans.parquet";
        _inSpansPath = fs::Path(_outDir) / "edge-indexer-in-spans.parquet";
    }

    // Minimal two-edge container the forged-file tests load against.
    std::unique_ptr<EdgeContainer> makeEdges() {
        std::vector<EdgeRecord> outEdges = {
            {0, 0, 1, 0},
            {1, 1, 0, 0},
        };
        std::unordered_map<EdgeID, EdgeID> tmpToFinalEdgeIDs;
        return EdgeContainer::create(NodeID {0}, EdgeID {0}, std::move(outEdges), tmpToFinalEdgeIDs);
    }

    fs::Path _nodeDataPath;
    fs::Path _outSpansPath;
    fs::Path _inSpansPath;
};

TEST_F(EdgeIndexerParquetTest, CoreOnlyRoundTrip) {
    LabelSetMap labelsets;
    const LabelSet labelSetA = LabelSet::fromList({LabelID {0}});
    const LabelSet labelSetB = LabelSet::fromList({LabelID {1}});
    const LabelSetHandle handleA = labelsets.getOrCreate(labelSetA);
    const LabelSetHandle handleB = labelsets.getOrCreate(labelSetB);

    // Nodes 0-4: nodes 0-2 are label set A, nodes 3-4 are label set B (grouped,
    // ascending by labelset id), so the edge indexer builds one span per label set.
    const NodeID firstNodeID {0};
    const std::vector<LabelSetHandle> perNode {handleA, handleA, handleA, handleB, handleB};
    const std::unique_ptr<NodeContainer> nodeContainer = NodeContainer::create(firstNodeID, perNode);
    ASSERT_NE(nodeContainer, nullptr);

    std::vector<EdgeRecord> outEdges = {
        {0, 0, 3, 0},
        {1, 1, 4, 0},
        {2, 2, 0, 0},
        {3, 3, 1, 0},
        {4, 4, 2, 0},
    };
    std::unordered_map<EdgeID, EdgeID> tmpToFinalEdgeIDs;
    const EdgeID firstEdgeID {0};
    const std::unique_ptr<EdgeContainer> edges = EdgeContainer::create(firstNodeID,
                                                                       firstEdgeID,
                                                                       std::move(outEdges),
                                                                       tmpToFinalEdgeIDs);
    ASSERT_NE(edges, nullptr);

    const std::unordered_map<NodeID, LabelSetHandle> emptyPatchLabelSets;
    const std::unique_ptr<EdgeIndexer> indexer = EdgeIndexer::create(*edges,
                                                                     *nodeContainer,
                                                                     0,
                                                                     emptyPatchLabelSets,
                                                                     0,
                                                                     0);
    ASSERT_NE(indexer, nullptr);

    const fs::Path base = fs::Path(_outDir);
    const fs::Path nodeDataPath = base / "edge-indexer-nodedata.parquet";
    const fs::Path outSpansPath = base / "edge-indexer-out-spans.parquet";
    const fs::Path inSpansPath = base / "edge-indexer-in-spans.parquet";

    EdgeIndexerParquetDumper::dump(*indexer, nodeDataPath, outSpansPath, inSpansPath);

    const std::unique_ptr<EdgeIndexer> loaded = EdgeIndexerParquetLoader::load(nodeDataPath,
                                                                               outSpansPath,
                                                                               inSpansPath,
                                                                               labelsets,
                                                                               *edges);

    EXPECT_TRUE(EdgeIndexerComparator::same(*indexer, *loaded));
    EXPECT_EQ(loaded->getFirstNodeID().getValue(), firstNodeID.getValue());
    EXPECT_EQ(loaded->getFirstEdgeID().getValue(), firstEdgeID.getValue());
    EXPECT_EQ(loaded->getCoreNodeCount(), indexer->getCoreNodeCount());
    EXPECT_EQ(loaded->getPatchNodeCount(), indexer->getPatchNodeCount());

    // Dump-everything: the comparator only checks the label-set spans, so verify the
    // per-node out/in ranges round-trip too.
    const std::span<const NodeEdgeData> originalNodes = indexer->getNodeData();
    const std::span<const NodeEdgeData> loadedNodes = loaded->getNodeData();
    ASSERT_EQ(originalNodes.size(), loadedNodes.size());
    for (size_t i = 0; i < originalNodes.size(); ++i) {
        EXPECT_EQ(originalNodes[i]._outRange._first, loadedNodes[i]._outRange._first);
        EXPECT_EQ(originalNodes[i]._outRange._count, loadedNodes[i]._outRange._count);
        EXPECT_EQ(originalNodes[i]._inRange._first, loadedNodes[i]._inRange._first);
        EXPECT_EQ(originalNodes[i]._inRange._count, loadedNodes[i]._inRange._count);
    }
}

TEST_F(EdgeIndexerParquetTest, MismatchedPatchCoreCountsThrow) {
    // One row, but the metadata claims 2 core + 1 patch nodes.
    writeNodeDataFile(_nodeDataPath, {0}, {1}, {0}, {0}, 2, 1);
    writeSpansFile(_outSpansPath, {}, {}, {});
    writeSpansFile(_inSpansPath, {}, {}, {});

    const LabelSetMap labelsets;
    const std::unique_ptr<EdgeContainer> edges = makeEdges();
    ASSERT_NE(edges, nullptr);

    EXPECT_THROW(EdgeIndexerParquetLoader::load(_nodeDataPath,
                                                _outSpansPath,
                                                _inSpansPath,
                                                labelsets,
                                                *edges),
                 FatalException);
}

TEST_F(EdgeIndexerParquetTest, NodeRangeOutOfBoundsThrows) {
    // A single core node whose out range [5, +1) exceeds the 2-edge container.
    writeNodeDataFile(_nodeDataPath, {5}, {1}, {0}, {0}, 1, 0);
    writeSpansFile(_outSpansPath, {}, {}, {});
    writeSpansFile(_inSpansPath, {}, {}, {});

    const LabelSetMap labelsets;
    const std::unique_ptr<EdgeContainer> edges = makeEdges();
    ASSERT_NE(edges, nullptr);

    EXPECT_THROW(EdgeIndexerParquetLoader::load(_nodeDataPath,
                                                _outSpansPath,
                                                _inSpansPath,
                                                labelsets,
                                                *edges),
                 FatalException);
}

TEST_F(EdgeIndexerParquetTest, SpanOffsetOutOfBoundsThrows) {
    // A valid node table, but an out span pointing past the 2-edge container.
    writeNodeDataFile(_nodeDataPath, {0}, {1}, {0}, {0}, 1, 0);
    writeSpansFile(_outSpansPath, {0}, {10}, {1});
    writeSpansFile(_inSpansPath, {}, {}, {});

    LabelSetMap labelsets;
    labelsets.getOrCreate(LabelSet::fromList({LabelID {0}}));

    const std::unique_ptr<EdgeContainer> edges = makeEdges();
    ASSERT_NE(edges, nullptr);

    EXPECT_THROW(EdgeIndexerParquetLoader::load(_nodeDataPath,
                                                _outSpansPath,
                                                _inSpansPath,
                                                labelsets,
                                                *edges),
                 FatalException);
}

TEST_F(EdgeIndexerParquetTest, WrongNodeDataSchemaThrows) {
    // A nodedata file missing its fourth column must be rejected by the expected-schema
    // check, not silently mis-routed by column index.
    {
        namespace layout = edgeIndexerParquetLayout;

        ParquetWriteSchema schema;
        schema.addColumn(layout::OUT_FIRST_COLUMN, ParquetColumnType::UInt64);
        schema.addColumn(layout::OUT_COUNT_COLUMN, ParquetColumnType::UInt64);
        schema.addColumn(layout::IN_FIRST_COLUMN, ParquetColumnType::UInt64);

        ParquetWriter writer(_nodeDataPath, schema);
        writer.finish();
    }
    writeSpansFile(_outSpansPath, {}, {}, {});
    writeSpansFile(_inSpansPath, {}, {}, {});

    const LabelSetMap labelsets;
    const std::unique_ptr<EdgeContainer> edges = makeEdges();
    ASSERT_NE(edges, nullptr);

    EXPECT_THROW(EdgeIndexerParquetLoader::load(_nodeDataPath,
                                                _outSpansPath,
                                                _inSpansPath,
                                                labelsets,
                                                *edges),
                 TuringException);
}

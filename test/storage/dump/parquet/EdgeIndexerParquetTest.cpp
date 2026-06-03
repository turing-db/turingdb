#include "TuringTest.h"

#include "dump/parquet/EdgeIndexerParquetDumper.h"
#include "dump/parquet/EdgeIndexerParquetLoader.h"
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

#include <memory>
#include <span>
#include <unordered_map>
#include <vector>

#include "ID.h"
#include "Path.h"

using namespace db;
using namespace turing::test;

class EdgeIndexerParquetTest : public TuringTest {
protected:
    void initialize() override {
    }
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

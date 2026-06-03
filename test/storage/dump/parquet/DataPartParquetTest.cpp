#include "TuringTest.h"

#include "dump/parquet/DataPartParquetDumper.h"
#include "dump/parquet/DataPartParquetLoader.h"
#include "comparators/DataPartComparator.h"
#include "datapart/DataPart.h"
#include "metadata/GraphMetadata.h"
#include "metadata/LabelSetMap.h"
#include "reader/GraphReader.h"
#include "versioning/Transaction.h"
#include "Graph.h"
#include "SimpleGraph.h"
#include "writers/GraphWriter.h"
#include "datapart/EdgeRecord.h"
#include "indexers/EdgeIndexer.h"
#include "metadata/PropertyType.h"

#include <stddef.h>

#include <memory>
#include <span>
#include <string>

#include "JobSystem.h"
#include "Path.h"

using namespace db;
using namespace turing::test;

class DataPartParquetTest : public TuringTest {
public:
    void initialize() override {
        _graphPath = fs::Path {_outDir} / "graph";
        _graph = Graph::create("test", _graphPath);
        SimpleGraph::createSimpleGraph(_graph.get());
    }

protected:
    fs::Path _graphPath;
    std::unique_ptr<Graph> _graph;
};

TEST_F(DataPartParquetTest, RoundTrip) {
    const auto tx = _graph->openTransaction();
    const auto reader = tx.readGraph();
    const auto parts = reader.dataparts();
    const LabelSetMap& labelsets = reader.getMetadata().labelsets();

    ASSERT_FALSE(parts.empty());

    for (size_t i = 0; i < parts.size(); ++i) {
        const DataPart& part = *parts[i];

        const fs::Path partDir = fs::Path {_outDir} / ("part-" + std::to_string(i));
        DataPartParquetDumper::dump(part, partDir);

        const std::unique_ptr<DataPart> loaded = DataPartParquetLoader::load(partDir, labelsets);

        EXPECT_TRUE(DataPartComparator::same(part, *loaded))
            << "DataPart " << i << " did not round-trip";
    }
}

// Builds a two-commit graph where the second commit adds an edge into a node from
// the first commit, so the second DataPart records that node as a patch node. This
// exercises the EdgeIndexer patch path (the patch-prefix/core-suffix split and the
// _patchNodeOffsets rebuild on load) with non-empty data.
class DataPartParquetPatchTest : public TuringTest {
public:
    void initialize() override {
        _graphPath = fs::Path {_outDir} / "patch-graph";
        _graph = Graph::create("patch", _graphPath);

        JobSystem jobSystem;
        jobSystem.init();
        GraphWriter writer(_graph.get(), &jobSystem);

        const NodeID a = writer.addNode({"Person"});
        writer.addNodeProperty<types::String>(a, "name", "A");
        const NodeID b = writer.addNode({"Person"});
        writer.addNodeProperty<types::String>(b, "name", "B");
        writer.addEdge("KNOWS", a, b);
        writer.submit();

        // Second commit: a new node c with an edge into the existing node a, which
        // makes a a patch node in the second DataPart.
        const NodeID c = writer.addNode({"Person"});
        writer.addNodeProperty<types::String>(c, "name", "C");
        writer.addEdge("KNOWS", c, a);
        writer.submit();

        _patchedNode = a;
    }

protected:
    fs::Path _graphPath;
    std::unique_ptr<Graph> _graph;
    NodeID _patchedNode {0};
};

TEST_F(DataPartParquetPatchTest, PatchRoundTrip) {
    const auto tx = _graph->openTransaction();
    const auto reader = tx.readGraph();
    const auto parts = reader.dataparts();
    const LabelSetMap& labelsets = reader.getMetadata().labelsets();

    bool foundPatch = false;
    for (size_t i = 0; i < parts.size(); ++i) {
        const DataPart& part = *parts[i];
        const size_t patchNodeCount = part.edgeIndexer().getPatchNodeCount();

        const fs::Path partDir = fs::Path {_outDir} / ("part-" + std::to_string(i));
        DataPartParquetDumper::dump(part, partDir);
        const std::unique_ptr<DataPart> loaded = DataPartParquetLoader::load(partDir, labelsets);

        EXPECT_TRUE(DataPartComparator::same(part, *loaded));
        EXPECT_EQ(loaded->edgeIndexer().getPatchNodeCount(), patchNodeCount);

        if (patchNodeCount > 0) {
            foundPatch = true;

            // The comparator only checks the label-set spans, so verify the patched
            // node's in-edges directly — this read resolves through _patchNodeOffsets.
            const std::span<const EdgeRecord> originalInEdges =
                part.edgeIndexer().getNodeInEdges(_patchedNode);
            const std::span<const EdgeRecord> loadedInEdges =
                loaded->edgeIndexer().getNodeInEdges(_patchedNode);

            ASSERT_EQ(originalInEdges.size(), loadedInEdges.size());
            EXPECT_GT(originalInEdges.size(), 0u);
            for (size_t e = 0; e < originalInEdges.size(); ++e) {
                EXPECT_EQ(originalInEdges[e]._edgeID.getValue(), loadedInEdges[e]._edgeID.getValue());
                EXPECT_EQ(originalInEdges[e]._nodeID.getValue(), loadedInEdges[e]._nodeID.getValue());
                EXPECT_EQ(originalInEdges[e]._otherID.getValue(), loadedInEdges[e]._otherID.getValue());
            }
        }
    }

    ASSERT_TRUE(foundPatch) << "expected at least one DataPart with patch nodes";
}

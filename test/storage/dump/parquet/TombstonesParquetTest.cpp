#include "TuringTest.h"

#include "dump/parquet/TombstonesParquetDumper.h"
#include "dump/parquet/TombstonesParquetLoader.h"
#include "comparators/TombstoneSetComparator.h"
#include "versioning/Tombstones.h"
#include "versioning/TombstoneSet.h"
#include "versioning/Transaction.h"
#include "views/GraphView.h"
#include "writers/GraphWriter.h"
#include "datapart/EdgeRecord.h"
#include "Graph.h"

#include <stddef.h>

#include <memory>

#include "JobSystem.h"
#include "ID.h"
#include "Path.h"

using namespace db;
using namespace turing::test;

// Builds a two-commit graph, deletes some nodes and an edge in the second commit, and
// round-trips the resulting tombstones. Tombstones have no public mutator, so they are
// produced through the real write path (GraphWriter), as in DataPartParquetPatchTest.
class TombstonesParquetTest : public TuringTest {
public:
    void initialize() override {
        _graphPath = fs::Path {_outDir} / "graph";
        _graph = Graph::create("tombstones", _graphPath);

        JobSystem jobSystem;
        jobSystem.init();
        GraphWriter writer(_graph.get(), &jobSystem);

        const NodeID a = writer.addNode({"Person"});
        const NodeID b = writer.addNode({"Person"});
        const NodeID c = writer.addNode({"Person"});
        const NodeID d = writer.addNode({"Person"});
        writer.addEdge("KNOWS", a, b);
        const EdgeRecord bc = writer.addEdge("KNOWS", b, c);
        writer.addEdge("KNOWS", c, d);
        writer.submit();

        writer.deleteNode(a);
        writer.deleteNode(c);
        writer.deleteEdge(bc._edgeID);
        writer.submit();
    }

protected:
    fs::Path _graphPath;
    std::unique_ptr<Graph> _graph;
};

TEST_F(TombstonesParquetTest, RoundTrip) {
    const auto tx = _graph->openTransaction();
    const GraphView view = tx.viewGraph();
    const Tombstones& original = view.tombstones();

    ASSERT_TRUE(original.hasNodes());
    ASSERT_TRUE(original.hasEdges());

    const fs::Path commitDir = fs::Path(_outDir);
    TombstonesParquetDumper::dump(original, commitDir);

    Tombstones loaded;
    TombstonesParquetLoader::load(commitDir, loaded);
    const Tombstones& loadedView = loaded;

    EXPECT_TRUE(TombstoneSetComparator<NodeID>::same(original.nodeTombstones(), loadedView.nodeTombstones()));
    EXPECT_TRUE(TombstoneSetComparator<EdgeID>::same(original.edgeTombstones(), loadedView.edgeTombstones()));
}

TEST_F(TombstonesParquetTest, EmptyRoundTrip) {
    const Tombstones original;

    const fs::Path commitDir = fs::Path(_outDir);
    TombstonesParquetDumper::dump(original, commitDir);

    Tombstones loaded;
    TombstonesParquetLoader::load(commitDir, loaded);
    const Tombstones& loadedView = loaded;

    EXPECT_TRUE(TombstoneSetComparator<NodeID>::same(original.nodeTombstones(), loadedView.nodeTombstones()));
    EXPECT_TRUE(TombstoneSetComparator<EdgeID>::same(original.edgeTombstones(), loadedView.edgeTombstones()));
    EXPECT_FALSE(loadedView.hasNodes());
    EXPECT_FALSE(loadedView.hasEdges());
}

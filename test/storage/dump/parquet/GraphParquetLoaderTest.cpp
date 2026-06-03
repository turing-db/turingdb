#include "TuringTest.h"

#include "dump/parquet/GraphParquetDumper.h"
#include "dump/parquet/GraphParquetLoader.h"
#include "comparators/GraphComparator.h"
#include "reader/GraphReader.h"
#include "versioning/Transaction.h"
#include "versioning/VersionController.h"
#include "writers/GraphWriter.h"
#include "metadata/PropertyType.h"
#include "Graph.h"
#include "ReactomeSampleGraph.h"
#include "Path.h"

#include "JobSystem.h"

#include <stddef.h>

#include <memory>

using namespace db;
using namespace turing::test;

// Full graph dump/load round-trip over the miniature Reactome sample graph: dump the whole
// graph to a Parquet directory, load it back into a fresh graph, and assert structural
// equality with GraphComparator::same (which deep-compares metadata, the commit chain, and
// every datapart of the head commit).
class GraphParquetLoaderTest : public TuringTest {
public:
    void initialize() override {
        _originalPath = fs::Path {_outDir} / "reactome";
        _dumpPath = fs::Path {_outDir} / "dump";

        _graph = Graph::create("reactome", _originalPath);
        ReactomeSampleGraph::create(_graph.get());
    }

protected:
    fs::Path _originalPath;
    fs::Path _dumpPath;

    std::unique_ptr<Graph> _graph;
};

TEST_F(GraphParquetLoaderTest, ReactomeRoundTrip) {
    GraphParquetDumper::dump(*_graph, _dumpPath);

    auto loadedGraph = Graph::create();
    GraphParquetLoader::load(loadedGraph.get(), _dumpPath);

    ASSERT_TRUE(GraphComparator::same(*_graph, *loadedGraph));
}

TEST_F(GraphParquetLoaderTest, ReactomeRoundTripPreservesDatapartIDs) {
    GraphParquetDumper::dump(*_graph, _dumpPath);

    auto loadedGraph = Graph::create();
    GraphParquetLoader::load(loadedGraph.get(), _dumpPath);

    const auto originalTx = _graph->openTransaction();
    const auto loadedTx = loadedGraph->openTransaction();

    const auto originalParts = originalTx.readGraph().dataparts();
    const auto loadedParts = loadedTx.readGraph().dataparts();

    ASSERT_EQ(originalParts.size(), loadedParts.size());
    ASSERT_FALSE(loadedParts.empty());

    for (size_t i = 0; i < originalParts.size(); ++i) {
        ASSERT_EQ(originalParts[i]->getID().get(), loadedParts[i]->getID().get())
            << "Mismatching datapart ID at index " << i;
    }
}

// A two-commit graph whose second commit both adds and deletes, so the commit log has more
// than one entry (exercising the prev-commit chain and the non-head commit shells) and the
// head commit carries non-empty tombstones. The reactome single-commit case never produces
// either.
class GraphParquetMultiCommitTest : public TuringTest {
public:
    void initialize() override {
        _originalPath = fs::Path {_outDir} / "multi";
        _dumpPath = fs::Path {_outDir} / "dump";

        _graph = Graph::create("multi", _originalPath);

        JobSystem jobSystem;
        jobSystem.init();
        GraphWriter writer(_graph.get(), &jobSystem);

        const NodeID alice = writer.addNode({"Person"});
        writer.addNodeProperty<types::String>(alice, "name", "Alice");
        const NodeID bob = writer.addNode({"Person"});
        writer.addNodeProperty<types::String>(bob, "name", "Bob");
        writer.addEdge("KNOWS", alice, bob);
        writer.submit();

        // Second commit: add a third node with an edge, then delete Bob — leaving a node
        // tombstone in the head commit.
        const NodeID carol = writer.addNode({"Person"});
        writer.addNodeProperty<types::String>(carol, "name", "Carol");
        writer.addEdge("KNOWS", carol, alice);
        writer.deleteNode(bob);
        writer.submit();
    }

protected:
    fs::Path _originalPath;
    fs::Path _dumpPath;

    std::unique_ptr<Graph> _graph;
};

TEST_F(GraphParquetMultiCommitTest, RoundTrip) {
    ASSERT_GT(_graph->getVersionController().getNumCommits(), 1u);

    GraphParquetDumper::dump(*_graph, _dumpPath);

    auto loadedGraph = Graph::create();
    GraphParquetLoader::load(loadedGraph.get(), _dumpPath);

    ASSERT_EQ(loadedGraph->getVersionController().getNumCommits(),
              _graph->getVersionController().getNumCommits());
    ASSERT_TRUE(GraphComparator::same(*_graph, *loadedGraph));
}

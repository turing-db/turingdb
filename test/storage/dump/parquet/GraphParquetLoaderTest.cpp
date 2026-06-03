#include "TuringTest.h"

#include "ParquetWriteSchema.h"
#include "ParquetWriter.h"

#include "dump/parquet/GraphParquetDumper.h"
#include "dump/parquet/GraphParquetLoader.h"
#include "dump/parquet/GraphParquetLayout.h"
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
#include "FatalException.h"
#include "TuringException.h"

#include <stddef.h>
#include <stdint.h>

#include <limits>
#include <memory>
#include <span>
#include <vector>

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
// either. The nodes carry one property of every remaining value type (double, uint64 at its
// boundary, embedding) so the full-graph round trip exercises the DataPart-level property
// wiring for all six types, not just the string/int64/bool ones the sample graphs use.
class GraphParquetMultiCommitTest : public TuringTest {
public:
    void initialize() override {
        _originalPath = fs::Path {_outDir} / "multi";
        _dumpPath = fs::Path {_outDir} / "dump";

        _graph = Graph::create("multi", _originalPath);

        JobSystem jobSystem;
        jobSystem.init();
        GraphWriter writer(_graph.get(), &jobSystem);

        const std::vector<float> aliceVector {0.25f, -1.5f, 3.0f, 0.0f};
        const std::vector<float> carolVector {-0.125f, 2.5f, -4.0f, 1.0f};

        const NodeID alice = writer.addNode({"Person"});
        writer.addNodeProperty<types::String>(alice, "name", "Alice");
        writer.addNodeProperty<types::Double>(alice, "score", 0.5);
        writer.addNodeProperty<types::UInt64>(alice, "stamp", std::numeric_limits<uint64_t>::max());
        writer.addNodeProperty<types::Embedding>(alice, "vec", std::span<const float>(aliceVector));
        const NodeID bob = writer.addNode({"Person"});
        writer.addNodeProperty<types::String>(bob, "name", "Bob");
        writer.addEdge("KNOWS", alice, bob);
        writer.submit();

        // Second commit: add a third node with an edge, then delete Bob — leaving a node
        // tombstone in the head commit.
        const NodeID carol = writer.addNode({"Person"});
        writer.addNodeProperty<types::String>(carol, "name", "Carol");
        writer.addNodeProperty<types::Double>(carol, "score", -2.25);
        writer.addNodeProperty<types::UInt64>(carol, "stamp", 0);
        writer.addNodeProperty<types::Embedding>(carol, "vec", std::span<const float>(carolVector));
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

TEST_F(GraphParquetMultiCommitTest, DumpRefusesExistingDirectory) {
    GraphParquetDumper::dump(*_graph, _dumpPath);

    EXPECT_THROW(GraphParquetDumper::dump(*_graph, _dumpPath), FatalException);
}

TEST_F(GraphParquetMultiCommitTest, DumpReplacesStaleTempDirectory) {
    // A leftover temp directory from a crashed dump must not block (or leak into) a
    // fresh dump.
    const fs::Path staleTempDir = graphParquetLayout::dumpTempDir(_dumpPath);
    ASSERT_TRUE(static_cast<bool>(staleTempDir.mkdir()));

    GraphParquetDumper::dump(*_graph, _dumpPath);

    EXPECT_FALSE(staleTempDir.exists());

    auto loadedGraph = Graph::create();
    GraphParquetLoader::load(loadedGraph.get(), _dumpPath);
    EXPECT_TRUE(GraphComparator::same(*_graph, *loadedGraph));
}

TEST_F(GraphParquetMultiCommitTest, LoadMissingGraphInfoThrows) {
    GraphParquetDumper::dump(*_graph, _dumpPath);
    ASSERT_TRUE(static_cast<bool>(graphParquetLayout::graphInfo(_dumpPath).rm()));

    auto loadedGraph = Graph::create();
    EXPECT_THROW(GraphParquetLoader::load(loadedGraph.get(), _dumpPath), TuringException);
}

TEST_F(GraphParquetMultiCommitTest, LoadMissingFormatVersionThrows) {
    GraphParquetDumper::dump(*_graph, _dumpPath);

    // Rewrite graph-info with the right schema but no format-version metadata.
    {
        ParquetWriteSchema schema;
        schema.addColumn(graphParquetLayout::GRAPH_ID_COLUMN, ParquetColumnType::UInt64);
        schema.addColumn(graphParquetLayout::NAME_COLUMN, ParquetColumnType::String);

        ParquetWriter writer(graphParquetLayout::graphInfo(_dumpPath), schema);

        const std::vector<int64_t> graphId {1};
        const std::vector<std::string_view> name {"multi"};
        writer.beginRowGroup(1);
        writer.writeInt64Column(0, graphId);
        writer.writeStringColumn(1, name);
        writer.finish();
    }

    auto loadedGraph = Graph::create();
    EXPECT_THROW(GraphParquetLoader::load(loadedGraph.get(), _dumpPath), FatalException);
}

TEST_F(GraphParquetMultiCommitTest, LoadUnsupportedFormatVersionThrows) {
    GraphParquetDumper::dump(*_graph, _dumpPath);

    // Rewrite graph-info claiming a future format version.
    {
        ParquetWriteSchema schema;
        schema.addColumn(graphParquetLayout::GRAPH_ID_COLUMN, ParquetColumnType::UInt64);
        schema.addColumn(graphParquetLayout::NAME_COLUMN, ParquetColumnType::String);

        ParquetWriter writer(graphParquetLayout::graphInfo(_dumpPath), schema);
        writer.setMetadata(graphParquetLayout::FORMAT_VERSION_KEY, "999");

        const std::vector<int64_t> graphId {1};
        const std::vector<std::string_view> name {"multi"};
        writer.beginRowGroup(1);
        writer.writeInt64Column(0, graphId);
        writer.writeStringColumn(1, name);
        writer.finish();
    }

    auto loadedGraph = Graph::create();
    EXPECT_THROW(GraphParquetLoader::load(loadedGraph.get(), _dumpPath), FatalException);
}

TEST_F(GraphParquetMultiCommitTest, LoadCommitLogSchemaMismatchThrows) {
    GraphParquetDumper::dump(*_graph, _dumpPath);

    // Rewrite the commit log with a wrongly named column; the loader's expected-schema
    // check must reject it instead of silently reading it positionally.
    {
        ParquetWriteSchema schema;
        schema.addColumn("not_commit_hash", ParquetColumnType::UInt64);

        ParquetWriter writer(graphParquetLayout::commitLog(_dumpPath), schema);
        writer.finish();
    }

    auto loadedGraph = Graph::create();
    EXPECT_THROW(GraphParquetLoader::load(loadedGraph.get(), _dumpPath), TuringException);
}

TEST_F(GraphParquetMultiCommitTest, LoadMissingCommitDirectoryThrows) {
    GraphParquetDumper::dump(*_graph, _dumpPath);

    const uint64_t headHash = _graph->getHeadHash().get();
    ASSERT_TRUE(static_cast<bool>(graphParquetLayout::commitDir(_dumpPath, headHash).rm()));

    auto loadedGraph = Graph::create();
    EXPECT_THROW(GraphParquetLoader::load(loadedGraph.get(), _dumpPath), TuringException);
}

TEST_F(GraphParquetMultiCommitTest, LoadMissingDatapartsThrow) {
    GraphParquetDumper::dump(*_graph, _dumpPath);
    ASSERT_TRUE(static_cast<bool>(graphParquetLayout::dataPartsDir(_dumpPath).rm()));

    auto loadedGraph = Graph::create();
    EXPECT_THROW(GraphParquetLoader::load(loadedGraph.get(), _dumpPath), TuringException);
}

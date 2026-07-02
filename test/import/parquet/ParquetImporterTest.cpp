#include "TuringTest.h"
#include "TuringTestEnv.h"

#include <memory>
#include <string>
#include <string_view>

#include "FileUtils.h"
#include "Graph.h"
#include "JobSystem.h"
#include "Path.h"
#include "SystemManager.h"
#include "TuringDB.h"
#include "comparators/GraphComparator.h"
#include "datapart/EdgeRecord.h"
#include "metadata/PropertyType.h"
#include "reader/GraphReader.h"
#include "versioning/Transaction.h"
#include "writers/GraphWriter.h"

using namespace db;
using namespace turing::test;

// Imports a two-file split-Parquet export carrying node and edge properties of every
// supported type, and checks the result against an equivalent graph built by hand
// with GraphWriter. The fixture column order (nodeInt, nodeDouble, nodeString,
// nodeBool, then the edge columns) fixes the PropertyTypeID assignment, so the
// hand-built graph registers property types in the same order for the metadata to
// line up under GraphComparator.
class ParquetImporterTest : public TuringTest {
protected:
    void initialize() override {
        _env = TuringTestEnv::create(fs::Path{_outDir} / "turing");
        _jobSystem = std::make_unique<JobSystem>();
        _jobSystem->init();
    }

    void terminate() override {
        _jobSystem.reset();
        _env.reset();
    }

    // Lays out a nodes/edges fixture pair inside the data directory as
    // nodes.parquet + edges.parquet and imports the containing directory.
    Graph* importSplit(SystemAccessor& system,
                       std::string_view graphName,
                       std::string_view nodeFixture = "nodes.parquet",
                       std::string_view edgeFixture = "edges.parquet") {
        const std::string importDirName {graphName};
        const FileUtils::Path dataDir {_env->getConfig().getDataDir().get()};
        const FileUtils::Path importDir = dataDir / importDirName;
        FileUtils::createDirectory(importDir);

        const FileUtils::Path testDataDir {PARQUET_TEST_DATA_DIR};
        FileUtils::copy(testDataDir / std::string {nodeFixture}, importDir / "nodes.parquet");
        FileUtils::copy(testDataDir / std::string {edgeFixture}, importDir / "edges.parquet");

        return system.importGraph(fs::Path {importDirName}, graphName);
    }

    size_t countNodes(Graph* graph) {
        const GraphReader reader = graph->openTransaction().readGraph();
        size_t nodeCount = 0;
        bool allPresent = true;
        for (const NodeID nodeID : reader.scanNodes()) {
            allPresent = allPresent && reader.graphHasNode(nodeID);
            ++nodeCount;
        }
        EXPECT_TRUE(allPresent);
        return nodeCount;
    }

    // Reproduces, by hand, the graph the fixtures describe.
    std::unique_ptr<Graph> buildExpectedGraph(std::string_view graphName) {
        std::unique_ptr<Graph> graph =
            Graph::create(std::string {graphName}, fs::Path {_outDir} / "expected");
        GraphWriter writer(graph.get(), _jobSystem.get());

        const PropertyType nodeInt = writer.addPropertyType("nodeInt", ValueType::Int64);
        const PropertyType nodeDouble = writer.addPropertyType("nodeDouble", ValueType::Double);
        const PropertyType nodeString = writer.addPropertyType("nodeString", ValueType::String);
        const PropertyType nodeBool = writer.addPropertyType("nodeBool", ValueType::Bool);
        const PropertyType edgeInt = writer.addPropertyType("edgeInt", ValueType::Int64);
        const PropertyType edgeDouble = writer.addPropertyType("edgeDouble", ValueType::Double);
        const PropertyType edgeString = writer.addPropertyType("edgeString", ValueType::String);
        const PropertyType edgeBool = writer.addPropertyType("edgeBool", ValueType::Bool);

        const NodeID n0 = writer.addNode({"Person"});
        const NodeID n1 = writer.addNode({"Person", "Employee"});
        const NodeID n2 = writer.addNode({"Company"});
        const NodeID n3 = writer.addNode({"Person", "Employee", "Manager"});

        writer.addNodeProperty<types::Int64>(n0, nodeInt, 10);
        writer.addNodeProperty<types::Double>(n0, nodeDouble, 1.5);
        writer.addNodeProperty<types::String>(n0, nodeString, "alice");
        writer.addNodeProperty<types::Bool>(n0, nodeBool, true);

        writer.addNodeProperty<types::Int64>(n1, nodeInt, 20);
        writer.addNodeProperty<types::Double>(n1, nodeDouble, 2.5);
        writer.addNodeProperty<types::String>(n1, nodeString, "bob");
        writer.addNodeProperty<types::Bool>(n1, nodeBool, false);

        writer.addNodeProperty<types::Int64>(n2, nodeInt, 30);
        writer.addNodeProperty<types::Double>(n2, nodeDouble, 3.5);
        writer.addNodeProperty<types::String>(n2, nodeString, "acme");
        writer.addNodeProperty<types::Bool>(n2, nodeBool, true);

        writer.addNodeProperty<types::Int64>(n3, nodeInt, 40);
        writer.addNodeProperty<types::Double>(n3, nodeDouble, 4.5);
        writer.addNodeProperty<types::String>(n3, nodeString, "carol");
        writer.addNodeProperty<types::Bool>(n3, nodeBool, false);

        const EdgeRecord e0 = writer.addEdge("KNOWS", n0, n1);
        const EdgeRecord e1 = writer.addEdge("WORKS_FOR", n1, n2);
        const EdgeRecord e2 = writer.addEdge("KNOWS", n2, n0);

        writer.addEdgeProperty<types::Int64>(e0, edgeInt, 100);
        writer.addEdgeProperty<types::Double>(e0, edgeDouble, 10.5);
        writer.addEdgeProperty<types::String>(e0, edgeString, "e0");
        writer.addEdgeProperty<types::Bool>(e0, edgeBool, true);

        writer.addEdgeProperty<types::Int64>(e1, edgeInt, 200);
        writer.addEdgeProperty<types::Double>(e1, edgeDouble, 20.5);
        writer.addEdgeProperty<types::String>(e1, edgeString, "e1");
        writer.addEdgeProperty<types::Bool>(e1, edgeBool, false);

        writer.addEdgeProperty<types::Int64>(e2, edgeInt, 300);
        writer.addEdgeProperty<types::Double>(e2, edgeDouble, 30.5);
        writer.addEdgeProperty<types::String>(e2, edgeString, "e2");
        writer.addEdgeProperty<types::Bool>(e2, edgeBool, true);

        writer.submit();

        return graph;
    }

    std::unique_ptr<TuringTestEnv> _env;
    std::unique_ptr<JobSystem> _jobSystem;
};

TEST_F(ParquetImporterTest, MatchesHandBuiltGraph) {
    constexpr std::string_view graphName = "typed";

    SystemAccessor system = _env->getSystemManager().accessUnique();
    Graph* imported = importSplit(system, graphName);
    ASSERT_NE(imported, nullptr);

    const std::unique_ptr<Graph> expected = buildExpectedGraph(graphName);
    ASSERT_NE(expected, nullptr);

    ASSERT_TRUE(GraphComparator::same(*imported, *expected));
}

// A flat scalar column (__id) split across many data pages within one row group
// must stay aligned with the repeated __labels column. Every node has exactly one
// label, so this isolates the flat multi-page path (no empty-label involvement).
TEST_F(ParquetImporterTest, HandlesFlatColumnSplitAcrossPages) {
    constexpr std::string_view graphName = "smallpage";
    constexpr size_t expectedNodeCount = 3000;

    SystemAccessor system = _env->getSystemManager().accessUnique();
    Graph* imported =
        importSplit(system, graphName, "smallpage_nodes.parquet", "smallpage_edges.parquet");
    ASSERT_NE(imported, nullptr);

    ASSERT_EQ(countNodes(imported), expectedNodeCount);
}

// A row group larger than the reader's chunk size must also import correctly.
TEST_F(ParquetImporterTest, HandlesRowGroupLargerThanChunk) {
    constexpr std::string_view graphName = "rowgroup";
    constexpr size_t expectedNodeCount = 70000;

    SystemAccessor system = _env->getSystemManager().accessUnique();
    Graph* imported =
        importSplit(system, graphName, "rowgroup_nodes.parquet", "rowgroup_edges.parquet");
    ASSERT_NE(imported, nullptr);

    ASSERT_EQ(countNodes(imported), expectedNodeCount);
}

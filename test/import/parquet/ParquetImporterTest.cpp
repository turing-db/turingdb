#include "TuringTest.h"
#include "TuringTestEnv.h"

#include <algorithm>
#include <memory>
#include <string>
#include <string_view>
#include <vector>

#include <spdlog/fmt/fmt.h>

#include "FileUtils.h"
#include "Graph.h"
#include "JobSystem.h"
#include "Path.h"
#include "SystemManager.h"
#include "TuringDB.h"
#include "TuringException.h"
#include "comparators/GraphComparator.h"
#include "datapart/EdgeRecord.h"
#include "metadata/GraphMetadata.h"
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
        return reader.getNodeCount();
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

    // Imports the given fixture pair and asserts the importer did not fail by tripping
    // an internal assertion (bioassert, which throws a FatalException whose message
    // starts with "Internal Error"). Malformed or unsupported input must surface as a
    // clean, user-facing error (or import successfully) rather than as an internal
    // logic error. Used by the findings that reject bad input via bioassert today.
    void importExpectingNoInternalAssertion(SystemAccessor& system,
                                            std::string_view graphName,
                                            std::string_view nodeFixture,
                                            std::string_view edgeFixture) {
        try {
            importSplit(system, graphName, nodeFixture, edgeFixture);
        } catch (const TuringException& e) {
            const std::string message = e.what();
            const bool trippedInternalAssertion =
                message.find("Internal Error") != std::string::npos;
            EXPECT_FALSE(trippedInternalAssertion)
                << "import tripped an internal assertion instead of handling the input "
                   "cleanly:\n"
                << message;
        }
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

// Properties must import even when __id / __source are REQUIRED (non-nullable)
// columns, which carry no definition levels. The chunk row count comes from
// onChunkEnd, not from a def-level stream. The `age`/`weight` properties are also
// REQUIRED, exercising the "no def levels => all rows present" path.
TEST_F(ParquetImporterTest, ImportsPropertiesWithRequiredIdColumns) {
    constexpr std::string_view graphName = "required";

    SystemAccessor system = _env->getSystemManager().accessUnique();
    Graph* imported =
        importSplit(system, graphName, "required_nodes.parquet", "required_edges.parquet");
    ASSERT_NE(imported, nullptr);

    const GraphReader reader = imported->openTransaction().readGraph();
    const GraphMetadata& metadata = reader.getMetadata();

    const auto ageType = metadata.propTypes().get("age");
    ASSERT_TRUE(ageType.has_value());
    std::vector<int64_t> ages;
    for (const int64_t age : reader.scanNodeProperties<types::Int64>(ageType->_id)) {
        ages.push_back(age);
    }
    std::ranges::sort(ages);
    ASSERT_EQ(ages, (std::vector<int64_t>{10, 20, 30}));

    const auto weightType = metadata.propTypes().get("weight");
    ASSERT_TRUE(weightType.has_value());
    std::vector<double> weights;
    for (const double weight : reader.scanEdgeProperties<types::Double>(weightType->_id)) {
        weights.push_back(weight);
    }
    std::ranges::sort(weights);
    ASSERT_EQ(weights, (std::vector<double>{1.5, 2.5}));
}

// --- Bug-demonstration tests (branch code-review findings 1-5) ---
//
// Each test asserts the CORRECT behavior, so it fails against current main and turns
// green once the finding is fixed. bioassert throws a catchable FatalException (a
// TuringException) rather than aborting, so importGraph propagates these failures to
// the caller here.

// Finding 1: a BYTE_ARRAY (string) property column split across many Parquet data
// pages within one row group. capturePropertyByteArray overwrites its stored span on
// every page, keeping only the last page's values, so on current main the import
// trips the "value index out of range" assertion (or assigns values from the wrong
// page). Correct behavior: every node's string property imports intact.
TEST_F(ParquetImporterTest, ImportsStringPropertySpanningManyPages) {
    constexpr std::string_view graphName = "multipage";
    constexpr size_t expectedNodeCount = 500;

    SystemAccessor system = _env->getSystemManager().accessUnique();
    Graph* imported = nullptr;
    ASSERT_NO_THROW(imported = importSplit(system,
                                           graphName,
                                           "multipage_string_nodes.parquet",
                                           "minimal_edges.parquet"));
    ASSERT_NE(imported, nullptr);
    ASSERT_EQ(countNodes(imported), expectedNodeCount);

    const GraphReader reader = imported->openTransaction().readGraph();
    const GraphMetadata& metadata = reader.getMetadata();

    const auto nameType = metadata.propTypes().get("name");
    ASSERT_TRUE(nameType.has_value());

    std::vector<std::string> names;
    for (const std::string_view name : reader.scanNodeProperties<types::String>(nameType->_id)) {
        names.emplace_back(name);
    }
    std::ranges::sort(names);

    std::vector<std::string> expected;
    for (size_t i = 0; i < expectedNodeCount; ++i) {
        expected.push_back(
            fmt::format("person_number_{:05d}_padded_so_the_value_is_wide_enough", i));
    }
    std::ranges::sort(expected);

    ASSERT_EQ(names, expected);
}

// Finding 2: a node whose __labels list is empty. fillLabels skips the empty entry
// before emplacing the per-node label vector, so _chunkNodeLabels ends up shorter
// than _chunkNodeIds and createNodes trips its "NodeID, Label mismatch" assertion on
// current main. Correct behavior: the empty-label node imports (with no labels).
TEST_F(ParquetImporterTest, ImportsNodeWithEmptyLabelList) {
    constexpr std::string_view graphName = "emptylabels";
    constexpr size_t expectedNodeCount = 3;

    SystemAccessor system = _env->getSystemManager().accessUnique();
    Graph* imported = nullptr;
    ASSERT_NO_THROW(imported = importSplit(system,
                                           graphName,
                                           "empty_labels_nodes.parquet",
                                           "minimal_edges.parquet"));
    ASSERT_NE(imported, nullptr);
    ASSERT_EQ(countNodes(imported), expectedNodeCount);
}

// Finding 3: an edge whose __type is null. fillEdgeTypes never consults the __type
// definition levels, so it delivers fewer EdgeTypeIDs than rows and createEdges trips
// its "Edge, Type mismatch" assertion on current main. A null edge type is invalid
// input and must be surfaced cleanly, not as an internal logic error.
TEST_F(ParquetImporterTest, RejectsNullEdgeTypeCleanly) {
    constexpr std::string_view graphName = "nulltype";

    SystemAccessor system = _env->getSystemManager().accessUnique();
    importExpectingNoInternalAssertion(system,
                                       graphName,
                                       "nulltype_nodes.parquet",
                                       "nulltype_edges.parquet");
}

// Finding 4: a repeated (LIST) property column. discoverPropertyColumn classifies it
// by physical type alone and registers it as a scalar property, so its level stream
// outruns the row count and applyNodeProperties trips "Definition levels, row
// mismatch" on current main. An unsupported LIST property must be rejected cleanly.
TEST_F(ParquetImporterTest, RejectsListPropertyColumnCleanly) {
    constexpr std::string_view graphName = "listprop";

    SystemAccessor system = _env->getSystemManager().accessUnique();
    importExpectingNoInternalAssertion(system,
                                       graphName,
                                       "list_property_nodes.parquet",
                                       "minimal_edges.parquet");
}

// Finding 5: a required column with the wrong physical type (__id as INT32). onFileStart
// enforces the type with a bioassert, so a schema-mismatched file trips an internal
// assertion on current main, whereas a missing column raises a clean TuringException a
// few lines later. Correct behavior: a wrong-typed required column is rejected the same
// clean way.
TEST_F(ParquetImporterTest, RejectsWrongTypedRequiredColumnCleanly) {
    constexpr std::string_view graphName = "wrongtype";

    SystemAccessor system = _env->getSystemManager().accessUnique();
    importExpectingNoInternalAssertion(system,
                                       graphName,
                                       "wrongtype_id_nodes.parquet",
                                       "minimal_edges.parquet");
}

int main(int argc, char** argv) {
    return turing::test::turingTestMain(argc, argv, [] {
        testing::GTEST_FLAG(repeat) = 5;
    });
}

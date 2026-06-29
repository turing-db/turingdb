#include "TuringTest.h"
#include "TuringTestEnv.h"

#include <algorithm>
#include <tuple>

#include "FileUtils.h"
#include "Graph.h"
#include "Path.h"
#include "SystemManager.h"
#include "TuringDB.h"
#include "metadata/GraphMetadata.h"
#include "reader/GraphReader.h"
#include "versioning/Transaction.h"

using namespace db;
using namespace turing::test;

class Neo4jParquetImporterTest : public TuringTest {
protected:
    void initialize() override {
        _env = TuringTestEnv::create(fs::Path{_outDir} / "turing");
    }

    void terminate() override {
        _env.reset();
    }

    std::unique_ptr<TuringTestEnv> _env;
};

TEST_F(Neo4jParquetImporterTest, LoadsNodes) {
    const std::string testFile = "multilabel.parquet";
    constexpr std::string_view graphName = "multilabel";

    FileUtils::copy(FileUtils::Path(PARQUET_TEST_DATA_DIR) / testFile,
                    FileUtils::Path(_env->getConfig().getDataDir().get()) / testFile);

    SystemAccessor system = _env->getSystemManager().accessUnique();
    Graph* graph = system.importGraph(fs::Path(testFile), graphName);
    ASSERT_NE(graph, nullptr);

    const GraphReader reader = graph->openTransaction().readGraph();
    const GraphMetadata& metadata = reader.getMetadata();

    const std::vector<std::vector<std::string>> expected = {
        {"Person"},
        {"Person", "CTO"},
        {"Person", "CEO"},
        {"Cartoon", "Dog", "MainCharacter"},
    };

    size_t nodeIndex = 0;
    std::vector<LabelID> labelIDs;
    for (const NodeID nodeID : reader.scanNodes()) {
        labelIDs.clear();
        reader.getNodeLabelSet(nodeID).decompose(labelIDs);

        std::vector<std::string> labelNames;
        for (const LabelID labelID : labelIDs) {
            const auto name = metadata.labels().getName(labelID);
            ASSERT_TRUE(name.has_value());
            labelNames.emplace_back(*name);
        }

        ASSERT_LT(nodeIndex, expected.size());
        ASSERT_EQ(labelNames, expected[nodeIndex]);
        ++nodeIndex;
    }

    ASSERT_EQ(nodeIndex, expected.size());
}

TEST_F(Neo4jParquetImporterTest, LoadsEdges) {
    const std::string testFile = "multilabel_edges.parquet";
    constexpr std::string_view graphName = "multilabel_edges";

    FileUtils::copy(FileUtils::Path(PARQUET_TEST_DATA_DIR) / testFile,
                    FileUtils::Path(_env->getConfig().getDataDir().get()) / testFile);

    SystemAccessor system = _env->getSystemManager().accessUnique();
    Graph* graph = system.importGraph(fs::Path(testFile), graphName);
    ASSERT_NE(graph, nullptr);

    const GraphReader reader = graph->openTransaction().readGraph();
    const GraphMetadata& metadata = reader.getMetadata();

    std::vector<NodeID> nodeIDs;
    for (const NodeID nodeID : reader.scanNodes()) {
        nodeIDs.push_back(nodeID);
    }
    ASSERT_EQ(nodeIDs.size(), 4u);

    const auto knowsWellOpt = metadata.edgeTypes().get("KNOWS_WELL");
    ASSERT_TRUE(knowsWellOpt.has_value());
    const EdgeTypeID knowsWellType = *knowsWellOpt;

    const auto worksForOpt = metadata.edgeTypes().get("WORKS_FOR");
    ASSERT_TRUE(worksForOpt.has_value());
    const EdgeTypeID worksForType = *worksForOpt;

    using EdgeTuple = std::tuple<NodeID, NodeID, EdgeTypeID>;
    std::vector<EdgeTuple> edges;
    for (const EdgeRecord& edge : reader.scanOutEdges()) {
        edges.emplace_back(edge._nodeID, edge._otherID, edge._edgeTypeID);
    }
    ASSERT_EQ(edges.size(), 3u);

    EXPECT_NE(std::ranges::find(edges, EdgeTuple{nodeIDs[0], nodeIDs[1], worksForType}), edges.end());
    EXPECT_NE(std::ranges::find(edges, EdgeTuple{nodeIDs[1], nodeIDs[2], knowsWellType}), edges.end());
    EXPECT_NE(std::ranges::find(edges, EdgeTuple{nodeIDs[2], nodeIDs[1], knowsWellType}), edges.end());
}

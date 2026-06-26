#include "TuringTest.h"
#include "TuringTestEnv.h"

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

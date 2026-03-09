#include <gtest/gtest.h>

#include "TuringDB.h"
#include "QueryConfig.h"
#include "Graph.h"
#include "SimpleGraph.h"
#include "SystemManager.h"
#include "dataframe/Dataframe.h"
#include "versioning/Transaction.h"
#include "reader/GraphReader.h"
#include "dump/GraphDumper.h"

#include "TuringTestEnv.h"
#include "TuringTest.h"

using namespace turing::test;

class LoadGraphTest : public TuringTest {
public:
    void initialize() override {
        const auto testTuringDir = fs::Path {_outDir} / "turing";
        _env = TuringTestEnv::createSyncedOnDisk(testTuringDir);
        _db = &_env->getDB();

        auto graph = Graph::create();
        SimpleGraph::createSimpleGraph(graph.get());
        const auto graphDir = testTuringDir / "graphs" / "simpledb";
        const auto dumpRes = GraphDumper::dump(*graph, graphDir);
        bioassert(dumpRes, "failed to dump simpledb graph");
    }

protected:
    const std::string _graphName = "simpledb";
    std::unique_ptr<TuringTestEnv> _env;
    TuringDB* _db {nullptr};
    QueryConfig _queryConfig;
    Graph* _graph {nullptr};
};

TEST_F(LoadGraphTest, loadGraph) {
    bool executed = false;

    const auto res = _db->query("LOAD GRAPH simpledb", "default", &_env->getMem(), &_queryConfig, [&](const Dataframe* df) -> void {
        ASSERT_TRUE(df != nullptr);
        ASSERT_EQ(df->cols().size(), 1);
        ASSERT_EQ(df->getLogicalRowCount(), 1);
        const auto& cols = df->cols();
        const auto* colName = cols.at(0)->as<ColumnConst<types::String::Primitive>>();
        ASSERT_EQ(colName->getRaw(), "simpledb");
        executed = true;
    });

    ASSERT_TRUE(res.isOk());
    ASSERT_TRUE(executed);
}

int main(int argc, char** argv) {
    return turing::test::turingTestMain(argc, argv, [] {
        testing::GTEST_FLAG(repeat) = 1;
    });
}

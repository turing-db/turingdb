#include <gtest/gtest.h>

#include <memory>
#include <span>
#include <string>
#include <string_view>
#include <vector>

#include "NLOutputSink.h"
#include "QueryInterpreterV3.h"
#include "QueryStatus.h"

#include "Graph.h"
#include "QueryConfig.h"
#include "SimpleGraph.h"
#include "SystemAccessor.h"
#include "SystemManager.h"
#include "TuringDB.h"
#include "versioning/Change.h"
#include "versioning/ChangeID.h"
#include "versioning/CommitHash.h"

#include "StringRowSink.h"
#include "TuringTest.h"
#include "TuringTestEnv.h"

using namespace db;
using namespace turing::test;

namespace {

class NullSink : public NLOutputSink {
public:
    void appendChunks(std::span<const Column* const> chunks, size_t offset, size_t rowCount) override {}
};

}

class CallYieldedNodeCreateEdgeTest : public TuringTest {
public:
    void initialize() override {
        _env = TuringTestEnv::create(fs::Path {_outDir} / "turing");

        SystemAccessor system = _env->getSystemManager().accessUnique();
        Graph* graph = system.createGraph(_graphName);
        SimpleGraph::createSimpleGraph(graph);

        _interpreter = std::make_unique<QueryInterpreterV3>(&_env->getSystemManager());
    }

protected:
    void runQuery(std::string_view query, NLOutputSink& sink) {
        QueryStatus status;
        _interpreter->execute(status, query, _graphName, CommitHash::head(), ChangeID::head(), &_env->getMem(), &sink);
        ASSERT_TRUE(status.isOk()) << query << ": " << status.getError();
    }

    void runWrite(std::string_view query) {
        ChangeID changeID;
        {
            SystemAccessor system = _env->getSystemManager().accessUnique();
            auto res = system.newChange(_graphName);
            ASSERT_TRUE(res);
            changeID = res.value()->id();
        }

        NullSink sink;
        QueryStatus status;
        _interpreter->execute(status, query, _graphName, CommitHash::head(), changeID, &_env->getMem(), &sink);
        ASSERT_TRUE(status.isOk()) << query << ": " << status.getError();

        QueryCallbacks callbacks;
        callbacks.setOnOutputData([](const Dataframe*) {});

        const QueryState submitState(_graphName, &_env->getMem(), &_queryConfig, &callbacks, CommitHash::head(), changeID);
        const QueryStatus submitStatus = _env->getDB().query("CHANGE SUBMIT", submitState);
        ASSERT_TRUE(submitStatus.isOk()) << submitStatus.getError();
    }

    const std::string _graphName = "simpledb";
    std::unique_ptr<TuringTestEnv> _env;
    std::unique_ptr<QueryInterpreterV3> _interpreter;
    QueryConfig _queryConfig;
};

// CALL db.getNodes([0]) YIELD id AS a CREATE (a)-[:PROBE]->(b:Probe {name: 'X'}): the
// yielded node is the source of the created edge, an existing node rather than one to
// create.
TEST_F(CallYieldedNodeCreateEdgeTest, createsAnEdgeFromAYieldedNode) {
    runWrite("CALL db.getNodes([0]) YIELD id AS a CREATE (a)-[:PROBE]->(b:Probe {name: 'X'})");

    StringRowSink sink;
    runQuery("MATCH (n {name: 'Remy'})-[:PROBE]->(b) RETURN b.name", sink);

    const std::vector<StringRowSink::Row> expected {{"X"}};
    EXPECT_EQ(sink.getRows(), expected);
}

// MATCH (n) CALL gnn.neighbourhoodSample(n, 1, 42) YIELD tgt CREATE (n)-[:SAMPLED]->(tgt):
// the created edge joins a carried node to a yielded one, one edge per sampled row.
TEST_F(CallYieldedNodeCreateEdgeTest, createsAnEdgeBetweenACarriedAndAYieldedNode) {
    runWrite("MATCH (n {name: 'Remy'}) CALL gnn.neighbourhoodSample(n, 1, 42) YIELD tgt CREATE (n)-[:SAMPLED]->(tgt)");

    StringRowSink sink;
    runQuery("MATCH (n {name: 'Remy'})-[:SAMPLED]->(m) RETURN count(m)", sink);

    const std::vector<StringRowSink::Row> expected {{"1"}};
    EXPECT_EQ(sink.getRows(), expected);
}

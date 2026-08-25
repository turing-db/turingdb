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

class CallYieldedEntityDeleteTest : public TuringTest {
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

// CALL db.getEdges([0]) YIELD id AS e DELETE e: the deleted edge is the one the call
// yielded, which no pattern bound - simpledb's 18 edges become 17.
TEST_F(CallYieldedEntityDeleteTest, deletesAYieldedEdge) {
    runWrite("CALL db.getEdges([0]) YIELD id AS e DELETE e");

    StringRowSink sink;
    runQuery("MATCH ()-[e]->() RETURN count(e)", sink);

    const std::vector<StringRowSink::Row> expected {{"17"}};
    EXPECT_EQ(sink.getRows(), expected);
}

// CALL db.getNodes([17]) YIELD id AS a DETACH DELETE a: Doruk, the last of simpledb's 18
// nodes, goes with the edge that hangs off it.
TEST_F(CallYieldedEntityDeleteTest, deletesAYieldedNode) {
    runWrite("CALL db.getNodes([17]) YIELD id AS a DETACH DELETE a");

    StringRowSink sink;
    runQuery("MATCH (n) RETURN count(n)", sink);

    const std::vector<StringRowSink::Row> expected {{"17"}};
    EXPECT_EQ(sink.getRows(), expected);
}

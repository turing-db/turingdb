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

class CallYieldedPropertyTest : public TuringTest {
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

    // Runs a writing query in a change of its own and submits the change, so a query on
    // the head sees what it wrote.
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

// CALL db.getNodes([0, 1]) YIELD id AS a RETURN a.name: the projection reads a property
// of the node the call yielded, without the node ever appearing in a pattern.
TEST_F(CallYieldedPropertyTest, readsAPropertyOfAYieldedNode) {
    StringRowSink sink;
    runQuery("CALL db.getNodes([0, 1]) YIELD id AS a RETURN a.name", sink);

    std::vector<StringRowSink::Row> rows;
    sink.sortedRows(rows);

    const std::vector<StringRowSink::Row> expected {{"Adam"}, {"Remy"}};
    EXPECT_EQ(rows, expected);
}

// The YIELD's WHERE compares a property of the yielded node: Remy and Adam are 32,
// Computers carries no age at all.
TEST_F(CallYieldedPropertyTest, filtersOnAPropertyOfAYieldedNode) {
    StringRowSink sink;
    runQuery("CALL db.getNodes([0, 1, 2]) YIELD id AS a WHERE a.age = 32 RETURN a.name", sink);

    std::vector<StringRowSink::Row> rows;
    sink.sortedRows(rows);

    const std::vector<StringRowSink::Row> expected {{"Adam"}, {"Remy"}};
    EXPECT_EQ(rows, expected);
}

TEST_F(CallYieldedPropertyTest, aggregatesAPropertyOfAYieldedNode) {
    StringRowSink sink;
    runQuery("CALL db.getNodes([0, 1]) YIELD id AS a RETURN sum(a.age)", sink);

    const std::vector<StringRowSink::Row> expected {{"64"}};
    EXPECT_EQ(sink.getRows(), expected);
}

TEST_F(CallYieldedPropertyTest, readsAPropertyOfAYieldedEdge) {
    StringRowSink sink;
    runQuery("CALL db.getEdges([0]) YIELD id AS e RETURN e.name", sink);

    const std::vector<StringRowSink::Row> expected {{"Remy -> Adam"}};
    EXPECT_EQ(sink.getRows(), expected);
}

// A call driven per matched row yields a node the projection reads a property of: the
// one neighbour sampled from Remy is one of the four nodes Remy points at.
TEST_F(CallYieldedPropertyTest, readsAPropertyOfANodeAPerRowCallYielded) {
    StringRowSink sink;
    runQuery("MATCH (n {name: 'Remy'}) CALL gnn.neighbourhoodSample(n, 1, 42) YIELD tgt RETURN tgt.name", sink);

    const std::vector<StringRowSink::Row>& rows = sink.getRows();
    ASSERT_EQ(rows.size(), 1u);

    const std::vector<std::string> neighbours {"Adam", "Computers", "Eighties", "Ghosts"};
    EXPECT_TRUE(std::find(neighbours.begin(), neighbours.end(), rows.front().front()) != neighbours.end());
}

TEST_F(CallYieldedPropertyTest, setsAPropertyOfAYieldedNode) {
    runWrite("CALL db.getNodes([0]) YIELD id AS a SET a.age = 40");

    StringRowSink sink;
    runQuery("MATCH (n {name: 'Remy'}) RETURN n.age", sink);

    const std::vector<StringRowSink::Row> expected {{"40"}};
    EXPECT_EQ(sink.getRows(), expected);
}

#include <gtest/gtest.h>

#include <algorithm>
#include <memory>
#include <string>
#include <string_view>

#include "QueryInterpreterV3.h"
#include "QueryStatus.h"

#include "Graph.h"
#include "QueryConfig.h"
#include "SimpleGraph.h"
#include "SystemAccessor.h"
#include "SystemManager.h"
#include "TuringDB.h"
#include "dataframe/Dataframe.h"
#include "versioning/ChangeID.h"
#include "versioning/CommitHash.h"

#include "IRTestRows.h"
#include "TuringTest.h"
#include "TuringTestEnv.h"

using namespace db;
using namespace turing::test;

// startNode() and endNode() over the simpledb fixture. Its three KNOWS_WELL edges are 0
// (Remy 0 -> Adam 1), 4 (Adam -> Remy) and 7 (Ghosts 6 -> Remy), so the ends a hop reports
// are checked against IDs the graph pins rather than against the pattern that walked them.
class EdgeEndpointsTest : public TuringTest {
protected:
    void initialize() override {
        _env = TuringTestEnv::create(fs::Path {_outDir} / "turing");
        _interpreter = std::make_unique<QueryInterpreterV3>(&_env->getSystemManager());

        SystemAccessor system = _env->getSystemManager().accessUnique();
        Graph* graph = system.createGraph(_graphName);
        SimpleGraph::createSimpleGraph(graph);
    }

    void openChange(ChangeID& changeID) {
        SystemAccessor system = _env->getSystemManager().accessUnique();
        const auto res = system.newChange(_graphName);
        ASSERT_TRUE(res);

        changeID = res.value()->id();
    }

    void submit(const ChangeID& changeID) {
        QueryCallbacks callbacks;
        callbacks.setOnOutputData([](const Dataframe*) {});

        const QueryState submitState(_graphName,
                                     &_env->getMem(),
                                     &_queryConfig,
                                     &callbacks,
                                     CommitHash::head(),
                                     changeID);
        const QueryStatus status = _env->getDB().query("CHANGE SUBMIT", submitState);
        ASSERT_TRUE(status.isOk()) << "CHANGE SUBMIT failed";
    }

    void write(std::string_view query) {
        ChangeID changeID;
        openChange(changeID);

        RowSink sink;
        QueryStatus status;
        _interpreter->execute(status,
                              query,
                              _graphName,
                              CommitHash::head(),
                              changeID,
                              &_env->getMem(),
                              &sink);
        ASSERT_TRUE(status.isOk()) << "query: " << query << "\nerror: " << status.getError();

        submit(changeID);
    }

    // The rows a writing query reports, read out of the change it wrote them in: the edge
    // it created is still staged there, so its ends are read off the write buffer
    void expectWrittenRows(std::string_view query, const Rows& expected) {
        ChangeID changeID;
        openChange(changeID);

        RowSink sink;
        QueryStatus status;
        _interpreter->execute(status,
                              query,
                              _graphName,
                              CommitHash::head(),
                              changeID,
                              &_env->getMem(),
                              &sink);
        ASSERT_TRUE(status.isOk()) << "query: " << query << "\nerror: " << status.getError();

        Rows actual;
        sink.sortedRows(actual);

        Rows sortedExpected = expected;
        std::sort(sortedExpected.begin(), sortedExpected.end());

        std::string actualText;
        describeRows(actual, actualText);

        EXPECT_EQ(actual, sortedExpected) << "query: " << query << "\ngot:\n" << actualText;

        submit(changeID);
    }

    void expectRows(std::string_view query, const Rows& expected) {
        RowSink sink;
        QueryStatus status;
        _interpreter->execute(status,
                              query,
                              _graphName,
                              CommitHash::head(),
                              ChangeID::head(),
                              &_env->getMem(),
                              &sink);
        ASSERT_TRUE(status.isOk()) << "query: " << query << "\nerror: " << status.getError();

        Rows actual;
        sink.sortedRows(actual);

        Rows sortedExpected = expected;
        std::sort(sortedExpected.begin(), sortedExpected.end());

        std::string actualText;
        describeRows(actual, actualText);

        EXPECT_EQ(actual, sortedExpected) << "query: " << query << "\ngot:\n" << actualText;
    }

    void expectRejected(std::string_view query, std::string_view messagePart) {
        RowSink sink;
        QueryStatus status;
        _interpreter->execute(status,
                              query,
                              _graphName,
                              CommitHash::head(),
                              ChangeID::head(),
                              &_env->getMem(),
                              &sink);

        ASSERT_FALSE(status.isOk()) << "query: " << query << " was accepted";
        EXPECT_NE(status.getError().find(messagePart), std::string::npos)
            << "query: " << query << "\nerror: " << status.getError();
    }

    const std::string _graphName = "simpledb";
    std::unique_ptr<TuringTestEnv> _env;
    std::unique_ptr<QueryInterpreterV3> _interpreter;
    QueryConfig _queryConfig;
};

TEST_F(EdgeEndpointsTest, readsTheEndsOfAnOutgoingHop) {
    expectRows("MATCH (a)-[e:KNOWS_WELL]->(b) RETURN startNode(e), endNode(e)",
               {{"0", "1"}, {"1", "0"}, {"6", "0"}});
}

TEST_F(EdgeEndpointsTest, readsTheEndsOfAnIncomingHop) {
    expectRows("MATCH (a)<-[e:KNOWS_WELL]-(b) RETURN startNode(e), endNode(e)",
               {{"0", "1"}, {"1", "0"}, {"6", "0"}});
}

TEST_F(EdgeEndpointsTest, readsTheStoredEndsOfAnUndirectedHop) {
    expectRows("MATCH (a)-[e:KNOWS_WELL]-(b) RETURN startNode(e), endNode(e)",
               {{"0", "1"}, {"0", "1"}, {"1", "0"}, {"1", "0"}, {"6", "0"}, {"6", "0"}});
}

TEST_F(EdgeEndpointsTest, readsTheEndsOfAnEdgeNoVariableNames) {
    expectRows("MATCH ()-[e:KNOWS_WELL]->() RETURN startNode(e), endNode(e)",
               {{"0", "1"}, {"1", "0"}, {"6", "0"}});
}

TEST_F(EdgeEndpointsTest, namesTheEndsThroughAWith) {
    expectRows("MATCH (a)-[e:KNOWS_WELL]->(b) WITH startNode(e) AS s, endNode(e) AS t "
               "RETURN s.name, t.name",
               {{"Remy", "Adam"}, {"Adam", "Remy"}, {"Ghosts", "Remy"}});
}

TEST_F(EdgeEndpointsTest, readsTheEndsOfEveryEdgeOfANode) {
    expectRows("MATCH (n:Person {name: 'Adam'})-[e]->(m) RETURN endNode(e)",
               {{"0"}, {"4"}, {"5"}});
}

TEST_F(EdgeEndpointsTest, matchesTheEndsAgainstThePatternVariables) {
    expectRows("MATCH (a)-[e:KNOWS_WELL]->(b) WHERE startNode(e) = a AND endNode(e) = b "
               "RETURN count(e)",
               {{"3"}});
}

TEST_F(EdgeEndpointsTest, matchesNoRowWhereTheEndsAreSwapped) {
    expectRows("MATCH (a)-[e:KNOWS_WELL]->(b) WHERE startNode(e) = b RETURN count(e)",
               {{"0"}});
}

TEST_F(EdgeEndpointsTest, readsTheStartOfAnIncomingHopAsTheFarEnd) {
    expectRows("MATCH (a)<-[e:KNOWS_WELL]-(b) WHERE startNode(e) = b AND endNode(e) = a "
               "RETURN count(e)",
               {{"3"}});
}

TEST_F(EdgeEndpointsTest, readsNullOnAnUnmatchedOptionalEdge) {
    expectRows("MATCH (n:Person) OPTIONAL MATCH (n)-[e:KNOWS_WELL]->(m) "
               "RETURN n.name, startNode(e), endNode(e)",
               {{"Remy", "0", "1"},
                {"Adam", "1", "0"},
                {"Maxime", "null", "null"},
                {"Luc", "null", "null"},
                {"Martina", "null", "null"},
                {"Suhas", "null", "null"},
                {"Cyrus", "null", "null"},
                {"Doruk", "null", "null"}});
}

TEST_F(EdgeEndpointsTest, hopsOutOfTheEndNode) {
    expectRows("MATCH (a)-[e:KNOWS_WELL]->(b) WITH endNode(e) AS t "
               "MATCH (t)-[:INTERESTED_IN]->(i) RETURN t.name, i.name",
               {{"Adam", "Bio"},
                {"Adam", "Cooking"},
                {"Remy", "Ghosts"},
                {"Remy", "Computers"},
                {"Remy", "Eighties"},
                {"Remy", "Ghosts"},
                {"Remy", "Computers"},
                {"Remy", "Eighties"}});
}

TEST_F(EdgeEndpointsTest, collectsTheEndsOfEveryEdgeOfANode) {
    expectRows("MATCH (a:Person {name: 'Remy'})-[e]->(b) RETURN count(startNode(e))",
               {{"4"}});
}

TEST_F(EdgeEndpointsTest, readsTheEndsOfACommittedEdge) {
    write("CREATE (x:Endpoint {name: 'x'})-[r:LINKS]->(y:Endpoint {name: 'y'})");

    expectRows("MATCH (:Endpoint)-[r:LINKS]->(:Endpoint) "
               "WITH startNode(r) AS s, endNode(r) AS t RETURN s.name, t.name",
               {{"x", "y"}});
}

TEST_F(EdgeEndpointsTest, readsTheEndsOfAnEdgeTheQueryJustWrote) {
    expectWrittenRows("CREATE (x:Endpoint {name: 'x'})-[r:LINKS]->(y:Endpoint {name: 'y'}) "
                      "RETURN startNode(r) = x, endNode(r) = y",
                      {{"true", "true"}});
}

TEST_F(EdgeEndpointsTest, rejectsANodeArgument) {
    expectRejected("MATCH (n:Person) RETURN startNode(n)",
                   "Invalid arguments for function 'startNode'");
}

TEST_F(EdgeEndpointsTest, rejectsAPropertyArgument) {
    expectRejected("MATCH (n:Person) RETURN endNode(n.name)",
                   "Invalid arguments for function 'endNode'");
}

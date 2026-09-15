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

// id() over the simpledb fixture, whose 18 nodes and 18 edges are numbered 0 to 17. The
// engine names an entity by that ID, so every case here asserts the ID the graph pinned:
// the Person nodes are Remy 0, Adam 1, Maxime 8, Luc 9, Martina 11, Suhas 12, Cyrus 15 and
// Doruk 17, and the three KNOWS_WELL edges are 0, 4 and 7.
class IdFunctionTest : public TuringTest {
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

TEST_F(IdFunctionTest, readsTheIDOfEveryPersonNode) {
    expectRows("MATCH (n:Person) RETURN n.name, id(n)",
               {{"Remy", "0"},
                {"Adam", "1"},
                {"Maxime", "8"},
                {"Luc", "9"},
                {"Martina", "11"},
                {"Suhas", "12"},
                {"Cyrus", "15"},
                {"Doruk", "17"}});
}

TEST_F(IdFunctionTest, answersWhatTheNodeItselfProjects) {
    expectRows("MATCH (n:Person) RETURN n, id(n)",
               {{"0", "0"},
                {"1", "1"},
                {"8", "8"},
                {"9", "9"},
                {"11", "11"},
                {"12", "12"},
                {"15", "15"},
                {"17", "17"}});
}

TEST_F(IdFunctionTest, readsTheIDOfAnEdge) {
    expectRows("MATCH ()-[e:KNOWS_WELL]->() RETURN id(e)", {{"0"}, {"4"}, {"7"}});
}

TEST_F(IdFunctionTest, matchesANodeByItsID) {
    expectRows("MATCH (n:Person) WHERE id(n) = 8 RETURN n.name", {{"Maxime"}});
}

TEST_F(IdFunctionTest, matchesAnEdgeByItsID) {
    expectRows("MATCH (a)-[e]->(b) WHERE id(e) = 7 RETURN a.name, b.name",
               {{"Ghosts", "Remy"}});
}

TEST_F(IdFunctionTest, ordersByTheID) {
    expectRows("MATCH (n:Person) RETURN n.name ORDER BY id(n) DESC LIMIT 3",
               {{"Doruk"}, {"Cyrus"}, {"Suhas"}});
}

TEST_F(IdFunctionTest, countsTheDistinctIDs) {
    expectRows("MATCH (n:Person) RETURN count(DISTINCT id(n))", {{"8"}});
}

TEST_F(IdFunctionTest, collectsTheIDs) {
    expectRows("MATCH (n:Person) RETURN collect(id(n))",
               {{"[0, 1, 8, 9, 11, 12, 15, 17]"}});
}

TEST_F(IdFunctionTest, readsTheIDOfAnEndOfAnEdge) {
    expectRows("MATCH (a)-[e:KNOWS_WELL]->(b) RETURN id(startNode(e)), id(endNode(e))",
               {{"0", "1"}, {"1", "0"}, {"6", "0"}});
}

TEST_F(IdFunctionTest, readsNullOnAnUnmatchedOptionalNode) {
    expectRows("MATCH (n:Person) OPTIONAL MATCH (n)-[:KNOWS_WELL]->(m) RETURN n.name, id(m)",
               {{"Remy", "1"},
                {"Adam", "0"},
                {"Maxime", "null"},
                {"Luc", "null"},
                {"Martina", "null"},
                {"Suhas", "null"},
                {"Cyrus", "null"},
                {"Doruk", "null"}});
}

TEST_F(IdFunctionTest, readsTheIDOfANodeTheQueryJustWrote) {
    expectWrittenRows("CREATE (n:Marker {name: 'm'}) RETURN id(n)", {{"18"}});
}

TEST_F(IdFunctionTest, readsTheIDOfACommittedNode) {
    write("CREATE (n:Marker {name: 'm'})");

    expectRows("MATCH (n:Marker) RETURN id(n)", {{"18"}});
}

TEST_F(IdFunctionTest, aVariableMayStillBeCalledId) {
    expectRows("MATCH (id:Person) WHERE id(id) = 0 RETURN id.name", {{"Remy"}});
}

TEST_F(IdFunctionTest, rejectsAPropertyArgument) {
    expectRejected("MATCH (n:Person) RETURN id(n.name)",
                   "Invalid arguments for function 'id'");
}

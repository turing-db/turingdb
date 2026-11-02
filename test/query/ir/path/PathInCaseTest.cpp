#include <gtest/gtest.h>

#include <algorithm>
#include <memory>
#include <string>
#include <string_view>
#include <vector>

#include "QueryInterpreterV3.h"
#include "QueryStatus.h"

#include "Graph.h"
#include "SimpleGraph.h"
#include "SystemAccessor.h"
#include "SystemManager.h"
#include "versioning/ChangeID.h"
#include "versioning/CommitHash.h"

#include "StringRowSink.h"
#include "TuringTest.h"
#include "TuringTestEnv.h"

using namespace db;
using namespace turing::test;

// A CASE or a coalesce whose value is a path is a path: length() and nodes() read it, and
// a branch not taken is null. KNOWS_WELL leaves Remy for Adam, Adam for Remy and Ghosts for
// Remy.
class PathInCaseTest : public TuringTest {
protected:
    void initialize() override {
        _env = TuringTestEnv::create(fs::Path {_outDir} / "turing");
        _interpreter = std::make_unique<QueryInterpreterV3>(&_env->getSystemManager(), &_env->getMem(), &_env->getCompilerContext());

        SystemAccessor system = _env->getSystemManager().accessUnique();
        Graph* graph = system.createGraph(_graphName);
        SimpleGraph::createSimpleGraph(graph);
    }

    void runQuery(std::string_view query, StringRowSink& sink, QueryStatus& status) {
        _interpreter->execute(status, query, _graphName, CommitHash::head(), ChangeID::head(), &sink);
    }

    void expectRows(std::string_view query, const std::vector<StringRowSink::Row>& expected) {
        StringRowSink sink;
        QueryStatus status;
        runQuery(query, sink, status);
        ASSERT_TRUE(status.isOk()) << query << ": " << status.getError();

        std::vector<StringRowSink::Row> actual;
        sink.sortedRows(actual);

        std::vector<StringRowSink::Row> sortedExpected = expected;
        std::sort(sortedExpected.begin(), sortedExpected.end());

        EXPECT_EQ(actual, sortedExpected) << "query: " << query;
    }

    const std::string _graphName = "simpledb";
    std::unique_ptr<TuringTestEnv> _env;
    std::unique_ptr<QueryInterpreterV3> _interpreter;
};

TEST_F(PathInCaseTest, readsTheLengthOfAPathACaseGives) {
    expectRows("MATCH pt = (a)-[:KNOWS_WELL]->(b) RETURN a.name, length(CASE WHEN a.name = 'Remy' THEN pt END)",
               {{"Remy", "1"}, {"Adam", "null"}, {"Ghosts", "null"}});
}

TEST_F(PathInCaseTest, readsTheNodesOfAPathACaseGives) {
    expectRows("MATCH pt = (a)-[:KNOWS_WELL]->(b) "
               "RETURN a.name, [n IN nodes(CASE WHEN a.name = 'Remy' THEN pt END) | n.name]",
               {{"Remy", "Remy, Adam"}, {"Adam", "null"}, {"Ghosts", "null"}});
}

TEST_F(PathInCaseTest, readsAPathACaseProjectedPastAWith) {
    expectRows("MATCH pt = (a)-[:KNOWS_WELL]->(b) WITH a, CASE WHEN a.name = 'Remy' THEN pt END AS q RETURN a.name, length(q)",
               {{"Remy", "1"}, {"Adam", "null"}, {"Ghosts", "null"}});
}

TEST_F(PathInCaseTest, readsTheLengthOfAWalkACaseGives) {
    expectRows("MATCH p = (:Person {name: 'Remy'})-[:KNOWS_WELL*1..2]->(b) RETURN b.name, length(CASE WHEN b.name = 'Adam' THEN p END)",
               {{"Adam", "1"}, {"Remy", "null"}});
}

TEST_F(PathInCaseTest, readsTheLengthOfAPathACoalesceGives) {
    expectRows("MATCH pt = (a)-[:KNOWS_WELL]->(b) RETURN a.name, length(coalesce(pt, pt))",
               {{"Remy", "1"}, {"Adam", "1"}, {"Ghosts", "1"}});

    expectRows("MATCH pt = (a)-[:KNOWS_WELL]->(b) RETURN a.name, length(coalesce(CASE WHEN a.name = 'Remy' THEN pt END, pt))",
               {{"Remy", "1"}, {"Adam", "1"}, {"Ghosts", "1"}});
}

TEST_F(PathInCaseTest, returnsThePathACaseGives) {
    expectRows("MATCH pt = (a)-[:KNOWS_WELL]->(b) RETURN a.name, CASE WHEN a.name = 'Remy' THEN pt END",
               {{"Remy", "(0), [0], (1)"}, {"Adam", "null"}, {"Ghosts", "null"}});
}

TEST_F(PathInCaseTest, readsTheRelationshipsOfThePathACaseSelects) {
    expectRows("MATCH pt = (a)-[:KNOWS_WELL]->(b) MATCH qt = (b)-[:KNOWS_WELL]->(c) "
               "RETURN a.name, relationships(CASE WHEN a.name = 'Remy' THEN pt ELSE qt END)",
               {{"Remy", "0"}, {"Adam", "0"}, {"Ghosts", "0"}});
}

TEST_F(PathInCaseTest, readsTheLengthOfAShortestPathACaseGives) {
    expectRows("MATCH (n {name: 'Adam'}), (m {name: 'Ghosts'}) "
               "SHORTESTPATH(n, m, duration, d, p) "
               "RETURN length(CASE WHEN d < 100 THEN p END), length(CASE WHEN d > 100 THEN p END)",
               {{"2", "null"}});
}

TEST_F(PathInCaseTest, typesTheCaseAsThePathItSelects) {
    StringRowSink sink;
    QueryStatus status;
    runQuery("EXPLAIN (db) MATCH pt = (a)-[:KNOWS_WELL]->(b) RETURN length(CASE WHEN a.name = 'Remy' THEN pt END)", sink, status);
    ASSERT_TRUE(status.isOk()) << status.getError();

    std::string dump;
    for (const StringRowSink::Row& row : sink.getRows()) {
        if (row.front() == "db") {
            dump = row.back();
        }
    }

    EXPECT_NE(dump.find("-> !db.column<!storage.path>"), std::string::npos) << dump;
}

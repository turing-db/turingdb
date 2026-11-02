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

// WHEN bodies decided by list predicates, folds and exists(), and branches that walk to the
// nodes they import. Remy and Adam are 32 and no other person has an age. KNOWS_WELL leaves
// Remy for Adam, Adam for Remy and Ghosts for Remy.
class ConditionalBodyCompositionTest : public TuringTest {
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

    void expectRowsInOrder(std::string_view query, const std::vector<StringRowSink::Row>& expected) {
        StringRowSink sink;
        QueryStatus status;
        runQuery(query, sink, status);
        ASSERT_TRUE(status.isOk()) << query << ": " << status.getError();

        EXPECT_EQ(sink.getRows(), expected) << "query: " << query;
    }

    const std::string _graphName = "simpledb";
    std::unique_ptr<TuringTestEnv> _env;
    std::unique_ptr<QueryInterpreterV3> _interpreter;
};

TEST_F(ConditionalBodyCompositionTest, ordersAndCutsOnACountWithAnElseBranch) {
    expectRowsInOrder("MATCH (p:Person) "
                      "RETURN p.name, COUNT { "
                      "  WHEN p.age > 30 THEN MATCH (p)-[:INTERESTED_IN]->(i) RETURN i "
                      "  ELSE RETURN 0 AS i "
                      "} AS c "
                      "ORDER BY c DESC, p.name LIMIT 3",
                      {{"Remy", "3"}, {"Adam", "2"}, {"Cyrus", "1"}});
}

TEST_F(ConditionalBodyCompositionTest, filtersOnACountDecidedByExists) {
    expectRows("MATCH (p:Person) "
               "WHERE COUNT { WHEN exists((p)-[:KNOWS_WELL]->()) THEN MATCH (p)-[:INTERESTED_IN]->(i) RETURN i } > 2 "
               "RETURN p.name",
               {{"Remy"}});
}

TEST_F(ConditionalBodyCompositionTest, collectsUnderAWhenOnAConstantAllReduce) {
    expectRows("MATCH (p:Person) "
               "RETURN p.name, COLLECT { "
               "  WHEN allReduce(s = 0, x IN [1, 2] | s + x, s < 10) THEN RETURN 'old' AS k "
               "  ELSE RETURN 'young' AS k "
               "}",
               {{"Remy", "old"},
                {"Adam", "old"},
                {"Maxime", "old"},
                {"Luc", "old"},
                {"Martina", "old"},
                {"Suhas", "old"},
                {"Cyrus", "old"},
                {"Doruk", "old"}});
}

TEST_F(ConditionalBodyCompositionTest, collectsUnderAWhenOnAReduceComparedWithTheRow) {
    expectRows("MATCH (p:Person) "
               "RETURN p.name, COLLECT { "
               "  WHEN reduce(s = 0, x IN [1, 2] | s + x) < p.age THEN RETURN 'old' AS k "
               "  ELSE RETURN 'young' AS k "
               "}",
               {{"Remy", "old"},
                {"Adam", "old"},
                {"Maxime", "young"},
                {"Luc", "young"},
                {"Martina", "young"},
                {"Suhas", "young"},
                {"Cyrus", "young"},
                {"Doruk", "young"}});
}

TEST_F(ConditionalBodyCompositionTest, decidesACaseOnAnAllReduceOverTheRow) {
    expectRows("MATCH (p:Person) "
               "RETURN p.name, CASE WHEN allReduce(s = 0, x IN [1, 2] | s + x, s < p.age) THEN 'old' ELSE 'young' END",
               {{"Remy", "old"},
                {"Adam", "old"},
                {"Maxime", "young"},
                {"Luc", "young"},
                {"Martina", "young"},
                {"Suhas", "young"},
                {"Cyrus", "young"},
                {"Doruk", "young"}});
}

TEST_F(ConditionalBodyCompositionTest, decidesAWhenOnAListPredicateOverAListOfTheImport) {
    expectRows("MATCH (p:Person) CALL (p) { WHEN any(x IN [p.age] WHERE x > 1) THEN RETURN 1 AS k } RETURN p.name, k",
               {{"Remy", "1"}, {"Adam", "1"}});
}

TEST_F(ConditionalBodyCompositionTest, filtersABodyOnAListPredicateOverTheImport) {
    expectRows("MATCH (p:Person) CALL (p) { WITH p WHERE any(x IN [1] WHERE x < p.age) RETURN 1 AS k } RETURN p.name, k",
               {{"Remy", "1"}, {"Adam", "1"}});
}

TEST_F(ConditionalBodyCompositionTest, projectsAComprehensionOverTheImportInABranch) {
    expectRows("MATCH (p:Person) CALL (p) { WHEN p.age > 1 THEN RETURN [x IN [1] | x + p.age] AS k } RETURN p.name, k",
               {{"Remy", "33"}, {"Adam", "33"}});
}

TEST_F(ConditionalBodyCompositionTest, countsABodyFilteredByAListPredicateOverTheRow) {
    expectRows("MATCH (p:Person) "
               "RETURN p.name, COUNT { MATCH (p)-[:INTERESTED_IN]->(i) WHERE any(x IN [1] WHERE x < p.age) RETURN i LIMIT 5 }",
               {{"Remy", "3"},
                {"Adam", "2"},
                {"Maxime", "0"},
                {"Luc", "0"},
                {"Martina", "0"},
                {"Suhas", "0"},
                {"Cyrus", "0"},
                {"Doruk", "0"}});
}

TEST_F(ConditionalBodyCompositionTest, decidesAWhenOnAListPredicateOverAnIntegerImport) {
    expectRows("WITH 5 AS a CALL (a) { WHEN any(x IN [1] WHERE x < a) THEN RETURN 1 AS k } RETURN k",
               {{"1"}});
    expectRows("UNWIND [5, 0] AS a CALL (a) { WHEN any(x IN [1] WHERE x < a) THEN RETURN 1 AS k } RETURN a, k",
               {{"5", "1"}});
    expectRows("UNWIND [5, 0] AS a RETURN a, COUNT { WHEN any(x IN [1] WHERE x < a) THEN RETURN 1 AS k }",
               {{"5", "1"}, {"0", "0"}});
}

TEST_F(ConditionalBodyCompositionTest, walksBetweenTwoImportsInABranch) {
    expectRows("MATCH (a:Person), (b:Person) "
               "WHERE a.name IN ['Remy', 'Adam'] AND b.name IN ['Remy', 'Adam'] "
               "CALL (a, b) { WHEN a.age > 1 THEN MATCH (a)-[:KNOWS_WELL*1..2]->(b) RETURN count(*) AS n } "
               "RETURN a.name, b.name, n",
               {{"Remy", "Remy", "1"}, {"Remy", "Adam", "1"}, {"Adam", "Remy", "1"}, {"Adam", "Adam", "1"}});
}

TEST_F(ConditionalBodyCompositionTest, countsAHopToABoundNodeInABranch) {
    expectRows("MATCH (a:Person), (b:Person) "
               "WHERE a.name IN ['Remy', 'Adam'] AND b.name IN ['Remy', 'Adam'] "
               "RETURN a.name, b.name, COUNT { WHEN a.age > 1 THEN MATCH (a)-[:KNOWS_WELL]->(b) RETURN 1 AS x }",
               {{"Remy", "Remy", "0"}, {"Remy", "Adam", "1"}, {"Adam", "Remy", "1"}, {"Adam", "Adam", "0"}});
}

TEST_F(ConditionalBodyCompositionTest, findsATwoHopWalkToABoundNodeInABranch) {
    expectRows("MATCH (a:Person), (b:Person) "
               "WHERE a.name IN ['Remy', 'Adam'] AND b.name IN ['Remy', 'Adam'] "
               "RETURN a.name, b.name, EXISTS { "
               "  WHEN a.age > 1 THEN MATCH (a)-[:KNOWS_WELL]->()-[:KNOWS_WELL]->(b) RETURN 1 AS x "
               "}",
               {{"Remy", "Remy", "true"},
                {"Remy", "Adam", "false"},
                {"Adam", "Remy", "false"},
                {"Adam", "Adam", "true"}});
}

TEST_F(ConditionalBodyCompositionTest, collectsAWalkToABoundNodeInEachBranch) {
    expectRows("MATCH (a:Person {name: 'Remy'}), (b:Interest) "
               "RETURN b.name, COLLECT { "
               "  WHEN b.isReal THEN MATCH (a)-[:INTERESTED_IN]->(b) RETURN b.name AS x "
               "  ELSE MATCH (a)-[:KNOWS_WELL]->()-[:INTERESTED_IN]->(b) RETURN 'via ' + b.name AS x "
               "}",
               {{"Animals", ""},
                {"Bio", "via Bio"},
                {"Computers", "Computers"},
                {"Cooking", "via Cooking"},
                {"Eighties", ""},
                {"Ghosts", "Ghosts"},
                {"Gym", ""},
                {"JiuJitsu", ""},
                {"Padel", ""},
                {"Travel", ""}});
}

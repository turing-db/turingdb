#include <gtest/gtest.h>

#include <algorithm>
#include <memory>
#include <string>
#include <string_view>

#include "NLOutputSink.h"
#include "QueryInterpreterV3.h"
#include "QueryStatus.h"

#include "Graph.h"
#include "SimpleGraph.h"
#include "SystemAccessor.h"
#include "SystemManager.h"
#include "versioning/ChangeID.h"
#include "versioning/CommitHash.h"

#include "IRTestRows.h"
#include "TuringTest.h"
#include "TuringTestEnv.h"

using namespace db;
using namespace turing::test;

// Long compositions of everything a read query can drive rows with at once: several
// MATCHes, a barrier carrying a cut, an UNWIND, a procedure or two, and two or three
// levels of aggregation over the result. Each expectation below is derived from the
// simpledb fixture rather than observed, so a wrong row is a wrong answer and not a
// changed one.
//
// The counts the derivations rest on, pinned by the first test: the graph holds
// 18 nodes, 9 labels and 2 edge types. Fifteen Person -[:INTERESTED_IN]-> Interest rows
// reach ten interests, with Gym drawing three people, Bio / Computers / Cooking two each,
// and the remaining six one each. Out of every node, Remy has four edges, Adam three,
// Cyrus / Luc / Maxime / Suhas two, Doruk / Martina / Ghosts one.
class ComplexCompositionTest : public TuringTest {
protected:
    void initialize() override {
        _env = TuringTestEnv::create(fs::Path {_outDir} / "turing");
        _interpreter = std::make_unique<QueryInterpreterV3>(&_env->getSystemManager());

        SystemAccessor system = _env->getSystemManager().accessUnique();
        Graph* graph = system.createGraph(_graphName);
        SimpleGraph::createSimpleGraph(graph);
    }

    QueryStatus runQuery(std::string_view query, NLOutputSink* sink) {
        QueryStatus status;
        _interpreter->execute(status,
                              query,
                              _graphName,
                              CommitHash::head(),
                              ChangeID::head(),
                              &_env->getMem(),
                              sink);

        return status;
    }

    // The rows in the order the query emits them, for the compositions ending on an
    // ORDER BY over their groups
    void expectRowsInOrder(std::string_view query, const Rows& expected) {
        RowSink sink;
        const QueryStatus status = runQuery(query, &sink);
        ASSERT_TRUE(status.isOk()) << "query: " << query << "\nerror: " << status.getError();

        std::string actualText;
        describeRows(sink.rows(), actualText);

        EXPECT_EQ(sink.rows(), expected) << "query: " << query << "\ngot:\n" << actualText;
    }

    const std::string _graphName = "simpledb";
    std::unique_ptr<TuringTestEnv> _env;
    std::unique_ptr<QueryInterpreterV3> _interpreter;
};

// Every composition below multiplies its rows by one or both of these, so a change to the
// fixture's schema shows up here rather than as an unexplained arithmetic failure.
TEST_F(ComplexCompositionTest, pinsTheSchemaCountsTheDerivationsRestOn) {
    expectRowsInOrder("CALL db.labels() YIELD label RETURN count(*)", {{"9"}});
    expectRowsInOrder("CALL db.edgeTypes() YIELD edgeType RETURN count(*)", {{"2"}});
    expectRowsInOrder("MATCH (n) RETURN count(*)", {{"18"}});
    expectRowsInOrder("MATCH (p:Person)-[:INTERESTED_IN]->(i:Interest) RETURN count(*)", {{"15"}});
}

// Three MATCHes behind a cut, crossed with two procedures and an UNWIND, reduced three
// times over. Each seed contributes 2 passes * 2 edge types * 9 labels = 36 rows for
// every edge out of every person interested in it, so grandRows is 36 times that fan-out
// and topReached is the widest out-degree among those people.
TEST_F(ComplexCompositionTest, reducesACutTwoHopThroughTwoProceduresAndThreeLevels) {
    expectRowsInOrder("MATCH (p:Person)-[r1:INTERESTED_IN]->(i:Interest) "
                      "WITH p, i, r1.proficiency AS prof ORDER BY p.name, i.name SKIP 1 LIMIT 10 "
                      "MATCH (i)<-[r2:INTERESTED_IN]-(q:Person) "
                      "MATCH (q)-[r3]->(j) "
                      "UNWIND [1, 2] AS pass "
                      "CALL db.edgeTypes() YIELD edgeType "
                      "CALL db.labels() YIELD label "
                      "WITH p.name AS person, i.name AS seed, q.name AS via, pass, edgeType, "
                      "     count(*) AS labelRows, count(DISTINCT label) AS labels, "
                      "     count(DISTINCT j.name) AS reached "
                      "WITH person, seed, pass, count(*) AS viaTypePairs, sum(labelRows) AS allRows, "
                      "     max(reached) AS maxReached, collect(DISTINCT via) AS vias "
                      "WITH person, seed, count(*) AS passes, sum(allRows) AS grandRows, "
                      "     max(maxReached) AS topReached "
                      "ORDER BY person, seed "
                      "RETURN person, seed, passes, grandRows, topReached",
                      {{"Adam", "Cooking", "2", "144", "3"},
                       {"Cyrus", "Gym", "2", "180", "2"},
                       {"Cyrus", "Travel", "2", "72", "2"},
                       {"Doruk", "Gym", "2", "180", "2"},
                       {"Luc", "Animals", "2", "72", "2"},
                       {"Luc", "Computers", "2", "216", "4"},
                       {"Martina", "Cooking", "2", "144", "3"},
                       {"Maxime", "Bio", "2", "180", "3"},
                       {"Maxime", "Padel", "2", "72", "2"},
                       {"Remy", "Computers", "2", "216", "4"}});
}

// The interests reachable from each cut seed through the people who share it. allRows is
// the (via, reached) fan-out times the 2 passes and 2 edge types crossed into it, and
// maxVia is how many people the most-shared reached interest was found through.
TEST_F(ComplexCompositionTest, countsDistinctReachesOverATwoHopFanOut) {
    expectRowsInOrder("MATCH (p:Person)-[r:INTERESTED_IN]->(i:Interest) "
                      "WITH p, i, count(*) AS one ORDER BY p.name, i.name LIMIT 6 "
                      "MATCH (i)<-[:INTERESTED_IN]-(q:Person) "
                      "MATCH (q)-[:INTERESTED_IN]->(j:Interest) "
                      "UNWIND [1, 2] AS pass "
                      "CALL db.edgeTypes() YIELD edgeType "
                      "WITH p.name AS person, i.name AS seed, j.name AS reached, pass, "
                      "     count(*) AS rows, count(DISTINCT q.name) AS via "
                      "WITH person, seed, count(DISTINCT reached) AS reachedCount, "
                      "     sum(rows) AS allRows, max(via) AS maxVia "
                      "ORDER BY person, seed "
                      "RETURN person, seed, reachedCount, allRows, maxVia",
                      {{"Adam", "Bio", "3", "16", "2"},
                       {"Adam", "Cooking", "2", "12", "2"},
                       {"Cyrus", "Gym", "3", "20", "3"},
                       {"Cyrus", "Travel", "2", "8", "1"},
                       {"Doruk", "Gym", "3", "20", "3"},
                       {"Luc", "Animals", "2", "8", "1"}});
}

// Two procedures crossed onto a node pair and reduced away entirely: each of the 9 labels
// pairs with each of the 2 edge types over all 324 node pairs, so the grand total is
// 9 * 2 * 324 and no key survives to the last projection.
TEST_F(ComplexCompositionTest, crossesTwoProceduresThroughThreeAggregationLevels) {
    expectRowsInOrder("MATCH (a) MATCH (b) "
                      "CALL db.labels() YIELD label "
                      "CALL db.edgeTypes() YIELD edgeType "
                      "WITH label, edgeType, count(*) AS pairs "
                      "WITH label, count(*) AS types, sum(pairs) AS total "
                      "WITH count(*) AS labels, sum(total) AS grand, max(types) AS maxTypes "
                      "RETURN labels, grand, maxTypes",
                      {{"9", "5832", "2"}});
}

// The same shape over enough rows to span several chunks: 18^4 node quadruples crossed
// with 9 labels is 944784 rows, grouped down to one row per label and then cut to five of
// them, so the cut and the collect run over groups the aggregation folded across chunk
// boundaries.
TEST_F(ComplexCompositionTest, cutsTheGroupsOfAChunkCrossingProcedureCross) {
    expectRowsInOrder("MATCH (a) MATCH (b) MATCH (c) MATCH (d) "
                      "CALL db.labels() YIELD label "
                      "WITH label, count(*) AS rows "
                      "WITH label, rows ORDER BY label SKIP 2 LIMIT 5 "
                      "WITH collect(label) AS labels, sum(rows) AS total, count(*) AS kept "
                      "RETURN labels, total, kept",
                      {{"[Founder, Interest, Person, Sales, SleepDisturber]", "524880", "5"}});
}

// A cut window of eight interests, each crossed with the people who share it and then
// with every label: labelHits is the fan-out repeated per label, so hits is 9 times it
// and maxHits is the fan-out itself.
TEST_F(ComplexCompositionTest, reducesTheFansOfEachCutInterestOverEveryLabel) {
    expectRowsInOrder("MATCH (p:Person)-[r:INTERESTED_IN]->(i:Interest) "
                      "WITH p, i, r.proficiency AS prof ORDER BY p.name, i.name SKIP 2 LIMIT 8 "
                      "MATCH (i)<-[:INTERESTED_IN]-(other:Person) "
                      "CALL db.labels() YIELD label "
                      "WITH p.name AS person, i.name AS interest, label, "
                      "     count(*) AS labelHits, collect(other.name) AS others "
                      "WITH person, interest, count(*) AS labels, sum(labelHits) AS hits, "
                      "     max(labelHits) AS maxHits "
                      "ORDER BY person, interest "
                      "RETURN person, interest, labels, hits, maxHits",
                      {{"Cyrus", "Gym", "9", "27", "3"},
                       {"Cyrus", "Travel", "9", "9", "1"},
                       {"Doruk", "Gym", "9", "27", "3"},
                       {"Luc", "Animals", "9", "9", "1"},
                       {"Luc", "Computers", "9", "18", "2"},
                       {"Martina", "Cooking", "9", "18", "2"},
                       {"Maxime", "Bio", "9", "18", "2"},
                       {"Maxime", "Padel", "9", "9", "1"}});
}

// Every interest joined to itself through the people who share it, which is one row per
// ordered pair of its fans: the squared fan-outs sum to 27 whichever pass and edge type
// they are crossed with, and Gym's three fans are the widest group.
TEST_F(ComplexCompositionTest, countsDistinctEndpointsOfEverySharedInterest) {
    expectRowsInOrder("MATCH (p:Person)-[:INTERESTED_IN]->(i:Interest) "
                      "MATCH (i)<-[:INTERESTED_IN]-(q:Person) "
                      "UNWIND [1, 2] AS pass "
                      "CALL db.edgeTypes() YIELD edgeType "
                      "WITH pass, edgeType, i.name AS interest, count(*) AS rows, "
                      "     count(DISTINCT p.name) AS ps, count(DISTINCT q.name) AS qs, "
                      "     collect(DISTINCT p.name) AS people "
                      "WITH pass, edgeType, count(*) AS interests, sum(rows) AS totalRows, "
                      "     max(ps) AS maxPs "
                      "ORDER BY pass, edgeType "
                      "RETURN pass, edgeType, interests, totalRows, maxPs",
                      {{"1", "INTERESTED_IN", "10", "27", "3"},
                       {"1", "KNOWS_WELL", "10", "27", "3"},
                       {"2", "INTERESTED_IN", "10", "27", "3"},
                       {"2", "KNOWS_WELL", "10", "27", "3"}});
}

// Two UNWINDs and two procedures stacked with no MATCH under them at all: the rows are
// driven entirely by the literal lists and the procedure results, and every (x, y) pair
// sees each of the 2 edge types carrying all 9 labels.
TEST_F(ComplexCompositionTest, reducesTwoStackedUnwindsAndTwoProcedures) {
    expectRowsInOrder("UNWIND [1, 2] AS x "
                      "CALL db.edgeTypes() YIELD edgeType "
                      "UNWIND [3, 4] AS y "
                      "CALL db.labels() YIELD label "
                      "WITH x, y, edgeType, count(*) AS labels "
                      "WITH x, y, count(*) AS types, sum(labels) AS total "
                      "ORDER BY x, y "
                      "RETURN x, y, types, total",
                      {{"1", "3", "2", "18"},
                       {"1", "4", "2", "18"},
                       {"2", "3", "2", "18"},
                       {"2", "4", "2", "18"}});
}

int main(int argc, char** argv) {
    return turing::test::turingTestMain(argc, argv);
}

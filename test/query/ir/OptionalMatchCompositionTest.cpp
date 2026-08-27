#include <gtest/gtest.h>

#include <stddef.h>

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

namespace {

using Row = StringRowSink::Row;
using Rows = std::vector<Row>;

// simpledb's node IDs are stable and other tests already rely on them: Remy is 0, Adam 1,
// Maxime 7. The CALL cases below name nodes by ID, which is what db.getNodes takes.
constexpr const char* remyID = "0";
constexpr const char* adamID = "1";
constexpr const char* maximeID = "7";

}

// OPTIONAL MATCH composed with the clauses around it - a mandatory MATCH, another OPTIONAL
// MATCH, a WITH barrier, an aggregation, a CALL - driven from Cypher text through the
// analyzer, DBProgramGenerator, NL lowering and the NL interpreter.
//
// Each of these puts the join somewhere the padded rows have to survive a further stage:
// a pattern walked from a variable that may be null, a barrier that republishes it, a
// group that must not count it, a procedure whose rows are crossed with it.
class OptionalMatchCompositionTest : public TuringTest {
protected:
    void initialize() override {
        _env = TuringTestEnv::create(fs::Path {_outDir} / "turing");
        _interpreter = std::make_unique<QueryInterpreterV3>(&_env->getSystemManager());

        SystemAccessor system = _env->getSystemManager().accessUnique();
        Graph* graph = system.createGraph(_graphName);
        SimpleGraph::createSimpleGraph(graph);
    }

    void expectRows(std::string_view query, const Rows& expected) {
        StringRowSink sink;

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

        EXPECT_EQ(actual, sortedExpected) << "query: " << query;
    }

    std::unique_ptr<TuringTestEnv> _env;
    std::unique_ptr<QueryInterpreterV3> _interpreter;
    std::string _graphName {"simpledb"};
};

/**
* A mandatory MATCH beside an OPTIONAL MATCH
*/

// Two mandatory patterns are walked together and the optional one joins onto what they
// bound, so its single match repeats over each of their rows
TEST_F(OptionalMatchCompositionTest, optionalJoinsOntoTwoMandatoryPatterns) {
    expectRows("MATCH (p:Person {name: 'Remy'}) MATCH (p)-[:INTERESTED_IN]->(i) "
               "OPTIONAL MATCH (p)-[:KNOWS_WELL]->(f) RETURN i.name, f.name",
               {{"Computers", "Adam"},
                {"Eighties", "Adam"},
                {"Ghosts", "Adam"}});
}

// The mandatory rows survive with the pattern null when it matches none of them
TEST_F(OptionalMatchCompositionTest, mandatoryRowsSurviveAnUnmatchedOptional) {
    expectRows("MATCH (p:Person {name: 'Martina'}) MATCH (p)-[:INTERESTED_IN]->(i) "
               "OPTIONAL MATCH (p)-[:KNOWS_WELL]->(f) RETURN i.name, f.name",
               {{"Cooking", "null"}});
}

// A comma-separated pattern is crossed before the join, and the optional pattern joins
// onto one side of that product
TEST_F(OptionalMatchCompositionTest, optionalJoinsOntoOneSideOfACrossProduct) {
    expectRows("MATCH (p:Person {name: 'Remy'}), (q:Person {name: 'Adam'}) "
               "OPTIONAL MATCH (q)-[:KNOWS_WELL]->(f) RETURN p.name, q.name, f.name",
               {{"Remy", "Adam", "Remy"}});
}

/**
* Patterns feeding each other
*/

// A second OPTIONAL MATCH walks from what the first bound: the rows it matched fan out,
// and the ones it missed carry their null through
TEST_F(OptionalMatchCompositionTest, optionalWalksFromAnotherOptionalsVariable) {
    expectRows("MATCH (p:Person) OPTIONAL MATCH (p)-[:KNOWS_WELL]->(f) "
               "OPTIONAL MATCH (f)-[:INTERESTED_IN]->(i) RETURN p.name, f.name, i.name",
               {{"Remy", "Adam", "Bio"},
                {"Remy", "Adam", "Cooking"},
                {"Adam", "Remy", "Ghosts"},
                {"Adam", "Remy", "Computers"},
                {"Adam", "Remy", "Eighties"},
                {"Maxime", "null", "null"},
                {"Luc", "null", "null"},
                {"Martina", "null", "null"},
                {"Suhas", "null", "null"},
                {"Cyrus", "null", "null"},
                {"Doruk", "null", "null"}});
}

// Walking from a variable the first pattern left null matches nothing rather than reading
// the graph at an ID it holds no row for, so the second pattern nulls in turn
TEST_F(OptionalMatchCompositionTest, optionalWalksFromANullVariable) {
    expectRows("MATCH (p:Person {name: 'Doruk'}) OPTIONAL MATCH (p)-[:KNOWS_WELL]->(f) "
               "OPTIONAL MATCH (f)-[:INTERESTED_IN]->(i) RETURN p.name, f.name, i.name",
               {{"Doruk", "null", "null"}});
}

// A mandatory MATCH walking from the optional pattern's own variable is mandatory again,
// so the rows it left null are dropped
TEST_F(OptionalMatchCompositionTest, mandatoryWalksFromTheOptionalsVariable) {
    expectRows("MATCH (p:Person) OPTIONAL MATCH (p)-[:KNOWS_WELL]->(f) "
               "MATCH (f)-[:INTERESTED_IN]->(i) RETURN p.name, i.name",
               {{"Remy", "Bio"},
                {"Remy", "Cooking"},
                {"Adam", "Ghosts"},
                {"Adam", "Computers"},
                {"Adam", "Eighties"}});
}

// A mandatory MATCH after the join reaching back to a variable the join carried, not to
// the one it bound: the optional column rides along its rows
TEST_F(OptionalMatchCompositionTest, mandatoryReachesBackPastTheJoin) {
    expectRows("MATCH (p:Person {name: 'Remy'}) OPTIONAL MATCH (p)-[:KNOWS_WELL]->(f) "
               "MATCH (p)-[:INTERESTED_IN]->(i) RETURN f.name, i.name",
               {{"Adam", "Computers"},
                {"Adam", "Eighties"},
                {"Adam", "Ghosts"}});
}

/**
* A WITH barrier around the join
*/

// A barrier ahead of the join publishes the rows it joins onto
TEST_F(OptionalMatchCompositionTest, barrierAheadOfTheJoin) {
    expectRows("MATCH (p:Person) WITH p WHERE p.name = 'Doruk' "
               "OPTIONAL MATCH (p)-[:KNOWS_WELL]->(f) RETURN p.name, f.name",
               {{"Doruk", "null"}});
}

// A cut at that barrier bounds what the join reads, not what it produced
TEST_F(OptionalMatchCompositionTest, cutAtTheBarrierAheadOfTheJoin) {
    expectRows("MATCH (p:Person) WITH p LIMIT 2 OPTIONAL MATCH (p)-[:KNOWS_WELL]->(f) "
               "RETURN p.name, f.name",
               {{"Remy", "Adam"}, {"Adam", "Remy"}});
}

// A barrier after the join republishes the padded rows, and a predicate over it reads the
// null the join left - the anti-join Cypher writes as WHERE x IS NULL
TEST_F(OptionalMatchCompositionTest, barrierAfterTheJoinKeepsTheNulls) {
    expectRows("MATCH (p:Person) OPTIONAL MATCH (p)-[:KNOWS_WELL]->(f) "
               "WITH p, f WHERE f IS NULL RETURN p.name",
               {{"Maxime"}, {"Luc"}, {"Martina"}, {"Suhas"}, {"Cyrus"}, {"Doruk"}});
}

// DISTINCT past the barrier keys the padded rows on their null, so they collapse to one
TEST_F(OptionalMatchCompositionTest, distinctPastTheBarrierCollapsesTheNulls) {
    expectRows("MATCH (p:Person) OPTIONAL MATCH (p)-[:KNOWS_WELL]->(f) WITH DISTINCT f RETURN f",
               {{remyID}, {adamID}, {"null"}});
}

/**
* Aggregates over the joined rows
*/

// A grouped count charges the non-null rows of each group, so a group whose pattern missed
// counts zero rather than the padded row it kept
TEST_F(OptionalMatchCompositionTest, groupedCountIgnoresThePaddedRows) {
    expectRows("MATCH (p:Person) OPTIONAL MATCH (p)-[:KNOWS_WELL]->(f) RETURN p.name, count(f)",
               {{"Remy", "1"},
                {"Adam", "1"},
                {"Maxime", "0"},
                {"Luc", "0"},
                {"Martina", "0"},
                {"Suhas", "0"},
                {"Cyrus", "0"},
                {"Doruk", "0"}});
}

// count(*) tallies every row the join kept, padded ones included, where count(x) does not
TEST_F(OptionalMatchCompositionTest, groupedWildcardCountTalliesEveryRow) {
    expectRows("MATCH (p:Person) OPTIONAL MATCH (p)-[:KNOWS_WELL]->(f) "
               "RETURN p.name, count(*)",
               {{"Remy", "1"},
                {"Adam", "1"},
                {"Maxime", "1"},
                {"Luc", "1"},
                {"Martina", "1"},
                {"Suhas", "1"},
                {"Cyrus", "1"},
                {"Doruk", "1"}});
}

// The padded rows share one null key, so a distinct count charges them nothing
TEST_F(OptionalMatchCompositionTest, distinctCountIgnoresThePaddedRows) {
    expectRows("MATCH (p:Person) OPTIONAL MATCH (p)-[:KNOWS_WELL]->(f) RETURN count(DISTINCT f)",
               {{"2"}});
}

// A value reduction folds the non-null rows: the two KNOWS_WELL edges between Persons each
// carry a duration of 20, and the six padded rows carry none
TEST_F(OptionalMatchCompositionTest, valueReductionsFoldTheNonNullRows) {
    expectRows("MATCH (p:Person) OPTIONAL MATCH (p)-[r:KNOWS_WELL]->(f) RETURN sum(r.duration)",
               {{"40"}});
    expectRows("MATCH (p:Person) OPTIONAL MATCH (p)-[r:KNOWS_WELL]->(f) RETURN avg(r.duration)",
               {{"20"}});
    expectRows("MATCH (p:Person) OPTIONAL MATCH (p)-[r:KNOWS_WELL]->(f) "
               "RETURN min(r.duration), max(r.duration)",
               {{"20", "20"}});
}

// A reduction over a pattern nothing matched folds no row at all: sum is Cypher's zero and
// the extremes are null
TEST_F(OptionalMatchCompositionTest, valueReductionsOverAWhollyUnmatchedPattern) {
    expectRows("MATCH (p:Person {name: 'Doruk'}) OPTIONAL MATCH (p)-[r:KNOWS_WELL]->(f) "
               "RETURN sum(r.duration), min(r.duration), max(r.duration)",
               {{"0", "null", "null"}});
}

// collect gathers the non-null values, so a pattern nothing matched collects an empty list
TEST_F(OptionalMatchCompositionTest, collectGathersTheNonNullValues) {
    expectRows("MATCH (p:Person) OPTIONAL MATCH (p)-[:KNOWS_WELL]->(f) RETURN collect(f.name)",
               {{"Adam, Remy"}});
    expectRows("MATCH (p:Person {name: 'Doruk'}) OPTIONAL MATCH (p)-[:KNOWS_WELL]->(f) "
               "RETURN collect(f.name)",
               {{""}});
}

/**
* A CALL beside the join
*/

// A procedure's rows drive the join: what it yielded is the column the pattern walks from,
// and the rows it left unmatched are padded like any other
TEST_F(OptionalMatchCompositionTest, callDrivesTheJoin) {
    expectRows("CALL db.getNodes([0, 7]) YIELD id AS p OPTIONAL MATCH (p)-[:KNOWS_WELL]->(f) "
               "RETURN p, f",
               {{remyID, adamID}, {maximeID, "null"}});
}

// The same when the procedure yields only rows the pattern misses
TEST_F(OptionalMatchCompositionTest, callDrivesTheJoinWithNoMatch) {
    expectRows("CALL db.getNodes([7]) YIELD id AS p OPTIONAL MATCH (p)-[:KNOWS_WELL]->(f) "
               "RETURN p, f",
               {{maximeID, "null"}});
}

// A call after the join reads none of its rows, so its own are crossed with them: nine
// labels against the eight rows the join kept
TEST_F(OptionalMatchCompositionTest, callAfterTheJoinCrossesItsRows) {
    expectRows("MATCH (p:Person) OPTIONAL MATCH (p)-[:KNOWS_WELL]->(f) "
               "CALL db.labels() YIELD label RETURN count(label)",
               {{"72"}});
}

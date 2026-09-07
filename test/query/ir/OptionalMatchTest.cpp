#include <gtest/gtest.h>

#include <stddef.h>

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

// OPTIONAL MATCH, driven from Cypher text through the analyzer, DBProgramGenerator,
// NL lowering and the NL interpreter.
class OptionalMatchTest : public TuringTest {
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

    void expectRows(std::string_view query, const Rows& expected) {
        RowSink sink;
        const QueryStatus status = runQuery(query, &sink);
        ASSERT_TRUE(status.isOk()) << "query: " << query << "\nerror: " << status.getError();

        Rows actual;
        sink.sortedRows(actual);

        Rows sortedExpected = expected;
        std::sort(sortedExpected.begin(), sortedExpected.end());

        std::string actualText;
        describeRows(actual, actualText);

        EXPECT_EQ(actual, sortedExpected) << "query: " << query << "\nactual:\n" << actualText;
    }

    // The rows in the order the query emits them, for the ORDER BY case
    void expectRowsInOrder(std::string_view query, const Rows& expected) {
        RowSink sink;
        const QueryStatus status = runQuery(query, &sink);
        ASSERT_TRUE(status.isOk()) << "query: " << query << "\nerror: " << status.getError();

        EXPECT_EQ(sink.rows(), expected) << "query: " << query;
    }

    void expectRowCount(std::string_view query, size_t expected) {
        RowSink sink;
        const QueryStatus status = runQuery(query, &sink);
        ASSERT_TRUE(status.isOk()) << "query: " << query << "\nerror: " << status.getError();

        EXPECT_EQ(sink.rows().size(), expected) << "query: " << query;
    }

    void expectCounts(std::string_view query, const Counts& expected) {
        CountSink sink;
        const QueryStatus status = runQuery(query, &sink);
        ASSERT_TRUE(status.isOk()) << "query: " << query << "\nerror: " << status.getError();

        Counts actual;
        sink.sortedCounts(actual);

        EXPECT_EQ(actual, expected) << "query: " << query;
    }

    std::unique_ptr<TuringTestEnv> _env;
    std::unique_ptr<QueryInterpreterV3> _interpreter;
    std::string _graphName {"simpledb"};
};

// The unmatched rows of the left are kept, with the pattern's own variables null
TEST_F(OptionalMatchTest, UnmatchedRowsAreNullPadded) {
    expectRows("MATCH (p:Person) OPTIONAL MATCH (p)-[:KNOWS_WELL]->(f) RETURN p.name, f.name",
               {{"Remy", "Adam"},
                {"Adam", "Remy"},
                {"Maxime", "null"},
                {"Luc", "null"},
                {"Martina", "null"},
                {"Suhas", "null"},
                {"Cyrus", "null"},
                {"Doruk", "null"}});
}

// A pattern every left row matches adds no null row, and fans out exactly as a MATCH does
TEST_F(OptionalMatchTest, FullyMatchedPatternFansOutLikeMatch) {
    expectRows("MATCH (p:Person) OPTIONAL MATCH (p)-[:INTERESTED_IN]->(i) RETURN p.name, i.name",
               {{"Remy", "Ghosts"},
                {"Remy", "Computers"},
                {"Remy", "Eighties"},
                {"Adam", "Bio"},
                {"Adam", "Cooking"},
                {"Maxime", "Bio"},
                {"Maxime", "Padel"},
                {"Luc", "Animals"},
                {"Luc", "Computers"},
                {"Martina", "Cooking"},
                {"Suhas", "Gym"},
                {"Suhas", "JiuJitsu"},
                {"Cyrus", "Gym"},
                {"Cyrus", "Travel"},
                {"Doruk", "Gym"}});
}

// The whole pattern matches or none of it does: a second hop that finds nothing nulls the
// first hop's variables too, and leaves one row - not one per partial match
TEST_F(OptionalMatchTest, PartialMatchNullsTheWholePattern) {
    expectRows("MATCH (p:Person {name: 'Adam'}) "
               "OPTIONAL MATCH (p)-[:INTERESTED_IN]->(i)-[:INTERESTED_IN]->(j) "
               "RETURN p.name, i.name, j.name",
               {{"Adam", "null", "null"}});
}

// The pattern's WHERE is part of the match: a left row whose every match the predicate
// cuts is null-padded, not dropped
TEST_F(OptionalMatchTest, PatternWhereIsPartOfTheMatch) {
    expectRows("MATCH (p:Person) OPTIONAL MATCH (p)-[:INTERESTED_IN]->(i) "
               "WHERE i.name = 'Gym' RETURN p.name, i.name",
               {{"Remy", "null"},
                {"Adam", "null"},
                {"Maxime", "null"},
                {"Luc", "null"},
                {"Martina", "null"},
                {"Suhas", "Gym"},
                {"Cyrus", "Gym"},
                {"Doruk", "Gym"}});
}

// An unmatched edge variable is null too, and so is every property read off it
TEST_F(OptionalMatchTest, UnmatchedEdgeAndItsPropertiesAreNull) {
    expectRows("MATCH (p:Person) OPTIONAL MATCH (p)-[r:KNOWS_WELL]->(f) RETURN p.name, r.name",
               {{"Remy", "Remy -> Adam"},
                {"Adam", "Adam -> Remy"},
                {"Maxime", "null"},
                {"Luc", "null"},
                {"Martina", "null"},
                {"Suhas", "null"},
                {"Cyrus", "null"},
                {"Doruk", "null"}});
}

// A null node projected whole renders as null rather than as an out-of-range ID
TEST_F(OptionalMatchTest, UnmatchedNodeProjectsAsNull) {
    expectRows("MATCH (p:Person {name: 'Doruk'}) OPTIONAL MATCH (p)-[:KNOWS_WELL]->(f) "
               "RETURN p.name, f",
               {{"Doruk", "null"}});
}

// count(x) charges the non-null rows, so the padded rows are not counted; count(*) tallies
// every row the OPTIONAL MATCH kept
TEST_F(OptionalMatchTest, CountIgnoresThePaddedRows) {
    expectCounts("MATCH (p:Person) OPTIONAL MATCH (p)-[:KNOWS_WELL]->(f) RETURN count(f)", {2});
    expectCounts("MATCH (p:Person) OPTIONAL MATCH (p)-[:KNOWS_WELL]->(f) RETURN count(*)", {8});
}

// Two OPTIONAL MATCHes chain: the second joins onto the rows the first left behind
TEST_F(OptionalMatchTest, ChainedOptionalMatches) {
    expectRows("MATCH (p:Person {name: 'Martina'}) "
               "OPTIONAL MATCH (p)-[:KNOWS_WELL]->(f) "
               "OPTIONAL MATCH (p)-[:INTERESTED_IN]->(i) "
               "RETURN p.name, f.name, i.name",
               {{"Martina", "null", "Cooking"}});
}

// A MATCH after an OPTIONAL MATCH is mandatory again, so it drops the padded rows
TEST_F(OptionalMatchTest, MandatoryMatchAfterOptionalDropsPaddedRows) {
    expectRows("MATCH (p:Person) "
               "OPTIONAL MATCH (p)-[:KNOWS_WELL]->(f) "
               "MATCH (f)-[:INTERESTED_IN]->(i) "
               "RETURN p.name, i.name",
               {{"Remy", "Bio"},
                {"Remy", "Cooking"},
                {"Adam", "Ghosts"},
                {"Adam", "Computers"},
                {"Adam", "Eighties"}});
}

// A WITH barrier carries the padded rows through unchanged
TEST_F(OptionalMatchTest, PaddedRowsSurviveABarrier) {
    expectRows("MATCH (p:Person {name: 'Luc'}) "
               "OPTIONAL MATCH (p)-[:KNOWS_WELL]->(f) "
               "WITH p.name AS person, f AS friend "
               "RETURN person, friend",
               {{"Luc", "null"}});
}

// A query opening on OPTIONAL MATCH joins onto the single empty row it starts from, so a
// pattern that matches behaves as a MATCH does
TEST_F(OptionalMatchTest, LeadingOptionalMatchThatMatches) {
    expectRows("OPTIONAL MATCH (p:Person {name: 'Remy'}) RETURN p.name", {{"Remy"}});
}

// And one that matches nothing leaves that row behind, with the pattern null
TEST_F(OptionalMatchTest, LeadingOptionalMatchThatMissesKeepsOneRow) {
    expectRows("OPTIONAL MATCH (p:Person {name: 'Nobody'}) RETURN p.name", {{"null"}});
}

// A pattern sharing no variable with the rows it joins onto crosses them, as a
// comma-separated MATCH does
TEST_F(OptionalMatchTest, DisconnectedPatternCrossesTheRows) {
    expectRows("MATCH (p:Person {name: 'Remy'}) OPTIONAL MATCH (i:Interest {name: 'Gym'}) "
               "RETURN p.name, i.name",
               {{"Remy", "Gym"}});
}

// The anti-join: keeping the rows whose pattern missed is what IS NULL over the pattern's
// own variable asks for
TEST_F(OptionalMatchTest, IsNullKeepsTheRowsThePatternMissed) {
    expectRows("MATCH (p:Person) OPTIONAL MATCH (p)-[:KNOWS_WELL]->(f) "
               "WITH p, f WHERE f IS NULL RETURN p.name",
               {{"Maxime"}, {"Luc"}, {"Martina"}, {"Suhas"}, {"Cyrus"}, {"Doruk"}});
}

// And IS NOT NULL keeps the ones it matched
TEST_F(OptionalMatchTest, IsNotNullKeepsTheRowsThePatternMatched) {
    expectRows("MATCH (p:Person) OPTIONAL MATCH (p)-[:KNOWS_WELL]->(f) "
               "WITH p, f WHERE f IS NOT NULL RETURN p.name",
               {{"Remy"}, {"Adam"}});
}

// A property of a null entity is null, so the test reads the same through an alias
TEST_F(OptionalMatchTest, IsNullOverAPropertyOfANullEntity) {
    expectRows("MATCH (p:Person) OPTIONAL MATCH (p)-[:KNOWS_WELL]->(f) "
               "WITH p.name AS person, f.name AS friend WHERE friend IS NULL RETURN person",
               {{"Maxime"}, {"Luc"}, {"Martina"}, {"Suhas"}, {"Cyrus"}, {"Doruk"}});
}

// A function reading a null entity reads a null rather than the graph at an ID it holds no
// row for
TEST_F(OptionalMatchTest, LabelsOfANullNodeIsNull) {
    expectRows("MATCH (p:Person {name: 'Doruk'}) OPTIONAL MATCH (p)-[:KNOWS_WELL]->(f) "
               "RETURN labels(f)",
               {{"null"}});
}

// An ORDER BY reads the padded rows as any other: a null sorts after every value, so the
// rows the pattern missed come last
TEST_F(OptionalMatchTest, OrderByOverPaddedRows) {
    expectRowsInOrder("MATCH (p:Person) OPTIONAL MATCH (p)-[:KNOWS_WELL]->(f) "
                      "RETURN p.name, f.name ORDER BY f.name, p.name",
                      {{"Remy", "Adam"},
                       {"Adam", "Remy"},
                       {"Cyrus", "null"},
                       {"Doruk", "null"},
                       {"Luc", "null"},
                       {"Martina", "null"},
                       {"Maxime", "null"},
                       {"Suhas", "null"}});
}

// A LIMIT cuts the drained rows, matched and padded alike
TEST_F(OptionalMatchTest, LimitCutsTheDrainedRows) {
    expectRowCount("MATCH (p:Person) OPTIONAL MATCH (p)-[:KNOWS_WELL]->(f) "
                   "RETURN p.name, f.name LIMIT 3",
                   3);
}

// DISTINCT keys the padded rows on their null, so they collapse together
TEST_F(OptionalMatchTest, DistinctCollapsesThePaddedRows) {
    expectRows("MATCH (p:Person) OPTIONAL MATCH (p)-[:KNOWS_WELL]->(f) RETURN DISTINCT f.name",
               {{"Adam"}, {"Remy"}, {"null"}});
}

// A group whose rows the pattern all missed collects an empty list, and keeps its row
// rather than dropping out of the result
TEST_F(OptionalMatchTest, CollectsAnEmptyListForAnUnmatchedOptional) {
    expectRows("MATCH (p:Person) OPTIONAL MATCH (p)-[:KNOWS_WELL]->(f) "
               "RETURN p.name, collect(f.name)",
               {{"Remy", "[Adam]"},
                {"Adam", "[Remy]"},
                {"Maxime", "[]"},
                {"Luc", "[]"},
                {"Martina", "[]"},
                {"Suhas", "[]"},
                {"Cyrus", "[]"},
                {"Doruk", "[]"}});
}

// A reduction beside the collected list charges the same groups: a group the pattern
// missed counts none of its padded rows
TEST_F(OptionalMatchTest, CountsBesideACollectedListOverPaddedRows) {
    expectRows("MATCH (p:Person) OPTIONAL MATCH (p)-[:KNOWS_WELL]->(f) "
               "RETURN p.name, collect(f.name), count(f)",
               {{"Remy", "[Adam]", "1"},
                {"Adam", "[Remy]", "1"},
                {"Maxime", "[]", "0"},
                {"Luc", "[]", "0"},
                {"Martina", "[]", "0"},
                {"Suhas", "[]", "0"},
                {"Cyrus", "[]", "0"},
                {"Doruk", "[]", "0"}});
}

// collect() drops a null, and an entity the pattern missed is one, so the group of a row
// it padded collects an empty list rather than the invalid ID standing for the null
TEST_F(OptionalMatchTest, CollectsAnEmptyListOfEntitiesForAnUnmatchedOptional) {
    expectRows("MATCH (p:Person) OPTIONAL MATCH (p)-[:KNOWS_WELL]->(f) "
               "RETURN p.name, collect(f)",
               {{"Remy", "[1]"},
                {"Adam", "[0]"},
                {"Maxime", "[]"},
                {"Luc", "[]"},
                {"Martina", "[]"},
                {"Suhas", "[]"},
                {"Cyrus", "[]"},
                {"Doruk", "[]"}});
}

// The edge sibling: an unmatched relationship is a null too
TEST_F(OptionalMatchTest, CollectsAnEmptyListOfEdgesForAnUnmatchedOptional) {
    expectRows("MATCH (p:Person {name: 'Luc'}) OPTIONAL MATCH (p)-[r:KNOWS_WELL]->(f) "
               "RETURN p.name, collect(r)",
               {{"Luc", "[]"}});
}

// collect(DISTINCT n) keys on the ID, and the null the pattern left is no key of its own
TEST_F(OptionalMatchTest, CollectsDistinctEntitiesWithoutTheUnmatchedNull) {
    expectRows("MATCH (p:Person) OPTIONAL MATCH (p)-[:KNOWS_WELL]->(f) "
               "RETURN collect(DISTINCT f)",
               {{"[1, 0]"}});
}

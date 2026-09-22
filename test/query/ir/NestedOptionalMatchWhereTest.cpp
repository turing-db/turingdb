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

// An OPTIONAL MATCH walking from another OPTIONAL MATCH's variable, carrying a WHERE of its
// own. The predicate belongs to the nested pattern, so a row whose every nested match it
// cuts comes back with the nested variables null - the same row a nested pattern that
// matched nothing leaves behind. Nothing here drops a row.
class NestedOptionalMatchWhereTest : public TuringTest {
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

// Remy walks to Adam, whose Bio passes the predicate. Adam walks to Remy, whose three
// interests all fail it, so that row keeps the friend and nulls the interest.
TEST_F(NestedOptionalMatchWhereTest, WhereOnTheNestedPatternNullsWhatItCuts) {
    expectRows("MATCH (p:Person) OPTIONAL MATCH (p)-[:KNOWS_WELL]->(f) "
               "OPTIONAL MATCH (f)-[:INTERESTED_IN]->(i) WHERE i.name = 'Bio' "
               "RETURN p.name, f.name, i.name",
               {{"Remy", "Adam", "Bio"},
                {"Adam", "Remy", "null"},
                {"Maxime", "null", "null"},
                {"Luc", "null", "null"},
                {"Martina", "null", "null"},
                {"Suhas", "null", "null"},
                {"Cyrus", "null", "null"},
                {"Doruk", "null", "null"}});
}

// A predicate no nested match satisfies leaves all 8 rows, 2 of them still holding a friend
TEST_F(NestedOptionalMatchWhereTest, NoNestedMatchPassesTheWhere) {
    expectRows("MATCH (p:Person) OPTIONAL MATCH (p)-[:KNOWS_WELL]->(f) "
               "OPTIONAL MATCH (f)-[:INTERESTED_IN]->(i) WHERE i.name = 'Nothing' "
               "RETURN p.name, f.name, i.name",
               {{"Remy", "Adam", "null"},
                {"Adam", "Remy", "null"},
                {"Maxime", "null", "null"},
                {"Luc", "null", "null"},
                {"Martina", "null", "null"},
                {"Suhas", "null", "null"},
                {"Cyrus", "null", "null"},
                {"Doruk", "null", "null"}});
}

// The nested pattern binds i, so i is never null inside a match of it and the predicate
// cuts every one - the same 8 rows, none dropped
TEST_F(NestedOptionalMatchWhereTest, IsNullOnTheNestedPatternsOwnVariableCutsEveryMatch) {
    expectRows("MATCH (p:Person) OPTIONAL MATCH (p)-[:KNOWS_WELL]->(f) "
               "OPTIONAL MATCH (f)-[:INTERESTED_IN]->(i) WHERE i IS NULL "
               "RETURN p.name, f.name, i.name",
               {{"Remy", "Adam", "null"},
                {"Adam", "Remy", "null"},
                {"Maxime", "null", "null"},
                {"Luc", "null", "null"},
                {"Martina", "null", "null"},
                {"Suhas", "null", "null"},
                {"Cyrus", "null", "null"},
                {"Doruk", "null", "null"}});
}

// The nested WHERE reads the variable the outer OPTIONAL MATCH bound, which is null on 6 of
// the 8 rows: f.name = 'Adam' is then null, not true, and those rows have no match to cut
TEST_F(NestedOptionalMatchWhereTest, TheNestedWhereReadsTheOuterOptionalsVariable) {
    expectRows("MATCH (p:Person) OPTIONAL MATCH (p)-[:KNOWS_WELL]->(f) "
               "OPTIONAL MATCH (f)-[:INTERESTED_IN]->(i) WHERE f.name = 'Adam' "
               "RETURN p.name, f.name, i.name",
               {{"Remy", "Adam", "Bio"},
                {"Remy", "Adam", "Cooking"},
                {"Adam", "Remy", "null"},
                {"Maxime", "null", "null"},
                {"Luc", "null", "null"},
                {"Martina", "null", "null"},
                {"Suhas", "null", "null"},
                {"Cyrus", "null", "null"},
                {"Doruk", "null", "null"}});
}

// Both KNOWS_WELL edges between Remy and Adam carry duration 20, so the predicate keeps
// every nested match on those two rows and cuts nothing anywhere else
TEST_F(NestedOptionalMatchWhereTest, TheNestedWhereReadsTheOuterOptionalsEdge) {
    expectRows("MATCH (p:Person) OPTIONAL MATCH (p)-[r:KNOWS_WELL]->(f) "
               "OPTIONAL MATCH (f)-[:INTERESTED_IN]->(i) WHERE r.duration = 20 "
               "RETURN p.name, f.name, i.name",
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

// A nested WHERE naming none of the nested pattern's variables still cuts per row: only
// Doruk's Gym walks on to its fans, the other three keep their interest and null the fan
TEST_F(NestedOptionalMatchWhereTest, TheNestedWhereReadsTheMandatoryVariable) {
    expectRows("MATCH (p:Person {hasPhD: false}) OPTIONAL MATCH (p)-[:INTERESTED_IN]->(i) "
               "OPTIONAL MATCH (i)<-[:INTERESTED_IN]-(q:Person) WHERE p.name = 'Doruk' "
               "RETURN p.name, i.name, q.name",
               {{"Doruk", "Gym", "Cyrus"},
                {"Doruk", "Gym", "Doruk"},
                {"Doruk", "Gym", "Suhas"},
                {"Maxime", "Bio", "null"},
                {"Maxime", "Padel", "null"},
                {"Suhas", "Gym", "null"},
                {"Suhas", "JiuJitsu", "null"},
                {"Cyrus", "Gym", "null"},
                {"Cyrus", "Travel", "null"}});
}

// The predicate reads the nested pattern's own edge: Remy's Ghosts and Eighties edges carry
// duration 20, Adam's two carry no duration at all, so Remy's row nulls
TEST_F(NestedOptionalMatchWhereTest, TheNestedWhereReadsItsOwnEdge) {
    expectRows("MATCH (p:Person) OPTIONAL MATCH (p)-[:KNOWS_WELL]->(f) "
               "OPTIONAL MATCH (f)-[r:INTERESTED_IN]->(i) WHERE r.duration = 20 "
               "RETURN p.name, f.name, i.name",
               {{"Remy", "Adam", "null"},
                {"Adam", "Remy", "Ghosts"},
                {"Adam", "Remy", "Eighties"},
                {"Maxime", "null", "null"},
                {"Luc", "null", "null"},
                {"Martina", "null", "null"},
                {"Suhas", "null", "null"},
                {"Cyrus", "null", "null"},
                {"Doruk", "null", "null"}});
}

// A predicate over one column of the nested pattern and one of the mandatory match: Remy
// and Adam both hold a PhD, and Ghosts and Computers are the real interests Remy walks to
TEST_F(NestedOptionalMatchWhereTest, TheNestedWhereComparesTheNestedAndMandatoryColumns) {
    expectRows("MATCH (p:Person) OPTIONAL MATCH (p)-[:KNOWS_WELL]->(f) "
               "OPTIONAL MATCH (f)-[:INTERESTED_IN]->(i) WHERE i.isReal = p.hasPhD "
               "RETURN p.name, f.name, i.name",
               {{"Remy", "Adam", "null"},
                {"Adam", "Remy", "Ghosts"},
                {"Adam", "Remy", "Computers"},
                {"Maxime", "null", "null"},
                {"Luc", "null", "null"},
                {"Martina", "null", "null"},
                {"Suhas", "null", "null"},
                {"Cyrus", "null", "null"},
                {"Doruk", "null", "null"}});
}

// Remy's three interests all carry isReal, so a test for its absence keeps the friend and
// nulls the interest
TEST_F(NestedOptionalMatchWhereTest, IsNullOnAPropertyInTheNestedWhere) {
    expectRows("MATCH (p:Person {name: 'Adam'}) OPTIONAL MATCH (p)-[:KNOWS_WELL]->(f) "
               "OPTIONAL MATCH (f)-[:INTERESTED_IN]->(i) WHERE i.isReal IS NULL "
               "RETURN f.name, i.name",
               {{"Remy", "null"}});
}

// Eighties is the one it is false on
TEST_F(NestedOptionalMatchWhereTest, APropertyValueInTheNestedWhere) {
    expectRows("MATCH (p:Person {name: 'Adam'}) OPTIONAL MATCH (p)-[:KNOWS_WELL]->(f) "
               "OPTIONAL MATCH (f)-[:INTERESTED_IN]->(i) WHERE i.isReal = false "
               "RETURN f.name, i.name",
               {{"Remy", "Eighties"}});
}

// A compound predicate keeps one match on each of the two rows that have any
TEST_F(NestedOptionalMatchWhereTest, ADisjunctionInTheNestedWhere) {
    expectRows("MATCH (p:Person) OPTIONAL MATCH (p)-[:KNOWS_WELL]->(f) "
               "OPTIONAL MATCH (f)-[:INTERESTED_IN]->(i) "
               "WHERE i.name = 'Ghosts' OR i.name = 'Cooking' "
               "RETURN p.name, f.name, i.name",
               {{"Remy", "Adam", "Cooking"},
                {"Adam", "Remy", "Ghosts"},
                {"Maxime", "null", "null"},
                {"Luc", "null", "null"},
                {"Martina", "null", "null"},
                {"Suhas", "null", "null"},
                {"Cyrus", "null", "null"},
                {"Doruk", "null", "null"}});
}

// The predicate reads a column a barrier aliased rather than the pattern variable itself
TEST_F(NestedOptionalMatchWhereTest, TheNestedWhereReadsAnAliasedColumn) {
    expectRows("MATCH (p:Person) OPTIONAL MATCH (p)-[:KNOWS_WELL]->(f) "
               "WITH p, f.name AS friend, f "
               "OPTIONAL MATCH (f)-[:INTERESTED_IN]->(i) WHERE friend = 'Adam' "
               "RETURN p.name, friend, i.name",
               {{"Remy", "Adam", "Bio"},
                {"Remy", "Adam", "Cooking"},
                {"Adam", "Remy", "null"},
                {"Maxime", "null", "null"},
                {"Luc", "null", "null"},
                {"Martina", "null", "null"},
                {"Suhas", "null", "null"},
                {"Cyrus", "null", "null"},
                {"Doruk", "null", "null"}});
}

// Every variable of the nested pattern is already bound, so it has nothing to null: the
// predicate cutting Adam's match leaves his row exactly as the first pattern built it
TEST_F(NestedOptionalMatchWhereTest, ANestedPatternBindingNothingNewDropsNoRow) {
    expectRows("MATCH (p:Person) OPTIONAL MATCH (p)-[:KNOWS_WELL]->(f) "
               "OPTIONAL MATCH (f)-[:KNOWS_WELL]->(p) WHERE p.name = 'Remy' "
               "RETURN p.name, f.name",
               {{"Remy", "Adam"},
                {"Adam", "Remy"},
                {"Maxime", "null"},
                {"Luc", "null"},
                {"Martina", "null"},
                {"Suhas", "null"},
                {"Cyrus", "null"},
                {"Doruk", "null"}});
}

// The nested pattern is two hops and the predicate sits on its far end: of Adam's two
// interests only Bio has Maxime for a fan
TEST_F(NestedOptionalMatchWhereTest, ATwoHopNestedPatternWithItsOwnWhere) {
    expectRows("MATCH (p:Person {name: 'Remy'}) OPTIONAL MATCH (p)-[:KNOWS_WELL]->(f) "
               "OPTIONAL MATCH (f)-[:INTERESTED_IN]->(i)<-[:INTERESTED_IN]-(q:Person) "
               "WHERE q.name = 'Maxime' "
               "RETURN f.name, i.name, q.name",
               {{"Adam", "Bio", "Maxime"}});
}

// Ghosts knows Remy well, so the undirected nested hop finds it from Remy and not from Adam
TEST_F(NestedOptionalMatchWhereTest, AnUndirectedNestedHopWithAWhere) {
    expectRows("MATCH (p:Person) OPTIONAL MATCH (p)-[:KNOWS_WELL]->(f) "
               "OPTIONAL MATCH (f)-[:KNOWS_WELL]-(g) WHERE g.name = 'Ghosts' "
               "RETURN p.name, f.name, g.name",
               {{"Remy", "Adam", "null"},
                {"Adam", "Remy", "Ghosts"},
                {"Maxime", "null", "null"},
                {"Luc", "null", "null"},
                {"Martina", "null", "null"},
                {"Suhas", "null", "null"},
                {"Cyrus", "null", "null"},
                {"Doruk", "null", "null"}});
}

// Both patterns carry a WHERE. Remy and Luc are the two whose interest survives the first
// one, and Luc is the only fan of Computers the second one keeps.
TEST_F(NestedOptionalMatchWhereTest, BothOptionalPatternsCarryAWhere) {
    expectRows("MATCH (p:Person) OPTIONAL MATCH (p)-[:INTERESTED_IN]->(i) "
               "WHERE i.name = 'Computers' "
               "OPTIONAL MATCH (i)<-[:INTERESTED_IN]-(q:Person) WHERE q.name = 'Luc' "
               "RETURN p.name, i.name, q.name",
               {{"Remy", "Computers", "Luc"},
                {"Luc", "Computers", "Luc"},
                {"Adam", "null", "null"},
                {"Maxime", "null", "null"},
                {"Martina", "null", "null"},
                {"Suhas", "null", "null"},
                {"Cyrus", "null", "null"},
                {"Doruk", "null", "null"}});
}

// A barrier between the two patterns republishes the null the first one wrote, and the
// nested WHERE reads it the same way
TEST_F(NestedOptionalMatchWhereTest, ABarrierBetweenTheTwoOptionals) {
    expectRows("MATCH (p:Person) OPTIONAL MATCH (p)-[:KNOWS_WELL]->(f) WITH p, f "
               "OPTIONAL MATCH (f)-[:INTERESTED_IN]->(i) WHERE i.name = 'Bio' "
               "RETURN p.name, f.name, i.name",
               {{"Remy", "Adam", "Bio"},
                {"Adam", "Remy", "null"},
                {"Maxime", "null", "null"},
                {"Luc", "null", "null"},
                {"Martina", "null", "null"},
                {"Suhas", "null", "null"},
                {"Cyrus", "null", "null"},
                {"Doruk", "null", "null"}});
}

// Three patterns with the WHERE on the middle one: Bio survives it and the third pattern
// fans out over its two fans
TEST_F(NestedOptionalMatchWhereTest, ThreeOptionalsWithTheWhereInTheMiddle) {
    expectRows("MATCH (p:Person {name: 'Remy'}) OPTIONAL MATCH (p)-[:KNOWS_WELL]->(f) "
               "OPTIONAL MATCH (f)-[:INTERESTED_IN]->(i) WHERE i.name = 'Bio' "
               "OPTIONAL MATCH (i)<-[:INTERESTED_IN]-(q:Person) "
               "RETURN f.name, i.name, q.name",
               {{"Adam", "Bio", "Adam"},
                {"Adam", "Bio", "Maxime"}});
}

// And with the middle predicate cutting every match, the third pattern walks from the null
// it left, matching nothing in turn
TEST_F(NestedOptionalMatchWhereTest, AThirdOptionalWalksFromWhatTheNestedWhereNulled) {
    expectRows("MATCH (p:Person {name: 'Adam'}) OPTIONAL MATCH (p)-[:KNOWS_WELL]->(f) "
               "OPTIONAL MATCH (f)-[:INTERESTED_IN]->(i) WHERE i.name = 'Bio' "
               "OPTIONAL MATCH (i)<-[:INTERESTED_IN]-(q:Person) "
               "RETURN f.name, i.name, q.name",
               {{"Remy", "null", "null"}});
}

// A mandatory MATCH behind the nested pattern is mandatory again, so the 7 rows the nested
// WHERE padded are dropped and only Remy's Bio walks on
TEST_F(NestedOptionalMatchWhereTest, AMandatoryMatchAfterTheNestedWhereDropsThePaddedRows) {
    expectRows("MATCH (p:Person) OPTIONAL MATCH (p)-[:KNOWS_WELL]->(f) "
               "OPTIONAL MATCH (f)-[:INTERESTED_IN]->(i) WHERE i.name = 'Bio' "
               "MATCH (i)<-[:INTERESTED_IN]-(q:Person) "
               "RETURN p.name, i.name, q.name",
               {{"Remy", "Bio", "Adam"},
                {"Remy", "Bio", "Maxime"}});
}

// The same predicate behind a WITH barrier is a filter over the rows the patterns produced,
// so it drops the 7 the nested WHERE keeps padded
TEST_F(NestedOptionalMatchWhereTest, TheSamePredicateBehindABarrierFilters) {
    expectRows("MATCH (p:Person) OPTIONAL MATCH (p)-[:KNOWS_WELL]->(f) "
               "OPTIONAL MATCH (f)-[:INTERESTED_IN]->(i) WITH p, f, i WHERE i.name = 'Bio' "
               "RETURN p.name, f.name, i.name",
               {{"Remy", "Adam", "Bio"}});
}

// count(i) counts the one nested match the predicate kept, count(*) the 8 rows it left
TEST_F(NestedOptionalMatchWhereTest, CountSkipsTheNulledNestedMatches) {
    expectCounts("MATCH (p:Person) OPTIONAL MATCH (p)-[:KNOWS_WELL]->(f) "
                 "OPTIONAL MATCH (f)-[:INTERESTED_IN]->(i) WHERE i.name = 'Bio' "
                 "RETURN count(i)",
                 {1});

    expectCounts("MATCH (p:Person) OPTIONAL MATCH (p)-[:KNOWS_WELL]->(f) "
                 "OPTIONAL MATCH (f)-[:INTERESTED_IN]->(i) WHERE i.name = 'Bio' "
                 "RETURN count(*)",
                 {8});
}

// Grouped on the person, 7 of the 8 groups count 0
TEST_F(NestedOptionalMatchWhereTest, GroupedCountSkipsTheNulledNestedMatches) {
    expectCounts("MATCH (p:Person) OPTIONAL MATCH (p)-[:KNOWS_WELL]->(f) "
                 "OPTIONAL MATCH (f)-[:INTERESTED_IN]->(i) WHERE i.name = 'Bio' "
                 "RETURN p.name, count(i)",
                 {0, 0, 0, 0, 0, 0, 0, 1});
}

// collect leaves the nulls out too
TEST_F(NestedOptionalMatchWhereTest, CollectSkipsTheNulledNestedMatches) {
    expectRows("MATCH (p:Person) OPTIONAL MATCH (p)-[:KNOWS_WELL]->(f) "
               "OPTIONAL MATCH (f)-[:INTERESTED_IN]->(i) WHERE i.name = 'Bio' "
               "RETURN collect(i.name)",
               {{"[Bio]"}});
}

// DISTINCT reads the null as a value of its own, so the 7 padded rows fold into one
TEST_F(NestedOptionalMatchWhereTest, DistinctOverTheNestedWhere) {
    expectRows("MATCH (p:Person) OPTIONAL MATCH (p)-[:KNOWS_WELL]->(f) "
               "OPTIONAL MATCH (f)-[:INTERESTED_IN]->(i) WHERE i.name = 'Bio' "
               "RETURN DISTINCT i.name",
               {{"Bio"}, {"null"}});
}

// The two patterns and the nested WHERE inside a per-row CALL subquery
TEST_F(NestedOptionalMatchWhereTest, NestedOptionalsInsideAPerRowCall) {
    expectRows("MATCH (p:Person {name: 'Remy'}) "
               "CALL (p) { OPTIONAL MATCH (p)-[:KNOWS_WELL]->(f) "
               "OPTIONAL MATCH (f)-[:INTERESTED_IN]->(i) WHERE i.name = 'Bio' "
               "RETURN i.name AS interest } "
               "RETURN p.name, interest",
               {{"Remy", "Bio"}});
}

// NOT reads the null the property of an unmatched row gives it: Remy's row keeps Cooking
// and Adam's keeps all three of Remy's interests
TEST_F(NestedOptionalMatchWhereTest, NotInTheNestedWhere) {
    expectRows("MATCH (p:Person) OPTIONAL MATCH (p)-[:KNOWS_WELL]->(f) "
               "OPTIONAL MATCH (f)-[:INTERESTED_IN]->(i) WHERE NOT i.name = 'Bio' "
               "RETURN p.name, f.name, i.name",
               {{"Remy", "Adam", "Cooking"},
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

// A label test in the nested WHERE: Ghosts is the one KNOWS_WELL neighbour of a Person that
// is an Interest, and it is Remy's
TEST_F(NestedOptionalMatchWhereTest, ALabelPredicateInTheNestedWhere) {
    expectRows("MATCH (p:Person) OPTIONAL MATCH (p)-[:KNOWS_WELL]-(g) WHERE g:Interest "
               "RETURN p.name, g.name",
               {{"Remy", "Ghosts"},
                {"Adam", "null"},
                {"Maxime", "null"},
                {"Luc", "null"},
                {"Martina", "null"},
                {"Suhas", "null"},
                {"Cyrus", "null"},
                {"Doruk", "null"}});
}

// Each UNION branch carries its own pair of patterns: Remy's friend Adam is interested in
// Bio, Adam's friend Remy is not
TEST_F(NestedOptionalMatchWhereTest, EachUnionBranchCarriesItsOwnNestedPatterns) {
    expectRows("MATCH (p:Person {name: 'Remy'}) OPTIONAL MATCH (p)-[:KNOWS_WELL]->(f) "
               "OPTIONAL MATCH (f)-[:INTERESTED_IN]->(i) WHERE i.name = 'Bio' "
               "RETURN i.name AS interest "
               "UNION ALL "
               "MATCH (p:Person {name: 'Adam'}) OPTIONAL MATCH (p)-[:KNOWS_WELL]->(f) "
               "OPTIONAL MATCH (f)-[:INTERESTED_IN]->(i) WHERE i.name = 'Bio' "
               "RETURN i.name AS interest",
               {{"Bio"}, {"null"}});
}

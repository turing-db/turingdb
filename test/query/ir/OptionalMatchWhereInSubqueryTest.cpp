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

// An OPTIONAL MATCH carrying a WHERE inside a subquery body: one CALL, a CALL in a CALL, an
// OPTIONAL CALL, and the nullable columns crossing those boundaries in both directions.
class OptionalMatchWhereInSubqueryTest : public TuringTest {
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

    std::unique_ptr<TuringTestEnv> _env;
    std::unique_ptr<QueryInterpreterV3> _interpreter;
    std::string _graphName {"simpledb"};
};

// The body pads its own input row, so every person comes back once: Remy with the friend
// the predicate kept, Adam with the null it cut
TEST_F(OptionalMatchWhereInSubqueryTest, theBodyPadsItsOwnInputRow) {
    expectRows("MATCH (p:Person) "
               "CALL (p) { OPTIONAL MATCH (p)-[:KNOWS_WELL]->(f) WHERE f.name = 'Adam' "
               "RETURN f.name AS friend } "
               "RETURN p.name, friend",
               {{"Remy", "Adam"},
                {"Adam", "null"},
                {"Maxime", "null"},
                {"Luc", "null"},
                {"Martina", "null"},
                {"Suhas", "null"},
                {"Cyrus", "null"},
                {"Doruk", "null"}});
}

// Two patterns chained inside one body, the WHERE on the second
TEST_F(OptionalMatchWhereInSubqueryTest, twoPatternsChainedInsideOneBody) {
    expectRows("MATCH (p:Person) "
               "CALL (p) { OPTIONAL MATCH (p)-[:KNOWS_WELL]->(f) "
               "OPTIONAL MATCH (f)-[:INTERESTED_IN]->(i) WHERE i.name = 'Bio' "
               "RETURN f.name AS friend, i.name AS interest } "
               "RETURN p.name, friend, interest",
               {{"Remy", "Adam", "Bio"},
                {"Adam", "Remy", "null"},
                {"Maxime", "null", "null"},
                {"Luc", "null", "null"},
                {"Martina", "null", "null"},
                {"Suhas", "null", "null"},
                {"Cyrus", "null", "null"},
                {"Doruk", "null", "null"}});
}

// The pattern sits two subquery levels down
TEST_F(OptionalMatchWhereInSubqueryTest, thePatternSitsInABodyInsideABody) {
    expectRows("MATCH (p:Person) "
               "CALL (p) { CALL (p) { OPTIONAL MATCH (p)-[:KNOWS_WELL]->(f) "
               "WHERE f.name = 'Adam' RETURN f.name AS friend } RETURN friend } "
               "RETURN p.name, friend",
               {{"Remy", "Adam"},
                {"Adam", "null"},
                {"Maxime", "null"},
                {"Luc", "null"},
                {"Martina", "null"},
                {"Suhas", "null"},
                {"Cyrus", "null"},
                {"Doruk", "null"}});
}

// A pattern at each level: the outer one binds f, the body walks from it and nulls what its
// own WHERE cuts
TEST_F(OptionalMatchWhereInSubqueryTest, aPatternOutsideAndAPatternInsideTheBody) {
    expectRows("MATCH (p:Person) OPTIONAL MATCH (p)-[:KNOWS_WELL]->(f) "
               "CALL (f) { OPTIONAL MATCH (f)-[:INTERESTED_IN]->(i) WHERE i.name = 'Bio' "
               "RETURN i.name AS interest } "
               "RETURN p.name, f.name, interest",
               {{"Remy", "Adam", "Bio"},
                {"Adam", "Remy", "null"},
                {"Maxime", "null", "null"},
                {"Luc", "null", "null"},
                {"Martina", "null", "null"},
                {"Suhas", "null", "null"},
                {"Cyrus", "null", "null"},
                {"Doruk", "null", "null"}});
}

// The body hands back the nullable entity itself, and a pattern behind the CALL walks from
// it with a WHERE of its own
TEST_F(OptionalMatchWhereInSubqueryTest, aPatternBehindTheCallWalksFromTheNullableYield) {
    expectRows("MATCH (p:Person) "
               "CALL (p) { OPTIONAL MATCH (p)-[:KNOWS_WELL]->(f) WHERE f.name = 'Adam' "
               "RETURN f } "
               "OPTIONAL MATCH (f)-[:INTERESTED_IN]->(i) WHERE i.name = 'Bio' "
               "RETURN p.name, f.name, i.name",
               {{"Remy", "Adam", "Bio"},
                {"Adam", "null", "null"},
                {"Maxime", "null", "null"},
                {"Luc", "null", "null"},
                {"Martina", "null", "null"},
                {"Suhas", "null", "null"},
                {"Cyrus", "null", "null"},
                {"Doruk", "null", "null"}});
}

// A CALL importing both nullable columns the two patterns left, and matching from them
TEST_F(OptionalMatchWhereInSubqueryTest, aCallImportsWhatTwoPatternsNulled) {
    expectRows("MATCH (p:Person) OPTIONAL MATCH (p)-[:KNOWS_WELL]->(f) "
               "OPTIONAL MATCH (f)-[:INTERESTED_IN]->(i) WHERE i.name = 'Bio' "
               "CALL (i) { OPTIONAL MATCH (i)<-[:INTERESTED_IN]-(q:Person) "
               "WHERE q.name = 'Maxime' RETURN q.name AS fan } "
               "RETURN p.name, f.name, i.name, fan",
               {{"Remy", "Adam", "Bio", "Maxime"},
                {"Adam", "Remy", "null", "null"},
                {"Maxime", "null", "null", "null"},
                {"Luc", "null", "null", "null"},
                {"Martina", "null", "null", "null"},
                {"Suhas", "null", "null", "null"},
                {"Cyrus", "null", "null", "null"},
                {"Doruk", "null", "null", "null"}});
}

// An OPTIONAL CALL over a body that already pads: the body yields one row per input row, so
// the CALL's own padding never comes into it
TEST_F(OptionalMatchWhereInSubqueryTest, anOptionalCallOverABodyThatAlreadyPads) {
    expectRows("MATCH (p:Person) "
               "OPTIONAL CALL (p) { OPTIONAL MATCH (p)-[:KNOWS_WELL]->(f) "
               "WHERE f.name = 'Adam' RETURN f.name AS friend } "
               "RETURN p.name, friend",
               {{"Remy", "Adam"},
                {"Adam", "null"},
                {"Maxime", "null"},
                {"Luc", "null"},
                {"Martina", "null"},
                {"Suhas", "null"},
                {"Cyrus", "null"},
                {"Doruk", "null"}});
}

// Both paddings in one query: the pattern's WHERE nulls f inside the body, the mandatory
// MATCH behind it then drops that row, and the OPTIONAL CALL pads the input row back
TEST_F(OptionalMatchWhereInSubqueryTest, theBodyDropsWhatThePatternNulledAndTheCallPadsItBack) {
    expectRows("MATCH (p:Person) "
               "OPTIONAL CALL (p) { OPTIONAL MATCH (p)-[:KNOWS_WELL]->(f) "
               "WHERE f.name = 'Adam' MATCH (f)-[:INTERESTED_IN]->(i) "
               "RETURN i.name AS interest } "
               "RETURN p.name, interest",
               {{"Remy", "Bio"},
                {"Remy", "Cooking"},
                {"Adam", "null"},
                {"Maxime", "null"},
                {"Luc", "null"},
                {"Martina", "null"},
                {"Suhas", "null"},
                {"Cyrus", "null"},
                {"Doruk", "null"}});
}

// The same, with the inner OPTIONAL CALL padding first and the outer one seeing a body that
// always yields
TEST_F(OptionalMatchWhereInSubqueryTest, anOptionalCallInsideAnOptionalCall) {
    expectRows("MATCH (p:Person) "
               "OPTIONAL CALL (p) { OPTIONAL CALL (p) { "
               "OPTIONAL MATCH (p)-[:KNOWS_WELL]->(f) WHERE f.name = 'Adam' "
               "MATCH (f)-[:INTERESTED_IN]->(i) RETURN i.name AS interest } "
               "RETURN interest } "
               "RETURN p.name, interest",
               {{"Remy", "Bio"},
                {"Remy", "Cooking"},
                {"Adam", "null"},
                {"Maxime", "null"},
                {"Luc", "null"},
                {"Martina", "null"},
                {"Suhas", "null"},
                {"Cyrus", "null"},
                {"Doruk", "null"}});
}

// The body aggregates over what its WHERE kept, so the padded row counts 0
TEST_F(OptionalMatchWhereInSubqueryTest, theBodyCountsWhatItsWhereKept) {
    expectRows("MATCH (p:Person) "
               "CALL (p) { OPTIONAL MATCH (p)-[:INTERESTED_IN]->(i) WHERE i.isReal = true "
               "RETURN count(i) AS real } "
               "RETURN p.name, real",
               {{"Remy", "2"},
                {"Adam", "0"},
                {"Maxime", "0"},
                {"Luc", "2"},
                {"Martina", "0"},
                {"Suhas", "2"},
                {"Cyrus", "2"},
                {"Doruk", "1"}});
}

// A per-row body sorting and cutting what its WHERE kept: the rows it padded sort as one
// null and the LIMIT keeps it
TEST_F(OptionalMatchWhereInSubqueryTest, aPerRowBodySortsWhatItsWhereKept) {
    expectRows("MATCH (p:Person) "
               "CALL (p) { OPTIONAL MATCH (p)-[:INTERESTED_IN]->(i) WHERE i.isReal = true "
               "RETURN i.name AS interest ORDER BY interest LIMIT 1 } "
               "RETURN p.name, interest",
               {{"Remy", "Computers"},
                {"Adam", "null"},
                {"Maxime", "null"},
                {"Luc", "Animals"},
                {"Martina", "null"},
                {"Suhas", "Gym"},
                {"Cyrus", "Gym"},
                {"Doruk", "Gym"}});
}

// Three bodies deep, a pattern at every level, the WHERE on the middle and inner ones
TEST_F(OptionalMatchWhereInSubqueryTest, threeBodiesDeepWithAPatternAtEveryLevel) {
    expectRows("MATCH (p:Person {name: 'Remy'}) "
               "CALL (p) { OPTIONAL MATCH (p)-[:KNOWS_WELL]->(f) "
               "CALL (f) { OPTIONAL MATCH (f)-[:INTERESTED_IN]->(i) WHERE i.name = 'Bio' "
               "CALL (i) { OPTIONAL MATCH (i)<-[:INTERESTED_IN]-(q:Person) "
               "WHERE q.name = 'Maxime' RETURN q.name AS fan } "
               "RETURN i.name AS interest, fan } "
               "RETURN f.name AS friend, interest, fan } "
               "RETURN p.name, friend, interest, fan",
               {{"Remy", "Adam", "Bio", "Maxime"}});
}

// The body's WHERE reads the null the outer pattern imported into it: f.name is null on 6
// of the 8 rows, so the scan it filters matches nobody there
TEST_F(OptionalMatchWhereInSubqueryTest, theBodysWhereReadsTheImportedNull) {
    expectRows("MATCH (p:Person) OPTIONAL MATCH (p)-[:KNOWS_WELL]->(f) "
               "CALL (f) { OPTIONAL MATCH (x:Person) WHERE x.name = f.name "
               "RETURN x.name AS mirror } "
               "RETURN p.name, f.name, mirror",
               {{"Remy", "Adam", "Adam"},
                {"Adam", "Remy", "Remy"},
                {"Maxime", "null", "null"},
                {"Luc", "null", "null"},
                {"Martina", "null", "null"},
                {"Suhas", "null", "null"},
                {"Cyrus", "null", "null"},
                {"Doruk", "null", "null"}});
}

// An UNWIND ahead of the pattern gives the body two rows per person, and the pattern pads
// each of them on its own: Adam holds Bio and no Gym, Suhas the other way round
TEST_F(OptionalMatchWhereInSubqueryTest, anUnwindAheadOfThePatternInTheBody) {
    expectRows("MATCH (p:Person) "
               "CALL (p) { UNWIND ['Bio', 'Gym'] AS want "
               "OPTIONAL MATCH (p)-[:INTERESTED_IN]->(i) WHERE i.name = want "
               "RETURN want, i.name AS got } "
               "RETURN p.name, want, got",
               {{"Remy", "Bio", "null"},
                {"Remy", "Gym", "null"},
                {"Adam", "Bio", "Bio"},
                {"Adam", "Gym", "null"},
                {"Maxime", "Bio", "Bio"},
                {"Maxime", "Gym", "null"},
                {"Luc", "Bio", "null"},
                {"Luc", "Gym", "null"},
                {"Martina", "Bio", "null"},
                {"Martina", "Gym", "null"},
                {"Suhas", "Bio", "null"},
                {"Suhas", "Gym", "Gym"},
                {"Cyrus", "Bio", "null"},
                {"Cyrus", "Gym", "Gym"},
                {"Doruk", "Bio", "null"},
                {"Doruk", "Gym", "Gym"}});
}

// The pattern's WHERE reads a count the body aggregated ahead of it: Remy's three interests
// are the only ones over the bar, so his is the one friend that survives
TEST_F(OptionalMatchWhereInSubqueryTest, theWhereReadsAnAggregateTheBodyComputed) {
    expectRows("MATCH (p:Person) "
               "CALL (p) { MATCH (p)-[:INTERESTED_IN]->(x) WITH p, count(x) AS interests "
               "OPTIONAL MATCH (p)-[:KNOWS_WELL]->(f) WHERE interests > 2 "
               "RETURN f.name AS friend, interests } "
               "RETURN p.name, friend, interests",
               {{"Remy", "Adam", "3"},
                {"Adam", "null", "2"},
                {"Maxime", "null", "2"},
                {"Luc", "null", "2"},
                {"Martina", "null", "1"},
                {"Suhas", "null", "2"},
                {"Cyrus", "null", "2"},
                {"Doruk", "null", "1"}});
}

// Two bodies side by side, the second importing what the first yielded null
TEST_F(OptionalMatchWhereInSubqueryTest, twoSiblingBodiesEachWithAPattern) {
    expectRows("MATCH (p:Person) "
               "CALL (p) { OPTIONAL MATCH (p)-[:KNOWS_WELL]->(f) WHERE f.name = 'Adam' "
               "RETURN f } "
               "CALL (f) { OPTIONAL MATCH (f)-[:INTERESTED_IN]->(i) WHERE i.name = 'Bio' "
               "RETURN i.name AS interest } "
               "RETURN p.name, f.name, interest",
               {{"Remy", "Adam", "Bio"},
                {"Adam", "null", "null"},
                {"Maxime", "null", "null"},
                {"Luc", "null", "null"},
                {"Martina", "null", "null"},
                {"Suhas", "null", "null"},
                {"Cyrus", "null", "null"},
                {"Doruk", "null", "null"}});
}

// A pattern ahead of the body, one inside it and one behind it, each with a WHERE
TEST_F(OptionalMatchWhereInSubqueryTest, aPatternBeforeTheBodyAndOneBehindIt) {
    expectRows("MATCH (p:Person) OPTIONAL MATCH (p)-[:KNOWS_WELL]->(f) "
               "CALL (f) { OPTIONAL MATCH (f)-[:INTERESTED_IN]->(i) WHERE i.name = 'Bio' "
               "RETURN i } "
               "OPTIONAL MATCH (i)<-[:INTERESTED_IN]-(q:Person) WHERE q.name = 'Maxime' "
               "RETURN p.name, f.name, i.name, q.name",
               {{"Remy", "Adam", "Bio", "Maxime"},
                {"Adam", "Remy", "null", "null"},
                {"Maxime", "null", "null", "null"},
                {"Luc", "null", "null", "null"},
                {"Martina", "null", "null", "null"},
                {"Suhas", "null", "null", "null"},
                {"Cyrus", "null", "null", "null"},
                {"Doruk", "null", "null", "null"}});
}

// A procedure's rows drive the body, and the pattern inside it pads them
TEST_F(OptionalMatchWhereInSubqueryTest, aProcedureDrivesTheBody) {
    expectRows("CALL db.getNodes([0, 1]) YIELD id AS p "
               "CALL (p) { OPTIONAL MATCH (p)-[:KNOWS_WELL]->(f) WHERE f.name = 'Adam' "
               "RETURN f.name AS friend } "
               "RETURN p.name, friend",
               {{"Remy", "Adam"},
                {"Adam", "null"}});
}

// DISTINCT inside the body reads the null the pattern wrote as a value of its own
TEST_F(OptionalMatchWhereInSubqueryTest, distinctInsideTheBody) {
    expectRows("MATCH (p:Person) "
               "CALL (p) { OPTIONAL MATCH (p)-[:INTERESTED_IN]->(i) WHERE i.isReal = true "
               "RETURN DISTINCT i.isReal AS real } "
               "RETURN p.name, real",
               {{"Remy", "true"},
                {"Adam", "null"},
                {"Maxime", "null"},
                {"Luc", "true"},
                {"Martina", "null"},
                {"Suhas", "true"},
                {"Cyrus", "true"},
                {"Doruk", "true"}});
}

// A filter inside the body drops the row the pattern padded, and the OPTIONAL CALL pads the
// input row back: the null goes and comes again
TEST_F(OptionalMatchWhereInSubqueryTest, anInBodyFilterDropsWhatTheOptionalCallThenPads) {
    expectRows("MATCH (p:Person) "
               "OPTIONAL CALL (p) { OPTIONAL MATCH (p)-[:KNOWS_WELL]->(f) "
               "WHERE f.name = 'Adam' WITH f WHERE f IS NOT NULL RETURN f.name AS friend } "
               "RETURN p.name, friend",
               {{"Remy", "Adam"},
                {"Adam", "null"},
                {"Maxime", "null"},
                {"Luc", "null"},
                {"Martina", "null"},
                {"Suhas", "null"},
                {"Cyrus", "null"},
                {"Doruk", "null"}});
}

// The body collects what its WHERE kept, so the four persons it padded collect an empty
// list and the UNWIND behind the CALL drops them
TEST_F(OptionalMatchWhereInSubqueryTest, collectInTheBodyThenUnwindOutside) {
    expectRows("MATCH (p:Person) "
               "CALL (p) { OPTIONAL MATCH (p)-[:INTERESTED_IN]->(i) WHERE i.isReal = true "
               "RETURN collect(i.name) AS reals } "
               "UNWIND reals AS real RETURN p.name, real",
               {{"Remy", "Ghosts"},
                {"Remy", "Computers"},
                {"Luc", "Animals"},
                {"Luc", "Computers"},
                {"Suhas", "Gym"},
                {"Suhas", "JiuJitsu"},
                {"Cyrus", "Gym"},
                {"Cyrus", "Travel"},
                {"Doruk", "Gym"}});
}

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

// CALL { ... } subqueries whose body aggregates, sorts, cuts or dedups: each of those
// applies to the rows of one input row at a time
class CallSubqueryPerRowTest : public TuringTest {
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

        EXPECT_EQ(actual, sortedExpected) << "query: " << query << "\ngot:\n" << actualText;
    }

    void expectRowCount(std::string_view query, size_t expected) {
        RowSink sink;
        const QueryStatus status = runQuery(query, &sink);
        ASSERT_TRUE(status.isOk()) << "query: " << query << "\nerror: " << status.getError();

        std::string actualText;
        describeRows(sink.rows(), actualText);

        EXPECT_EQ(sink.rows().size(), expected) << "query: " << query << "\ngot:\n" << actualText;
    }

    const std::string _graphName = "simpledb";
    std::unique_ptr<TuringTestEnv> _env;
    std::unique_ptr<QueryInterpreterV3> _interpreter;
};

// The first interest by name of each Person: Computers ahead of Eighties and Ghosts for
// Remy, Gym ahead of JiuJitsu and Travel
TEST_F(CallSubqueryPerRowTest, ordersAndCutsPerInputRow) {
    expectRows("MATCH (p:Person) "
               "CALL (p) { MATCH (p)-[:INTERESTED_IN]->(i) RETURN i ORDER BY i.name LIMIT 1 } "
               "RETURN p.name, i.name",
               {{"Remy", "Computers"},
                {"Adam", "Bio"},
                {"Maxime", "Bio"},
                {"Luc", "Animals"},
                {"Martina", "Cooking"},
                {"Suhas", "Gym"},
                {"Cyrus", "Gym"},
                {"Doruk", "Gym"}});
}

// Two per Person: every edge but Remy's Ghosts, which comes third by name
TEST_F(CallSubqueryPerRowTest, takesTheTopTwoPerInputRow) {
    expectRows("MATCH (p:Person) "
               "CALL (p) { MATCH (p)-[:INTERESTED_IN]->(i) RETURN i.name AS interest ORDER BY interest LIMIT 2 } "
               "RETURN p.name, interest",
               {{"Remy", "Computers"},
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

// Everything but each Person's first interest: Martina and Doruk have one and keep none
TEST_F(CallSubqueryPerRowTest, skipsPerInputRow) {
    expectRows("MATCH (p:Person) "
               "CALL (p) { MATCH (p)-[:INTERESTED_IN]->(i) RETURN i.name AS interest ORDER BY interest SKIP 1 } "
               "RETURN p.name, interest",
               {{"Remy", "Eighties"},
                {"Remy", "Ghosts"},
                {"Adam", "Cooking"},
                {"Maxime", "Padel"},
                {"Luc", "Computers"},
                {"Suhas", "JiuJitsu"},
                {"Cyrus", "Travel"}});
}

// A keyless count over no rows is one row of 0, so the six Persons knowing nobody stay
TEST_F(CallSubqueryPerRowTest, countsPerInputRowIncludingZero) {
    expectRows("MATCH (p:Person) "
               "CALL (p) { MATCH (p)-[:KNOWS_WELL]->(k) RETURN count(k) AS known } "
               "RETURN p.name, known",
               {{"Remy", "1"},
                {"Adam", "1"},
                {"Maxime", "0"},
                {"Luc", "0"},
                {"Martina", "0"},
                {"Suhas", "0"},
                {"Cyrus", "0"},
                {"Doruk", "0"}});
}

TEST_F(CallSubqueryPerRowTest, countsTheInterestsOfEachPerson) {
    expectRows("MATCH (p:Person) "
               "CALL (p) { MATCH (p)-[:INTERESTED_IN]->(i) RETURN count(i) AS interests } "
               "RETURN p.name, interests",
               {{"Remy", "3"},
                {"Adam", "2"},
                {"Maxime", "2"},
                {"Luc", "2"},
                {"Martina", "1"},
                {"Suhas", "2"},
                {"Cyrus", "2"},
                {"Doruk", "1"}});
}

// The Persons sharing an interest with each Person, themselves included, each once:
// Remy reaches himself through three interests and Luc through Computers
TEST_F(CallSubqueryPerRowTest, dedupsPerInputRow) {
    expectRows("MATCH (p:Person) "
               "CALL (p) { MATCH (p)-[:INTERESTED_IN]->(i)<-[:INTERESTED_IN]-(q:Person) RETURN DISTINCT q } "
               "RETURN p.name, count(q)",
               {{"Remy", "2"},
                {"Adam", "3"},
                {"Maxime", "2"},
                {"Luc", "2"},
                {"Martina", "2"},
                {"Suhas", "3"},
                {"Cyrus", "3"},
                {"Doruk", "3"}});
}

// A grouped count inside the body groups the rows of one input row: how many interests
// each Person shares with each other Person
TEST_F(CallSubqueryPerRowTest, groupsPerInputRow) {
    expectRows("MATCH (p:Person) "
               "CALL (p) { "
               "  MATCH (p)-[:INTERESTED_IN]->(i)<-[:INTERESTED_IN]-(q:Person) "
               "  RETURN q.name AS other, count(i) AS shared "
               "} "
               "RETURN p.name, other, shared",
               {{"Remy", "Remy", "3"},
                {"Remy", "Luc", "1"},
                {"Adam", "Adam", "2"},
                {"Adam", "Maxime", "1"},
                {"Adam", "Martina", "1"},
                {"Maxime", "Maxime", "2"},
                {"Maxime", "Adam", "1"},
                {"Luc", "Luc", "2"},
                {"Luc", "Remy", "1"},
                {"Martina", "Martina", "1"},
                {"Martina", "Adam", "1"},
                {"Suhas", "Suhas", "2"},
                {"Suhas", "Cyrus", "1"},
                {"Suhas", "Doruk", "1"},
                {"Cyrus", "Cyrus", "2"},
                {"Cyrus", "Suhas", "1"},
                {"Cyrus", "Doruk", "1"},
                {"Doruk", "Doruk", "1"},
                {"Doruk", "Suhas", "1"},
                {"Doruk", "Cyrus", "1"}});
}

// Two equal input rows are two rows out, not one
TEST_F(CallSubqueryPerRowTest, keepsDuplicateInputRowsApart) {
    expectRows("UNWIND [1, 2] AS k "
               "MATCH (p:Person {name: 'Remy'}) "
               "CALL (p) { MATCH (p)-->(x) RETURN count(x) AS c } "
               "RETURN k, c",
               {{"1", "4"}, {"2", "4"}});
}

// A cut on a WITH inside the body is per input row too: the fans of each Person's first
// interest by name
TEST_F(CallSubqueryPerRowTest, cutsABarrierInsideTheBodyPerInputRow) {
    expectRows("MATCH (p:Person) "
               "CALL (p) { "
               "  MATCH (p)-[:INTERESTED_IN]->(i) WITH i ORDER BY i.name LIMIT 1 "
               "  MATCH (i)<-[:INTERESTED_IN]-(q:Person) RETURN q "
               "} "
               "RETURN p.name, count(q)",
               {{"Remy", "2"},
                {"Adam", "2"},
                {"Maxime", "2"},
                {"Luc", "1"},
                {"Martina", "2"},
                {"Suhas", "3"},
                {"Cyrus", "3"},
                {"Doruk", "3"}});
}

// The inner body takes one fan per interest, so the outer count is the interest count
TEST_F(CallSubqueryPerRowTest, nestsAPerRowBodyInAPerRowBody) {
    expectRows("MATCH (p:Person) "
               "CALL (p) { "
               "  MATCH (p)-[:INTERESTED_IN]->(i) "
               "  CALL (i) { MATCH (i)<-[:INTERESTED_IN]-(q:Person) RETURN q ORDER BY q.name LIMIT 1 } "
               "  RETURN count(q) AS c "
               "} "
               "RETURN p.name, c",
               {{"Remy", "3"},
                {"Adam", "2"},
                {"Maxime", "2"},
                {"Luc", "2"},
                {"Martina", "1"},
                {"Suhas", "2"},
                {"Cyrus", "2"},
                {"Doruk", "1"}});
}

TEST_F(CallSubqueryPerRowTest, aggregatesAnUncorrelatedBodyOncePerInputRow) {
    expectRows("MATCH (p:Person) "
               "CALL { MATCH (i:Interest) RETURN count(i) AS interests } "
               "RETURN p.name, interests",
               {{"Remy", "10"},
                {"Adam", "10"},
                {"Maxime", "10"},
                {"Luc", "10"},
                {"Martina", "10"},
                {"Suhas", "10"},
                {"Cyrus", "10"},
                {"Doruk", "10"}});
}

TEST_F(CallSubqueryPerRowTest, opensTheQueryWithAnAggregatingBody) {
    expectRows("CALL { MATCH (i:Interest) RETURN count(i) AS interests } RETURN interests", {{"10"}});
}

TEST_F(CallSubqueryPerRowTest, aLimitAfterTheCallBoundsItsRows) {
    expectRowCount("MATCH (p:Person) "
                   "CALL (p) { MATCH (p)-->(x) RETURN count(x) AS c } "
                   "RETURN c LIMIT 3",
                   3);
}

// The import rides the cut the barrier applies, so it is still readable below one
TEST_F(CallSubqueryPerRowTest, anImportSurvivesACuttingBarrier) {
    expectRows("MATCH (p:Person) "
               "CALL (p) { "
               "  MATCH (p)-[:INTERESTED_IN]->(i) WITH i ORDER BY i.name LIMIT 1 "
               "  RETURN p.name AS person, i.name AS interest "
               "} "
               "RETURN person, interest",
               {{"Remy", "Computers"},
                {"Adam", "Bio"},
                {"Maxime", "Bio"},
                {"Luc", "Animals"},
                {"Martina", "Cooking"},
                {"Suhas", "Gym"},
                {"Cyrus", "Gym"},
                {"Doruk", "Gym"}});
}

// A dedup reads the import with the rest of the row, and it holds one value per input row,
// so it tells none of them apart: Remy reaches Luc through Computers alone
TEST_F(CallSubqueryPerRowTest, anImportSurvivesADedupingBarrier) {
    expectRows("MATCH (p:Person {name: 'Remy'}) "
               "CALL (p) { "
               "  MATCH (p)-[:INTERESTED_IN]->(i)<-[:INTERESTED_IN]-(q:Person) WITH DISTINCT q "
               "  RETURN p.name AS person, q.name AS sharer "
               "} "
               "RETURN person, sharer",
               {{"Remy", "Remy"}, {"Remy", "Luc"}});
}

// A grouped reduction keeps its keys, and the import joins them: it holds one value per
// input row, so the groups are the ones the query's own key makes
TEST_F(CallSubqueryPerRowTest, anImportSurvivesAGroupedBarrier) {
    expectRows("MATCH (p:Person {name: 'Remy'}) "
               "CALL (p) { "
               "  MATCH (p)-[:INTERESTED_IN]->(i)<-[:INTERESTED_IN]-(q:Person) "
               "  WITH q, count(i) AS shared "
               "  RETURN p.name AS person, q.name AS sharer, shared "
               "} "
               "RETURN person, sharer, shared",
               {{"Remy", "Remy", "3"}, {"Remy", "Luc", "1"}});
}

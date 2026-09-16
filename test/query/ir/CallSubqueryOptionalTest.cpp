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

// OPTIONAL CALL { ... }: an input row the body yields nothing for is kept once, with the
// body's columns null
class CallSubqueryOptionalTest : public TuringTest {
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

    const std::string _graphName = "simpledb";
    std::unique_ptr<TuringTestEnv> _env;
    std::unique_ptr<QueryInterpreterV3> _interpreter;
};

// Only Remy and Adam know anybody well; the six others come back with a null
TEST_F(CallSubqueryOptionalTest, padsTheRowsACarryingBodyYieldsNothingFor) {
    expectRows("MATCH (p:Person) "
               "OPTIONAL CALL (p) { MATCH (p)-[:KNOWS_WELL]->(k) RETURN k } "
               "RETURN p.name, k.name",
               {{"Remy", "Adam"},
                {"Adam", "Remy"},
                {"Maxime", "null"},
                {"Luc", "null"},
                {"Martina", "null"},
                {"Suhas", "null"},
                {"Cyrus", "null"},
                {"Doruk", "null"}});
}

TEST_F(CallSubqueryOptionalTest, padsTheRowsAPerRowBodyYieldsNothingFor) {
    expectRows("MATCH (p:Person) "
               "OPTIONAL CALL (p) { MATCH (p)-[:KNOWS_WELL]->(k) RETURN k.name AS known ORDER BY known LIMIT 1 } "
               "RETURN p.name, known",
               {{"Remy", "Adam"},
                {"Adam", "Remy"},
                {"Maxime", "null"},
                {"Luc", "null"},
                {"Martina", "null"},
                {"Suhas", "null"},
                {"Cyrus", "null"},
                {"Doruk", "null"}});
}

// A keyless count yields a row for every input row, so nothing is padded
TEST_F(CallSubqueryOptionalTest, anAggregatingBodyLeavesNothingToPad) {
    expectRows("MATCH (p:Person) "
               "OPTIONAL CALL (p) { MATCH (p)-[:KNOWS_WELL]->(k) RETURN count(k) AS known } "
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

// The tag rides past the barrier in the body: Remy reaches Adam's two interests, Adam
// Remy's three, and the six others are padded with one null the count ignores
TEST_F(CallSubqueryOptionalTest, carriesTheTagPastABarrierInTheBody) {
    expectRows("MATCH (p:Person) "
               "OPTIONAL CALL (p) { "
               "  MATCH (p)-[:KNOWS_WELL]->(k) WITH k MATCH (k)-[:INTERESTED_IN]->(i) RETURN i.name AS interest "
               "} "
               "RETURN p.name, count(interest)",
               {{"Remy", "2"},
                {"Adam", "3"},
                {"Maxime", "0"},
                {"Luc", "0"},
                {"Martina", "0"},
                {"Suhas", "0"},
                {"Cyrus", "0"},
                {"Doruk", "0"}});
}

TEST_F(CallSubqueryOptionalTest, dedupsPerInputRowAndPadsTheRest) {
    expectRows("MATCH (p:Person) "
               "OPTIONAL CALL (p) { "
               "  MATCH (p)-[:KNOWS_WELL]->(k)-[:INTERESTED_IN]->(i) RETURN DISTINCT i.name AS interest "
               "} "
               "RETURN p.name, interest",
               {{"Remy", "Bio"},
                {"Remy", "Cooking"},
                {"Adam", "Ghosts"},
                {"Adam", "Computers"},
                {"Adam", "Eighties"},
                {"Maxime", "null"},
                {"Luc", "null"},
                {"Martina", "null"},
                {"Suhas", "null"},
                {"Cyrus", "null"},
                {"Doruk", "null"}});
}

// A query opening on OPTIONAL CALL joins onto the single empty row, which comes back
// padded when the body matches nothing
TEST_F(CallSubqueryOptionalTest, opensTheQueryOnAnEmptyBody) {
    expectRows("OPTIONAL CALL { MATCH (n:Nobody) RETURN n } RETURN n", {{"null"}});
}

TEST_F(CallSubqueryOptionalTest, keepsTheRowsTheBodyYieldsFor) {
    expectRows("MATCH (p:Person) "
               "OPTIONAL CALL (p) { MATCH (p)-[:INTERESTED_IN]->(i) RETURN i } "
               "RETURN count(i)",
               {{"15"}});
}

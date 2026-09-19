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

// OPTIONAL CALL { ... } over a body returning a boolean. A label test lowers to a mask
// column, which has no null to pad with, so the drain has to read it as a nullable value
// column before it collects it
class CallSubqueryOptionalBoolTest : public TuringTest {
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

// Remy and Adam know each other well and are both Founders; the six others match no
// KNOWS_WELL edge, so there is no k to test the label of
TEST_F(CallSubqueryOptionalBoolTest, padsALabelTestWithNull) {
    expectRows("MATCH (p:Person) "
               "OPTIONAL CALL (p) { MATCH (p)-[:KNOWS_WELL]->(k) RETURN k:Founder AS founder } "
               "RETURN p.name, founder",
               {{"Remy", "true"},
                {"Adam", "true"},
                {"Maxime", "null"},
                {"Luc", "null"},
                {"Martina", "null"},
                {"Suhas", "null"},
                {"Cyrus", "null"},
                {"Doruk", "null"}});
}

// Neither Remy nor Adam is in Sales, so the two rows the body yields are false and a
// padded row is only distinguishable from them if it comes back null
TEST_F(CallSubqueryOptionalBoolTest, tellsAPaddedRowFromAFalseOne) {
    expectRows("MATCH (p:Person) "
               "OPTIONAL CALL (p) { MATCH (p)-[:KNOWS_WELL]->(k) RETURN k:Sales AS seller } "
               "RETURN p.name, seller",
               {{"Remy", "false"},
                {"Adam", "false"},
                {"Maxime", "null"},
                {"Luc", "null"},
                {"Martina", "null"},
                {"Suhas", "null"},
                {"Cyrus", "null"},
                {"Doruk", "null"}});
}

TEST_F(CallSubqueryOptionalBoolTest, countsThePaddedRowsAsNull) {
    expectRows("MATCH (p:Person) "
               "OPTIONAL CALL (p) { MATCH (p)-[:KNOWS_WELL]->(k) RETURN k:Founder AS founder } "
               "WITH p, founder WHERE founder IS NULL "
               "RETURN count(p)",
               {{"6"}});
}

// A grouped aggregate yields no row for a Person with no KNOWS_WELL edge, and the tally
// beside the key has no more of a null to pad with than the mask above
TEST_F(CallSubqueryOptionalBoolTest, padsATallyWithNullRatherThanZero) {
    expectRows("MATCH (p:Person) "
               "OPTIONAL CALL (p) { MATCH (p)-[:KNOWS_WELL]->(k) RETURN k.name AS known, count(k) AS tally } "
               "RETURN p.name, known, tally",
               {{"Remy", "Adam", "1"},
                {"Adam", "Remy", "1"},
                {"Maxime", "null", "null"},
                {"Luc", "null", "null"},
                {"Martina", "null", "null"},
                {"Suhas", "null", "null"},
                {"Cyrus", "null", "null"},
                {"Doruk", "null", "null"}});
}

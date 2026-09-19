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
class CallSubqueryStepBlockTest : public TuringTest {
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

// The names are unique, so the join pairs each Person with itself and the body walks the
// 15 INTERESTED_IN edges once
TEST_F(CallSubqueryStepBlockTest, importsBothSidesOfAHashJoin) {
    expectRows("MATCH (a:Person), (b:Person) WHERE a.name = b.name "
               "CALL (a, b) { MATCH (a)-[:INTERESTED_IN]->(x) RETURN x } RETURN count(x)",
               {{"15"}});
}

// One count per pair the join made
TEST_F(CallSubqueryStepBlockTest, importsBothSidesOfAHashJoinIntoAPerRowBody) {
    expectRows("MATCH (a:Person), (b:Person) WHERE a.name = b.name "
               "CALL (a, b) { MATCH (a)-[:INTERESTED_IN]->(x) RETURN count(x) AS c } RETURN count(c)",
               {{"8"}});
}

// 64 pairs, each carrying the 15 edges of its own a: 8 * 15
TEST_F(CallSubqueryStepBlockTest, importsBothSidesOfACrossProduct) {
    expectRows("MATCH (a:Person), (b:Person) "
               "CALL (a, b) { MATCH (a)-[:INTERESTED_IN]->(x) RETURN x } RETURN count(x)",
               {{"120"}});
}

// Only Remy and Adam know anyone well, and they reach each other's 2 and 3 interests
TEST_F(CallSubqueryStepBlockTest, importsAColumnAnOptionalMatchBound) {
    expectRows("MATCH (p:Person) OPTIONAL MATCH (p)-[:KNOWS_WELL]->(k) "
               "CALL (p, k) { MATCH (k)-[:INTERESTED_IN]->(x) RETURN x } RETURN count(x)",
               {{"5"}});
}

TEST_F(CallSubqueryStepBlockTest, importsAColumnAnOptionalMatchBoundIntoAPerRowBody) {
    expectRows("MATCH (p:Person) OPTIONAL MATCH (p)-[:KNOWS_WELL]->(k) "
               "CALL (p, k) { MATCH (k)-[:INTERESTED_IN]->(x) RETURN x ORDER BY x LIMIT 1 } "
               "RETURN count(x)",
               {{"2"}});
}

// A bare LIMIT in a per-row body holds its handle inside the loop over the input rows, so
// the walk that hands the handle to producing loops stops at the body's boundary. Remy has
// 3 interests and the 7 others 2 or fewer: 15 edges less the one the cut drops
TEST_F(CallSubqueryStepBlockTest, cutsPerInputRowUnderABareLimit) {
    expectRows("MATCH (p:Person) CALL (p) { MATCH (p)-[:INTERESTED_IN]->(i) RETURN i LIMIT 2 } "
               "RETURN count(i)",
               {{"14"}});
}

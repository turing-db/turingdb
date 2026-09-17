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

// A barrier of a subquery body carries the imports beside its own items, so its ORDER BY
// may order by one: an import holds a single value per invocation, which is a grouping key
// in the strongest sense, and ordering by it reorders nothing
class CallSubqueryImportOrderByTest : public TuringTest {
protected:
    void initialize() override {
        _env = TuringTestEnv::create(fs::Path {_outDir} / "turing");
        _interpreter = std::make_unique<QueryInterpreterV3>(&_env->getSystemManager());

        SystemAccessor system = _env->getSystemManager().accessUnique();
        Graph* graph = system.createGraph(_graphName);
        SimpleGraph::createSimpleGraph(graph);
    }

    void expectRows(std::string_view query, const Rows& expected) {
        RowSink sink;
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

        std::string actualText;
        describeRows(actual, actualText);

        EXPECT_EQ(actual, sortedExpected) << "query: " << query << "\ngot:\n" << actualText;
    }

    const std::string _graphName = "simpledb";
    std::unique_ptr<TuringTestEnv> _env;
    std::unique_ptr<QueryInterpreterV3> _interpreter;
};

// The rows anImportSurvivesAGroupedBarrier reports without the ORDER BY
TEST_F(CallSubqueryImportOrderByTest, ordersAGroupedBarrierByAnImport) {
    expectRows("MATCH (p:Person {name: 'Remy'}) "
               "CALL (p) { "
               "  MATCH (p)-[:INTERESTED_IN]->(i)<-[:INTERESTED_IN]-(q:Person) "
               "  WITH q, count(i) AS shared ORDER BY p.name "
               "  RETURN q.name AS sharer, shared "
               "} "
               "RETURN sharer, shared",
               {{"Remy", "3"}, {"Luc", "1"}});
}

// The rows anImportSurvivesADedupingBarrier reports without the ORDER BY
TEST_F(CallSubqueryImportOrderByTest, ordersADedupingBarrierByAnImport) {
    expectRows("MATCH (p:Person {name: 'Remy'}) "
               "CALL (p) { "
               "  MATCH (p)-[:INTERESTED_IN]->(i)<-[:INTERESTED_IN]-(q:Person) "
               "  WITH DISTINCT q ORDER BY p.name "
               "  RETURN q.name AS sharer "
               "} "
               "RETURN sharer",
               {{"Remy"}, {"Luc"}});
}

// One value per invocation orders nothing: the 15 interests come out as they would with no
// ORDER BY at all
TEST_F(CallSubqueryImportOrderByTest, leavesTheRowsOfEveryInvocationAlone) {
    expectRows("MATCH (p:Person) "
               "CALL (p) { "
               "  MATCH (p)-[:INTERESTED_IN]->(i) "
               "  WITH i.name AS interest, count(i) AS n ORDER BY p.name "
               "  RETURN interest "
               "} "
               "RETURN p.name, interest",
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

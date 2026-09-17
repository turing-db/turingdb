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

// A reduction inside a CALL subquery body whose input is a constant: the body reads no
// relation of its own, so the value is folded where the constant is bound - above the
// body, at function scope - while the body's dataflow is rooted in the step it runs over
class CallSubqueryConstantAggregateTest : public TuringTest {
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

// RETURN count(42) is one row holding 1, as it is without a MATCH around it
TEST_F(CallSubqueryConstantAggregateTest, countsASpelledOutConstantPerInputRow) {
    expectRows("MATCH (p:Person) CALL (p) { RETURN count(42) AS c } RETURN p.name, c",
               {{"Remy", "1"},
                {"Adam", "1"},
                {"Maxime", "1"},
                {"Luc", "1"},
                {"Martina", "1"},
                {"Suhas", "1"},
                {"Cyrus", "1"},
                {"Doruk", "1"}});
}

// An imported constant stands for one value per row, so the reduction over it is that
// value
TEST_F(CallSubqueryConstantAggregateTest, sumsAnImportedConstantPerInputRow) {
    expectRows("MATCH (p:Person) WITH p, 2 AS two "
               "CALL (p, two) { RETURN sum(two) AS s } "
               "RETURN p.name, s",
               {{"Remy", "2"},
                {"Adam", "2"},
                {"Maxime", "2"},
                {"Luc", "2"},
                {"Martina", "2"},
                {"Suhas", "2"},
                {"Cyrus", "2"},
                {"Doruk", "2"}});
}

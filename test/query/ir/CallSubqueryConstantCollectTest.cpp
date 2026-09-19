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

// A collect inside a CALL subquery body whose input is a constant: the column it folds is
// bound above the body, at function scope, while the accumulator is hoisted to the row
// loop the body is rooted in, so the update has to sit no higher than that loop
class CallSubqueryConstantCollectTest : public TuringTest {
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

// collect(42) is one list holding one 42, per input row
TEST_F(CallSubqueryConstantCollectTest, collectsASpelledOutConstantPerInputRow) {
    expectRows("MATCH (p:Person) CALL (p) { RETURN collect(42) AS xs } RETURN p.name, xs",
               {{"Remy", "[42]"},
                {"Adam", "[42]"},
                {"Maxime", "[42]"},
                {"Luc", "[42]"},
                {"Martina", "[42]"},
                {"Suhas", "[42]"},
                {"Cyrus", "[42]"},
                {"Doruk", "[42]"}});
}

// The key is a constant too, so the collect groups on a column bound above the row loop
TEST_F(CallSubqueryConstantCollectTest, groupsOnAConstantKey) {
    expectRows("MATCH (p:Person) CALL (p) { RETURN 42 AS k, collect(7) AS xs } RETURN p.name, k, xs",
               {{"Remy", "42", "[7]"},
                {"Adam", "42", "[7]"},
                {"Maxime", "42", "[7]"},
                {"Luc", "42", "[7]"},
                {"Martina", "42", "[7]"},
                {"Suhas", "42", "[7]"},
                {"Cyrus", "42", "[7]"},
                {"Doruk", "42", "[7]"}});
}

TEST_F(CallSubqueryConstantCollectTest, collectsAnImportedConstantPerInputRow) {
    expectRows("MATCH (p:Person) WITH p, 2 AS two "
               "CALL (p, two) { RETURN collect(two) AS xs } "
               "RETURN p.name, xs",
               {{"Remy", "[2]"},
                {"Adam", "[2]"},
                {"Maxime", "[2]"},
                {"Luc", "[2]"},
                {"Martina", "[2]"},
                {"Suhas", "[2]"},
                {"Cyrus", "[2]"},
                {"Doruk", "[2]"}});
}

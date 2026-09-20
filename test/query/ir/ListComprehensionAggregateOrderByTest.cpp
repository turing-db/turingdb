#include <gtest/gtest.h>

#include <algorithm>
#include <memory>
#include <string>
#include <string_view>

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

// ORDER BY a key holding a list comprehension, over an aggregating projection. The
// element the comprehension binds is bound per element inside the key, so it is
// group-wise by construction and the key is one the grouped rows can be ordered on -
// as `ORDER BY n.age + 1` already is over the same projection.
class ListComprehensionAggregateOrderByTest : public TuringTest {
protected:
    void initialize() override {
        _env = TuringTestEnv::create(fs::Path {_outDir} / "turing");
        _interpreter = std::make_unique<QueryInterpreterV3>(&_env->getSystemManager());

        SystemAccessor system = _env->getSystemManager().accessUnique();
        Graph* graph = system.createGraph(_graphName);
        SimpleGraph::createSimpleGraph(graph);
    }

    void runQuery(std::string_view query, RowSink& sink) {
        QueryStatus status;
        _interpreter->execute(status,
                              query,
                              _graphName,
                              CommitHash::head(),
                              ChangeID::head(),
                              &_env->getMem(),
                              &sink);
        ASSERT_TRUE(status.isOk()) << "query: " << query << "\nerror: " << status.getError();
    }

    void expectRows(std::string_view query, const Rows& expected) {
        RowSink sink;
        runQuery(query, sink);

        Rows actual;
        sink.sortedRows(actual);

        Rows sortedExpected = expected;
        std::sort(sortedExpected.begin(), sortedExpected.end());

        std::string actualText;
        describeRows(actual, actualText);

        EXPECT_EQ(actual, sortedExpected) << "query: " << query << "\ngot:\n" << actualText;
    }

    void expectRowsInOrder(std::string_view query, const Rows& expected) {
        RowSink sink;
        runQuery(query, sink);

        std::string actualText;
        describeRows(sink.rows(), actualText);

        EXPECT_EQ(sink.rows(), expected) << "query: " << query << "\ngot:\n" << actualText;
    }

    const std::string _graphName = "simpledb";
    std::unique_ptr<TuringTestEnv> _env;
    std::unique_ptr<QueryInterpreterV3> _interpreter;
};

TEST_F(ListComprehensionAggregateOrderByTest, ordersByAKeyOverConstantElements) {
    expectRows("MATCH (n:Person) RETURN n.name, count(*) ORDER BY size([x IN [1,2] | x + 1])",
               {{"Remy", "1"},
                {"Adam", "1"},
                {"Maxime", "1"},
                {"Luc", "1"},
                {"Martina", "1"},
                {"Suhas", "1"},
                {"Cyrus", "1"},
                {"Doruk", "1"}});
}

TEST_F(ListComprehensionAggregateOrderByTest, ordersByAKeyReadingAGroupingKey) {
    expectRows("MATCH (n:Person) RETURN n.age, count(*) "
               "ORDER BY size([x IN [1,2] WHERE x + n.age > 33])",
               {{"32", "2"},
                {"null", "6"}});
}

// Remy is the one group whose key is 1, every other group's is 0, so the descending
// order puts it first whatever the ties behind it do.
TEST_F(ListComprehensionAggregateOrderByTest, ordersTheGroupsByTheKeyItBuilt) {
    expectRowsInOrder("MATCH (n:Person) RETURN n.name, count(*) "
                      "ORDER BY size([x IN [n.name] WHERE x = 'Remy']) DESC LIMIT 1",
                      {{"Remy", "1"}});
}

// The same projection ordered by a plain expression over the grouping key, which the
// engine already accepts: the comprehension is the only difference above.
TEST_F(ListComprehensionAggregateOrderByTest, ordersByAPlainExpressionOverAGroupingKey) {
    expectRows("MATCH (n:Person) RETURN n.age, count(*) ORDER BY n.age + 1",
               {{"32", "2"},
                {"null", "6"}});
}

int main(int argc, char** argv) {
    return turing::test::turingTestMain(argc, argv);
}

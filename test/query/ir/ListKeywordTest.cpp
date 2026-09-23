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

// `list` is a variable name like any other. The parser holds LIST for the keyword of
// LIST GRAPH, and the manual's own list examples name their variable `list`.
class ListKeywordTest : public TuringTest {
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

    void expectError(std::string_view query, std::string_view message) {
        RowSink sink;
        QueryStatus status;
        _interpreter->execute(status,
                              query,
                              _graphName,
                              CommitHash::head(),
                              ChangeID::head(),
                              &_env->getMem(),
                              &sink);

        ASSERT_FALSE(status.isOk()) << "query: " << query;
        EXPECT_NE(status.getError().find(message), std::string::npos)
            << "query: " << query << "\nerror: " << status.getError();
    }

    const std::string _graphName = "simpledb";
    std::unique_ptr<TuringTestEnv> _env;
    std::unique_ptr<QueryInterpreterV3> _interpreter;
};

TEST_F(ListKeywordTest, bindsAListToTheNameList) {
    expectRows("WITH [1, 2, 3, 4] AS list RETURN list[0] AS first, list[2] AS third, list[-1] AS last",
               {{"1", "3", "4"}});
}

TEST_F(ListKeywordTest, comprehendsTheListNamedList) {
    expectRows("WITH [1, 2, 3, 4, 5] AS list RETURN [n IN list WHERE n > 2 | n] AS filtered",
               {{"[3, 4, 5]"}});
}

TEST_F(ListKeywordTest, matchesAPatternNamedList) {
    expectRows("MATCH (list:Person {name: 'Remy'}) RETURN list.name AS name", {{"Remy"}});
}

// The keyword the parser holds LIST for, which naming a variable `list` must leave alone.
// The test environment opens a graph of its own beside the one this fixture builds.
TEST_F(ListKeywordTest, listsTheGraphs) {
    expectRows("LIST GRAPH", {{"default"}, {"simpledb"}});
}

int main(int argc, char** argv) {
    return turing::test::turingTestMain(argc, argv);
}

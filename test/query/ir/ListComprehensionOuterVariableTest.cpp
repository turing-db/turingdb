#include <gtest/gtest.h>

#include <algorithm>
#include <memory>
#include <string>
#include <string_view>
#include <vector>

#include "QueryInterpreterV3.h"
#include "QueryStatus.h"

#include "Graph.h"
#include "SimpleGraph.h"
#include "SystemAccessor.h"
#include "SystemManager.h"
#include "versioning/ChangeID.h"
#include "versioning/CommitHash.h"

#include "StringRowSink.h"
#include "TuringTest.h"
#include "TuringTestEnv.h"

using namespace db;
using namespace turing::test;

// A list comprehension body reads a variable bound outside it as the one value its row
// carries, and every element of that row's list is computed against that value. What the
// projection beside the comprehension names cannot change the list it builds.
class ListComprehensionOuterVariableTest : public TuringTest {
protected:
    void initialize() override {
        _env = TuringTestEnv::create(fs::Path {_outDir} / "turing");
        _interpreter = std::make_unique<QueryInterpreterV3>(&_env->getSystemManager());

        SystemAccessor system = _env->getSystemManager().accessUnique();
        Graph* graph = system.createGraph(_graphName);
        SimpleGraph::createSimpleGraph(graph);
    }

    void runQuery(std::string_view query, StringRowSink& sink, QueryStatus& status) {
        _interpreter->execute(status, query, _graphName, CommitHash::head(), ChangeID::head(), &_env->getMem(), &sink);
    }

    void expectRows(std::string_view query, const std::vector<StringRowSink::Row>& expected) {
        StringRowSink sink;
        QueryStatus status;
        runQuery(query, sink, status);
        ASSERT_TRUE(status.isOk()) << query << ": " << status.getError();

        std::vector<StringRowSink::Row> actual;
        sink.sortedRows(actual);

        std::vector<StringRowSink::Row> sortedExpected = expected;
        std::sort(sortedExpected.begin(), sortedExpected.end());

        EXPECT_EQ(actual, sortedExpected) << "query: " << query;
    }

    const std::string _graphName = "simpledb";
    std::unique_ptr<TuringTestEnv> _env;
    std::unique_ptr<QueryInterpreterV3> _interpreter;
};

TEST_F(ListComprehensionOuterVariableTest, buildsTheListAgainstTheUnwoundValue) {
    expectRows("UNWIND [1,2] AS k RETURN [x IN [1,2] | x + k]", {{"2, 3"}, {"3, 4"}});
}

TEST_F(ListComprehensionOuterVariableTest, buildsTheListBesideTheUnwoundValue) {
    expectRows("UNWIND [1,2] AS k RETURN k, [x IN [1,2] | x + k]",
               {{"1", "2, 3"}, {"2", "3, 4"}});
}

TEST_F(ListComprehensionOuterVariableTest, filtersTheElementsAgainstTheUnwoundValue) {
    expectRows("UNWIND [1,2] AS k RETURN k, [x IN [1,2] WHERE x >= k | x]",
               {{"1", "1, 2"}, {"2", "2"}});
}

TEST_F(ListComprehensionOuterVariableTest, buildsTheListAgainstAProjectedConstant) {
    expectRows("WITH 1 AS k RETURN [x IN [1,2] | x + k]", {{"2, 3"}});
}

TEST_F(ListComprehensionOuterVariableTest, buildsTheListAgainstAMatchedProperty) {
    expectRows("MATCH (n:Person) WITH n.age AS a WHERE a IS NOT NULL RETURN a, [x IN [1,2] | x + a]",
               {{"32", "33, 34"}, {"32", "33, 34"}});
}

int main(int argc, char** argv) {
    return turing::test::turingTestMain(argc, argv);
}

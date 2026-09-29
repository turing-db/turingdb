#include <gtest/gtest.h>

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

// AFL input that failed the assertion 'element.getTag() == expectedTag' in NLExecutor:
// a list literal holding a nested list and an expression that evaluates to null.
class FuzzComputedNullInNestedListTest : public TuringTest {
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

        std::string actualText;
        describeRows(actual, actualText);

        EXPECT_EQ(actual, expected) << "query: " << query << "\nactual:\n" << actualText;
    }

    std::unique_ptr<TuringTestEnv> _env;
    std::unique_ptr<QueryInterpreterV3> _interpreter;
    std::string _graphName {"simpledb"};
};

TEST_F(FuzzComputedNullInNestedListTest, Unwind000123) {
    expectRows("UNWIND [[1], [1], [7], null- null] AS l RETURN DISTINCT l", {{"[1]"}, {"[7]"}, {"null"}});
}

TEST_F(FuzzComputedNullInNestedListTest, UnwindNullDifferenceBesideNestedList) {
    expectRows("UNWIND [[1], null - null] AS l RETURN l", {{"[1]"}, {"null"}});
}

TEST_F(FuzzComputedNullInNestedListTest, UnwindNullSumBesideNestedList) {
    expectRows("UNWIND [[1], null + 1] AS l RETURN l", {{"[1]"}, {"null"}});
}

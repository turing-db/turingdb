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

// A CALL subquery whose body returns a constant hands back one row, and that row sorts
// and dedups as any other row does.
class SubqueryConstantSortTest : public TuringTest {
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

TEST_F(SubqueryConstantSortTest, returnsTheConstantTheBodyHandedBack) {
    expectRows("CALL { RETURN 1 AS z } RETURN z", {{"1"}});
}

TEST_F(SubqueryConstantSortTest, countsTheConstantTheBodyHandedBack) {
    expectRows("CALL { RETURN 1 AS z } RETURN count(z)", {{"1"}});
}

TEST_F(SubqueryConstantSortTest, ordersOnTheConstantTheBodyHandedBack) {
    expectRows("CALL { RETURN 1 AS z } RETURN z ORDER BY z", {{"1"}});
}

TEST_F(SubqueryConstantSortTest, dedupsTheConstantTheBodyHandedBack) {
    expectRows("CALL { RETURN 1 AS z } RETURN DISTINCT z", {{"1"}});
}

int main(int argc, char** argv) {
    return turing::test::turingTestMain(argc, argv);
}

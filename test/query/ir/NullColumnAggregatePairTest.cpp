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

// Two aggregates of one projection over a column that is null on every row. Each answers
// what it answers alone, in whichever order the projection writes them: collect drops the
// nulls and yields the empty list, sum tallies no value and yields 0.
class NullColumnAggregatePairTest : public TuringTest {
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

TEST_F(NullColumnAggregatePairTest, collectsThenSumsAColumnNullOnEveryRow) {
    expectRows("MATCH (n:Person) RETURN collect(n.nope), sum(n.nope)", {{"[]", "0"}});
}

TEST_F(NullColumnAggregatePairTest, sumsThenCollectsTheSameColumn) {
    expectRows("MATCH (n:Person) RETURN sum(n.nope), collect(n.nope)", {{"0", "[]"}});
}

TEST_F(NullColumnAggregatePairTest, collectsThenTakesTheMaximum) {
    expectRows("MATCH (n:Person) RETURN collect(n.nope), max(n.nope)", {{"[]", "null"}});
}

TEST_F(NullColumnAggregatePairTest, collectsThenCountsEveryRow) {
    expectRows("MATCH (n:Person) RETURN collect(n.nope), count(*)", {{"[]", "8"}});
}

TEST_F(NullColumnAggregatePairTest, collectsThenSumsAColumnThePropertyHolds) {
    expectRows("MATCH (n:Person) RETURN collect(n.age), sum(n.age)", {{"[32, 32]", "64"}});
}

int main(int argc, char** argv) {
    return turing::test::turingTestMain(argc, argv);
}

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

// Reductions over a column that is null on every row: a name no property in simpledb
// carries, or the null literal. Nothing is reduced, so min, max and avg answer null, sum
// answers 0, count answers 0 and collect answers the empty list.
class NullAggregateTest : public TuringTest {
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

TEST_F(NullAggregateTest, extremaOfNothingAreNull) {
    expectRows("MATCH (n:Person) RETURN min(n.shoeSize)", {{"null"}});
    expectRows("MATCH (n:Person) RETURN max(n.shoeSize)", {{"null"}});
}

TEST_F(NullAggregateTest, averageOfNothingIsNull) {
    expectRows("MATCH (n:Person) RETURN avg(n.shoeSize)", {{"null"}});
}

// Cypher's one reduction that answers a value over no value at all
TEST_F(NullAggregateTest, sumOfNothingIsZero) {
    expectRows("MATCH (n:Person) RETURN sum(n.shoeSize)", {{"0"}});
}

TEST_F(NullAggregateTest, tallyOfNothingIsZeroAndTheListIsEmpty) {
    expectRows("MATCH (n:Person) RETURN count(n.shoeSize)", {{"0"}});
    expectRows("MATCH (n:Person) RETURN collect(n.shoeSize)", {{"[]"}});
}

// The null literal reduces as an absent property does: it is the same column of nulls
TEST_F(NullAggregateTest, reducesTheNullLiteral) {
    expectRows("RETURN max(null)", {{"null"}});
    expectRows("RETURN sum(null)", {{"0"}});
}

// Each group reduces its own rows, and every one of them is null
TEST_F(NullAggregateTest, reducesPerGroup) {
    expectRows("MATCH (n:Person) RETURN n.hasPhD, min(n.shoeSize)",
               {{"true", "null"}, {"false", "null"}});
    expectRows("MATCH (n:Person) RETURN n.hasPhD, sum(n.shoeSize)",
               {{"true", "0"}, {"false", "0"}});
}

// Dropping repeated values leaves the same nothing to reduce
TEST_F(NullAggregateTest, reducesDistinctNothing) {
    expectRows("MATCH (n:Person) RETURN sum(DISTINCT n.shoeSize)", {{"0"}});
    expectRows("MATCH (n:Person) RETURN count(DISTINCT n.shoeSize)", {{"0"}});
}

// A reduction over a null column stands beside one that has values to reduce
TEST_F(NullAggregateTest, reducesBesideAPopulatedColumn) {
    expectRows("MATCH (n:Person) RETURN max(n.age), max(n.shoeSize)", {{"32", "null"}});
}

int main(int argc, char** argv) {
    return turing::test::turingTestMain(argc, argv);
}

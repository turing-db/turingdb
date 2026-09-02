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

namespace {

constexpr size_t nodeCount = 18;

}

// The query test suite's avg-mixed-return-error, count-m-return-n, count-n-return-m and
// return-count-n-count-m cases on the v3 engine. Each returns an aggregate beside another
// item, which the v1 planner turned away as one unsupported shape; here the plain item is
// the grouping key, as SQL groups by what it selects beside an aggregate.
class AggregateWithReturnItemsTest : public TuringTest {
public:
    void initialize() override {
        _env = TuringTestEnv::create(fs::Path {_outDir} / "turing");

        SystemAccessor system = _env->getSystemManager().accessUnique();
        Graph* graph = system.createGraph(_graphName);
        SimpleGraph::createSimpleGraph(graph);

        _interpreter = std::make_unique<QueryInterpreterV3>(&_env->getSystemManager());
    }

protected:
    void expectRows(std::string_view query, std::vector<StringRowSink::Row>& expected) {
        StringRowSink sink;
        QueryStatus status;

        _interpreter->execute(status,
                              query,
                              _graphName,
                              CommitHash::head(),
                              ChangeID::head(),
                              &_env->getMem(),
                              &sink);

        ASSERT_TRUE(status.isOk()) << "query: " << query << "\nerror: " << status.getError();

        std::vector<StringRowSink::Row> rows;
        sink.sortedRows(rows);

        std::sort(expected.begin(), expected.end());
        EXPECT_EQ(rows, expected) << "query: " << query;
    }

    const std::string _graphName = "simpledb";
    std::unique_ptr<TuringTestEnv> _env;
    std::unique_ptr<QueryInterpreterV3> _interpreter;
};

// avg-mixed-return-error: one group per node, so each average reads that node's age alone.
// Remy and Adam are the only nodes carrying one, both 32; every other group averages a
// single null.
TEST_F(AggregateWithReturnItemsTest, averagesWithinTheGroupOfEachReturnedNode) {
    std::vector<StringRowSink::Row> expected;
    for (size_t node = 0; node < nodeCount; node++) {
        const std::string age = node <= 1 ? "32" : "null";
        expected.push_back({age, std::to_string(node)});
    }

    expectRows("MATCH (n) RETURN avg(n.age), n", expected);
}

// The mirror of the average above with the key written first: the item order moves the
// columns, not the grouping, so each node still averages its own age alone
TEST_F(AggregateWithReturnItemsTest, averagesWithinTheGroupOfEachReturnedNodeWithTheKeyFirst) {
    std::vector<StringRowSink::Row> expected;
    for (size_t node = 0; node < nodeCount; node++) {
        const std::string age = node <= 1 ? "32" : "null";
        expected.push_back({std::to_string(node), age});
    }

    expectRows("MATCH (n) RETURN n, avg(n.age)", expected);
}

// count(*) beside a grouping key tallies the rows of that key's group, which for a single
// pattern is the one row each node matched - not the eighteen the whole match holds
TEST_F(AggregateWithReturnItemsTest, countsTheOneRowOfEachReturnedNode) {
    std::vector<StringRowSink::Row> expected;
    for (size_t node = 0; node < nodeCount; node++) {
        expected.push_back({std::to_string(node), "1"});
    }

    expectRows("MATCH (n) RETURN n, count(*)", expected);
}

// count-m-return-n: the two patterns cross into a row per pair of nodes, so each of the
// eighteen groups counts the eighteen m it was paired with
TEST_F(AggregateWithReturnItemsTest, countsThePairsOfEachReturnedNode) {
    std::vector<StringRowSink::Row> expected;
    for (size_t node = 0; node < nodeCount; node++) {
        expected.push_back({std::to_string(node), std::to_string(nodeCount)});
    }

    expectRows("MATCH (n), (m) RETURN n, count(m)", expected);
}

// count-n-return-m: the same grouping when the aggregate is written first
TEST_F(AggregateWithReturnItemsTest, countsThePairsWhenTheAggregateComesFirst) {
    std::vector<StringRowSink::Row> expected;
    for (size_t node = 0; node < nodeCount; node++) {
        expected.push_back({std::to_string(nodeCount), std::to_string(node)});
    }

    expectRows("MATCH (n), (m) RETURN count(n), m", expected);
}

// return-count-n-count-m: two aggregates and nothing else leave no grouping key, so the
// whole cross product of eighteen by eighteen is counted into one row
TEST_F(AggregateWithReturnItemsTest, countsEveryPairOnceWhenBothItemsAggregate) {
    std::vector<StringRowSink::Row> expected {{"324", "324"}};

    expectRows("MATCH (n), (m) RETURN COUNT(n), COUNT(m)", expected);
}

// The ungrouped baseline of the two above: with no item beside it, the tally is the whole
// cross product rather than one group of it
TEST_F(AggregateWithReturnItemsTest, countsEveryPairWithoutAGroupingKey) {
    std::vector<StringRowSink::Row> expected {{"324"}};

    expectRows("MATCH (n), (m) RETURN count(*)", expected);
}

int main(int argc, char** argv) {
    return turing::test::turingTestMain(argc, argv);
}

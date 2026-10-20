#include <gtest/gtest.h>

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
using Rows = std::vector<StringRowSink::Row>;
}

// An aggregate whose argument is computed from constants by an op that runs per row, such
// as range(1, 4), next to another aggregate of the same RETURN. Every argument is read over
// the rows ahead of the reductions, so each aggregate folds the 3 unwound rows.
class AggregateOverComputedConstantTest : public TuringTest {
protected:
    void initialize() override {
        _env = TuringTestEnv::create(fs::Path {_outDir} / "turing");
        _interpreter = std::make_unique<QueryInterpreterV3>(&_env->getSystemManager(), &_env->getMem(), &_env->getCompilerContext());

        SystemAccessor system = _env->getSystemManager().accessUnique();
        Graph* graph = system.createGraph(_graphName);
        SimpleGraph::createSimpleGraph(graph);
    }

    void expectRows(std::string_view query, const Rows& expected) {
        StringRowSink sink;
        QueryStatus status;
        _interpreter->execute(status,
                              query,
                              _graphName,
                              CommitHash::head(),
                              ChangeID::head(),
                              &sink);

        ASSERT_TRUE(status.isOk()) << "query: " << query << "\nerror: " << status.getError();
        EXPECT_EQ(sink.getRows(), expected) << "query: " << query;
    }

    const std::string _graphName = "simpledb";
    std::unique_ptr<TuringTestEnv> _env;
    std::unique_ptr<QueryInterpreterV3> _interpreter;
};

TEST_F(AggregateOverComputedConstantTest, countsARangeAfterACount) {
    expectRows("UNWIND range(1, 3) AS i RETURN count(i), count(range(1, 4))", {{"3", "3"}});
}

TEST_F(AggregateOverComputedConstantTest, countsARangeBeforeACount) {
    expectRows("UNWIND range(1, 3) AS i RETURN count(range(1, 4)), count(i)", {{"3", "3"}});
}

TEST_F(AggregateOverComputedConstantTest, collectsAReduceAfterACount) {
    expectRows("UNWIND range(1, 3) AS i RETURN count(*), collect(reduce(s = 'v', x IN range(1, 4) | s))",
               {{"3", "v, v, v"}});
}

TEST_F(AggregateOverComputedConstantTest, countsAComprehensionReadingTheRowAfterACount) {
    expectRows("UNWIND range(1, 3) AS i RETURN count(i), count([y IN range(1, 4) | i])", {{"3", "3"}});
}

TEST_F(AggregateOverComputedConstantTest, sumsTheSizeOfARangeAfterACount) {
    expectRows("UNWIND range(1, 3) AS i RETURN count(i), sum(size(range(1, 4)))", {{"3", "12"}});
}

TEST_F(AggregateOverComputedConstantTest, countsASplitAfterACount) {
    expectRows("UNWIND range(1, 3) AS i RETURN count(i), count(split('a,b', ','))", {{"3", "3"}});
}

TEST_F(AggregateOverComputedConstantTest, addsTwoCountsInOneItem) {
    expectRows("UNWIND range(1, 3) AS i RETURN count(i) + count(range(1, 4))", {{"6"}});
}

TEST_F(AggregateOverComputedConstantTest, countsARangeAfterTwoCollects) {
    expectRows("UNWIND range(1, 3) AS i RETURN collect(i) AS a, collect(i * 2) AS b, count(range(1, 4))",
               {{"1, 2, 3", "2, 4, 6", "3"}});
}

TEST_F(AggregateOverComputedConstantTest, countsARangeOverMatchedPeople) {
    expectRows("MATCH (n:Person) RETURN count(n), count(range(1, 2))", {{"8", "8"}});
}

TEST_F(AggregateOverComputedConstantTest, collectsARangeElementBesideAPropertyCount) {
    expectRows("MATCH (n:Person) RETURN count(n.name), collect(range(1, 2)[0])", {{"8", "1, 1, 1, 1, 1, 1, 1, 1"}});
}

// After the WITH the rows in flight are the single row count(i) left
TEST_F(AggregateOverComputedConstantTest, countsARangeOverTheRowOfAnEarlierCount) {
    expectRows("UNWIND range(1, 3) AS i WITH count(i) AS c RETURN sum(c), count(range(1, 4))", {{"3", "1"}});
}

int main(int argc, char** argv) {
    return turing::test::turingTestMain(argc, argv);
}

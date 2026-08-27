#include <gtest/gtest.h>

#include <stdint.h>

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
using ColumnNames = std::vector<std::string>;

constexpr uint64_t simpleGraphNodeCount = 18;

}

// The query test suite's avg-literal case on the v3 engine: RETURN avg(42)
//
// The constant is the aggregate's whole input, and a constant column holds one value
// standing for every row rather than one per row, so what the fold charges is the rows of
// the driving relation. A bare RETURN runs over one row, so 42 is charged once; a MATCH in
// front of it lays the same constant over every matched row, which leaves the average 42
// but not the sum or the tally. With no row at all there is nothing to charge and the
// average is null - where the legacy engine reduces the single row the constant is, and
// answers 42 whatever the relation holds.
class AvgLiteralTest : public TuringTest {
protected:
    void initialize() override {
        _env = TuringTestEnv::create(fs::Path {_outDir} / "turing");
        _interpreter = std::make_unique<QueryInterpreterV3>(&_env->getSystemManager());

        SystemAccessor system = _env->getSystemManager().accessUnique();
        Graph* graph = system.createGraph(_graphName);
        SimpleGraph::createSimpleGraph(graph);
    }

    void runQuery(std::string_view query, StringRowSink& sink) {
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
        StringRowSink sink;
        runQuery(query, sink);

        EXPECT_EQ(sink.getRows(), expected) << "query: " << query;
    }

    void expectNamedRows(std::string_view query, const ColumnNames& names, const Rows& expected) {
        StringRowSink sink;
        runQuery(query, sink);

        EXPECT_EQ(sink.getNames(), names) << "query: " << query;
        EXPECT_EQ(sink.getRows(), expected) << "query: " << query;
    }

    const std::string _graphName = "simpledb";
    std::unique_ptr<TuringTestEnv> _env;
    std::unique_ptr<QueryInterpreterV3> _interpreter;
};

// avg-literal: the one row a bare RETURN runs over holds the constant, so the average is
// the constant itself - carried in the double an average is, whatever the literal was
TEST_F(AvgLiteralTest, averagesAnIntegerLiteral) {
    const Rows expected = {{"42"}};
    expectNamedRows("RETURN avg(42)", {"avg(42)"}, expected);
}

TEST_F(AvgLiteralTest, averagesADoubleLiteral) {
    const Rows expected = {{"42.5"}};
    expectNamedRows("RETURN avg(42.5)", {"avg(42.5)"}, expected);
}

TEST_F(AvgLiteralTest, averagesALiteralUnderAnExpression) {
    const Rows expected = {{"43"}};
    expectRows("RETURN avg(42) + 1", expected);
}

// The same constant over a match: it is laid out over the 18 matched rows, and the mean of
// one value repeated is that value, so the broadcast leaves the answer alone.
TEST_F(AvgLiteralTest, averagesTheLiteralRepeatedByEveryMatchedRow) {
    const Rows expected = {{"42"}};
    expectRows("MATCH (n) RETURN avg(42)", expected);
}

// The two reductions the broadcast does move: the sum charges the constant once per matched
// row and the tally counts them, where a bare RETURN would answer 42 and 1.
TEST_F(AvgLiteralTest, sumsAndCountsTheLiteralOverEveryMatchedRow) {
    const uint64_t sum = 42 * simpleGraphNodeCount;

    const Rows expected = {{std::to_string(sum), std::to_string(simpleGraphNodeCount)}};
    expectRows("MATCH (n) RETURN sum(42), count(42)", expected);
}

TEST_F(AvgLiteralTest, sumsAndCountsTheLiteralOfABareReturn) {
    const Rows expected = {{"42", "1"}};
    expectRows("RETURN sum(42), count(42)", expected);
}

// No row to charge the constant against, so there is no value to divide: the average is
// null rather than the constant standing on its own.
TEST_F(AvgLiteralTest, averagesTheLiteralOfAnEmptyMatchToNull) {
    const Rows expected = {{"null"}};
    expectRows("MATCH (n) WHERE n.name = 'THIS IS NOT A NAME' RETURN avg(42)", expected);
}

// Under a grouping key the constant is averaged within each group: Remy (0) and Adam (1)
// are the two nodes carrying an age, so each answers for its own row.
TEST_F(AvgLiteralTest, averagesTheLiteralWithinEachGroup) {
    const Rows expected = {{"0", "42"}, {"1", "42"}};
    expectRows("MATCH (n) WHERE n.age = 32 RETURN n, avg(42)", expected);
}

// A constant is one value however many rows it stands for, so avg(DISTINCT 42) charges it
// once and answers the same 42 the ungrouped average does.
TEST_F(AvgLiteralTest, averagesTheDistinctLiteralOverAMatch) {
    const Rows expected = {{"42"}};
    expectRows("MATCH (n) RETURN avg(DISTINCT 42)", expected);
}

int main(int argc, char** argv) {
    return turing::test::turingTestMain(argc, argv);
}

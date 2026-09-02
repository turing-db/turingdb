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

namespace {

// The eighteen edges of simpledb, which MATCH (a)-->(b) matches
constexpr size_t edgeCount = 18;

}

// MATCH (a)-->(b) WITH a, b LIMIT 3 RETURN 1 on the v3 engine.
//
// A constant projection has no per-row column of its own, so its row count comes from the
// relation standing at it - here the three rows the WITH's LIMIT published, not the
// eighteen the match produced. Nothing past the barrier reads a or b, which is what makes
// the shape worth pinning: the carried columns are the only thing carrying the row count,
// so pruning them as unread would answer one row, or eighteen, instead of three.
//
// The cut has no ORDER BY, so which three rows it keeps is the engine's choice - but the
// constant is the same value for all of them, which leaves the answer determined anyway.
class ConstantReturnAfterWithLimitTest : public TuringTest {
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

    void expectRowsInOrder(std::string_view query, const Rows& expected) {
        RowSink sink;
        const QueryStatus status = runQuery(query, &sink);
        ASSERT_TRUE(status.isOk()) << "query: " << query << "\nerror: " << status.getError();

        std::string actualText;
        describeRows(sink.rows(), actualText);

        EXPECT_EQ(sink.rows(), expected) << "query: " << query << "\ngot:\n" << actualText;
    }

    // The number of rows a query answered, for a cut with no order to name which rows
    // those are
    void expectRowCount(std::string_view query, size_t expected) {
        RowSink sink;
        const QueryStatus status = runQuery(query, &sink);
        ASSERT_TRUE(status.isOk()) << "query: " << query << "\nerror: " << status.getError();

        EXPECT_EQ(sink.rows().size(), expected) << "query: " << query;
    }

    void expectCounts(std::string_view query, const Counts& expected) {
        CountSink sink;
        const QueryStatus status = runQuery(query, &sink);
        ASSERT_TRUE(status.isOk()) << "query: " << query << "\nerror: " << status.getError();

        Counts actual;
        sink.sortedCounts(actual);

        EXPECT_EQ(actual, expected) << "query: " << query;
    }

    const std::string _graphName = "simpledb";
    std::unique_ptr<TuringTestEnv> _env;
    std::unique_ptr<QueryInterpreterV3> _interpreter;
};

TEST_F(ConstantReturnAfterWithLimitTest, emitsTheConstantOncePerRowTheCutLeft) {
    expectRows("MATCH (a)-->(b) WITH a, b LIMIT 3 RETURN 1", {{"1"}, {"1"}, {"1"}});
}

TEST_F(ConstantReturnAfterWithLimitTest, countsTheRowsTheCutLeft) {
    expectCounts("MATCH (a)-->(b) WITH a, b LIMIT 3 RETURN count(*)", {3});
}

// Without a cut the same projection answers for every matched row, which is what makes
// the three above the LIMIT's doing rather than the constant's.
TEST_F(ConstantReturnAfterWithLimitTest, emitsTheConstantOncePerMatchedRowWithoutACut) {
    expectRows("MATCH (a)-->(b) WITH a, b RETURN 1", Rows(edgeCount, Row {"1"}));
}

TEST_F(ConstantReturnAfterWithLimitTest, emitsTheConstantForEveryRowUnderALimitPastTheLastOne) {
    expectRows("MATCH (a)-->(b) WITH a, b LIMIT 100 RETURN 1", Rows(edgeCount, Row {"1"}));
}

// A zero-width window publishes nothing, so the constant is emitted for no row at all
TEST_F(ConstantReturnAfterWithLimitTest, emitsNothingUnderAZeroLimit) {
    expectRows("MATCH (a)-->(b) WITH a, b LIMIT 0 RETURN 1", {});
}

TEST_F(ConstantReturnAfterWithLimitTest, emitsNothingPastTheLastRow) {
    expectRows("MATCH (a)-->(b) WITH a, b SKIP 100 LIMIT 3 RETURN 1", {});
}

// The same unordered cut, read by a projection that names a carried column. Which three
// rows it keeps is undefined without an order, but there are three of them - so the cut
// reaching the output is not what the constant-only projection above is missing.
TEST_F(ConstantReturnAfterWithLimitTest, keepsTheCutWhenTheProjectionNamesACarriedColumn) {
    expectRowCount("MATCH (a)-->(b) WITH a, b LIMIT 3 RETURN a", 3);
}

TEST_F(ConstantReturnAfterWithLimitTest, keepsTheCutWhenTheConstantStandsBesideACarriedColumn) {
    expectRowCount("MATCH (a)-->(b) WITH a, b LIMIT 3 RETURN 1, a", 3);
}

// Ordering the cut names the three rows it keeps, and the constant rides beside the
// carried columns of exactly those - Adam's three edges, first in name order.
TEST_F(ConstantReturnAfterWithLimitTest, emitsTheConstantBesideTheCarriedColumnsOfTheCutRows) {
    expectRowsInOrder("MATCH (a)-->(b) WITH a, b ORDER BY a.name, b.name LIMIT 3 "
                      "RETURN 1, a.name, b.name",
                      {{"1", "Adam", "Bio"}, {"1", "Adam", "Cooking"}, {"1", "Adam", "Remy"}});
}

int main(int argc, char** argv) {
    return turing::test::turingTestMain(argc, argv);
}

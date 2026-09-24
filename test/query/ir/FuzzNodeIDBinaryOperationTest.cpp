#include <gtest/gtest.h>

#include <stddef.h>

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

// AFL inputs that threw 'Unsupported binary operation of columns of kinds
// ColumnVector<ID<unsigned long, 1>> and ColumnVector<unsigned long>': a node grouping key
// compared to a count.
class FuzzNodeIDBinaryOperationTest : public TuringTest {
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

    void expectRows(std::string_view query, Rows& expected) {
        RowSink sink;
        const QueryStatus status = runQuery(query, &sink);
        ASSERT_TRUE(status.isOk()) << "query: " << query << "\nerror: " << status.getError();

        Rows actual;
        sink.sortedRows(actual);
        std::ranges::sort(expected);

        std::string actualText;
        describeRows(actual, actualText);

        EXPECT_EQ(actual, expected) << "query: " << query << "\nactual:\n" << actualText;
    }

    void expectOneRowPerNode(std::string_view query, size_t columnCount) {
        RowSink sink;
        const QueryStatus status = runQuery(query, &sink);
        ASSERT_TRUE(status.isOk()) << "query: " << query << "\nerror: " << status.getError();

        const Rows& rows = sink.rows();
        ASSERT_EQ(rows.size(), _nodeCount) << "query: " << query;

        const bool everyRowHasEveryColumn = std::ranges::all_of(rows, [columnCount](const Row& row) {
            return row.size() == columnCount;
        });
        EXPECT_TRUE(everyRowHasEveryColumn) << "query: " << query;
    }

    std::unique_ptr<TuringTestEnv> _env;
    std::unique_ptr<QueryInterpreterV3> _interpreter;
    std::string _graphName {"simpledb"};
    size_t _nodeCount {18};
};

TEST_F(FuzzNodeIDBinaryOperationTest, Return000159) {
    expectOneRowPerNode("MATCH (n), (m) WHERE m.age = 32 REtURN COUNT(n) >= +                          10< COUNT(n)  + 20010 + 0, n    = + COUNT(n) >0 + COUNT(n)  + 20010 + 0, n        , n.a2000", 4);
}

TEST_F(FuzzNodeIDBinaryOperationTest, Return000160) {
    expectOneRowPerNode("MATCH (n), (m) WHERE m.age = 32 REtURN COUNT(n) >= +                          10< COUNT(n)  + 20010 + 0, n    = +    + COUNT(n) >0 + COUNT(n)  + 20010 + 0, n        , n.a2000", 4);
}

TEST_F(FuzzNodeIDBinaryOperationTest, Return000161) {
    expectOneRowPerNode("MATCH (n), (m) WHERE m.age = 32 REtURN COUNT(n) >= +          000000000<=0.00000< COUNT(n)  + 2001020010 + 0, n    = + COUNT(n) >0 + COUNT(n)  + 20010 + 0, n        , n.a2000", 4);
}

// A node compared to an integer compares its ID, as WHERE n = 1 does. Grouped by n, every
// count is 1, so only node 1 equals its count.
TEST_F(FuzzNodeIDBinaryOperationTest, ReturnNodeEqualsCount) {
    Rows expected;
    for (size_t node = 0; node < _nodeCount; node++) {
        expected.push_back({node == 1 ? "true" : "false", std::to_string(node)});
    }

    expectRows("MATCH (n) RETURN n = count(n), n", expected);
}

TEST_F(FuzzNodeIDBinaryOperationTest, ReturnCountEqualsNode) {
    Rows expected;
    for (size_t node = 0; node < _nodeCount; node++) {
        expected.push_back({node == 1 ? "true" : "false", std::to_string(node)});
    }

    expectRows("MATCH (n) RETURN count(n) = n, n", expected);
}

TEST_F(FuzzNodeIDBinaryOperationTest, ReturnNodeDiffersFromCount) {
    Rows expected;
    for (size_t node = 0; node < _nodeCount; node++) {
        expected.push_back({node == 1 ? "false" : "true", std::to_string(node)});
    }

    expectRows("MATCH (n) RETURN n <> count(n), n", expected);
}

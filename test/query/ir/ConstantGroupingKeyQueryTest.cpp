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

class ConstantGroupingKeyQueryTest : public TuringTest {
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

    void expectSortedRows(std::string_view query, const Rows& expected) {
        StringRowSink sink;
        runQuery(query, sink);

        Rows actual;
        sink.sortedRows(actual);

        EXPECT_EQ(actual, expected) << "query: " << query;
    }

    const std::string _graphName = "simpledb";
    std::unique_ptr<TuringTestEnv> _env;
    std::unique_ptr<QueryInterpreterV3> _interpreter;
};

TEST_F(ConstantGroupingKeyQueryTest, groupsOnAConstantToFloat) {
    const Rows expected = {{"5", "1"}};
    expectRows("WITH 5 AS d RETURN toFloat(d), count(*)", expected);
}

TEST_F(ConstantGroupingKeyQueryTest, groupsOnAConstantToIntegerThatIsNull) {
    const Rows expected = {{"null", "1"}};
    expectRows("WITH 'x' AS d RETURN toInteger(d), count(*)", expected);
}

TEST_F(ConstantGroupingKeyQueryTest, groupsOnAConstantDateTimeComponent) {
    const Rows expected = {{"2026", "1"}};
    expectRows("WITH datetime('2026-10-01T14:05:00Z') AS d RETURN d.year, count(*)", expected);
}

TEST_F(ConstantGroupingKeyQueryTest, groupsOnTheCurrentDateTimeComponent) {
    StringRowSink sink;
    runQuery("WITH datetime() AS d RETURN d.year, count(*)", sink);

    const Rows& rows = sink.getRows();
    ASSERT_EQ(rows.size(), 1u);
    ASSERT_EQ(rows.front().size(), 2u);

    EXPECT_NE(rows.front()[0], "null");
    EXPECT_EQ(rows.front()[1], "1");
}

TEST_F(ConstantGroupingKeyQueryTest, groupsOnAConstantSize) {
    const Rows expected = {{"3", "1"}};
    expectRows("WITH [1, 2, 3] AS l RETURN size(l), count(*)", expected);
}

TEST_F(ConstantGroupingKeyQueryTest, groupsOnTwoConstantKeys) {
    const Rows expected = {{"5", "3", "1"}};
    expectRows("WITH 5 AS d, [1, 2, 3] AS l RETURN toFloat(d), size(l), count(*)", expected);
}

TEST_F(ConstantGroupingKeyQueryTest, groupsEveryMatchedRowUnderAConstantKey) {
    const Rows expected = {{"5", "18"}};
    expectRows("MATCH (n) WITH n, 5 AS d RETURN toFloat(d), count(*)", expected);
}

TEST_F(ConstantGroupingKeyQueryTest, returnsAConstantToFloatWithNoAggregate) {
    const Rows expected = {{"5"}};
    expectRows("WITH 5 AS d RETURN toFloat(d)", expected);
}

TEST_F(ConstantGroupingKeyQueryTest, groupsOnARowVaryingToFloat) {
    const Rows expected = {{"32", "2"}, {"null", "16"}};
    expectSortedRows("MATCH (n) RETURN toFloat(n.age), count(*)", expected);
}

TEST_F(ConstantGroupingKeyQueryTest, groupsOnARowVaryingKeyBesideAConstantOne) {
    const Rows expected = {{"32", "5", "2"}, {"null", "5", "16"}};
    expectSortedRows("MATCH (n) RETURN n.age, toFloat(5), count(*)", expected);
}

int main(int argc, char** argv) {
    return turing::test::turingTestMain(argc, argv);
}

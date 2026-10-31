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

// A collect or a percentile reads the rows of its projection as every other aggregate
// beside it does, in either order and with or without grouping keys. StringRowSink prints
// a list without brackets, and an empty one as nothing.
class CollectBesideAggregatesTest : public TuringTest {
protected:
    void initialize() override {
        _env = TuringTestEnv::create(fs::Path {_outDir} / "turing");
        _interpreter = std::make_unique<QueryInterpreterV3>(&_env->getSystemManager(), &_env->getMem(), &_env->getCompilerContext());

        SystemAccessor system = _env->getSystemManager().accessUnique();
        Graph* graph = system.createGraph(_graphName);
        SimpleGraph::createSimpleGraph(graph);
    }

    void runQuery(std::string_view query, StringRowSink& sink, QueryStatus& status) {
        _interpreter->execute(status, query, _graphName, CommitHash::head(), ChangeID::head(), &sink);
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

TEST_F(CollectBesideAggregatesTest, collectsBesideACountOfTheSingleRow) {
    expectRows("RETURN collect(1) AS k, count(*) AS c", {{"1", "1"}});
    expectRows("RETURN count(*) AS c, collect(1) AS k", {{"1", "1"}});
}

TEST_F(CollectBesideAggregatesTest, collectsBesideEveryReductionOfTheSingleRow) {
    expectRows("RETURN collect(1) AS k, min(1) AS m, max(2) AS x, sum(3) AS s, avg(4) AS a, stDev(5) AS d, count(6) AS c",
               {{"1", "1", "2", "3", "4", "0", "1"}});
    expectRows("RETURN min(1) AS m, max(2) AS x, sum(3) AS s, avg(4) AS a, stDev(5) AS d, count(6) AS c, collect(1) AS k",
               {{"1", "2", "3", "4", "0", "1", "1"}});
}

TEST_F(CollectBesideAggregatesTest, collectsBesideDistinctReductionsOfTheSingleRow) {
    expectRows("RETURN collect(DISTINCT 1) AS k, count(DISTINCT 1) AS d, sum(DISTINCT 2) AS s", {{"1", "1", "2"}});
    expectRows("RETURN count(DISTINCT 1) AS d, sum(DISTINCT 2) AS s, collect(DISTINCT 1) AS k", {{"1", "2", "1"}});
}

TEST_F(CollectBesideAggregatesTest, takesAPercentileBesideACountOfTheSingleRow) {
    expectRows("RETURN percentileDisc(5, 0.5) AS p, count(*) AS c", {{"5", "1"}});
    expectRows("RETURN count(*) AS c, percentileCont(5, 0.5) AS p", {{"1", "5"}});
    expectRows("RETURN collect(5) AS k, percentileDisc(5, 0.5) AS p, min(5) AS m", {{"5", "5", "5"}});
}

TEST_F(CollectBesideAggregatesTest, collectsBesideACountOfWhatAFilterLeaves) {
    expectRows("WITH 1 AS a WHERE a > 0 RETURN collect(a) AS k, count(*) AS c", {{"1", "1"}});
    expectRows("WITH 1 AS a WHERE a > 0 RETURN count(*) AS c, collect(a) AS k", {{"1", "1"}});
    expectRows("WITH 1 AS a WHERE a > 2 RETURN collect(a) AS k, count(*) AS c", {{"", "0"}});
    expectRows("WITH 1 AS a WHERE a > 2 RETURN sum(a) AS s, collect(a) AS k", {{"0", ""}});
}

TEST_F(CollectBesideAggregatesTest, collectsBesideReductionsOfARelation) {
    expectRows("UNWIND [1, 2, 3] AS x RETURN collect(1) AS k, count(*) AS c, sum(x) AS s", {{"1, 1, 1", "3", "6"}});
    expectRows("UNWIND [1, 2, 3] AS x RETURN count(*) AS c, sum(x) AS s, collect(1) AS k", {{"3", "6", "1, 1, 1"}});
    expectRows("MATCH (p:Person) RETURN collect(p.age) AS k, count(p.age) AS c, avg(p.age) AS a, percentileDisc(p.age, 0.5) AS p",
               {{"32, 32", "2", "32", "32"}});
}

TEST_F(CollectBesideAggregatesTest, collectsBesideReductionsOfNoRow) {
    expectRows("MATCH (n:NoSuchLabel) RETURN collect(n.name) AS k, count(*) AS c, count(n.age) AS d, sum(n.age) AS s, "
               "min(n.age) AS m, avg(n.age) AS a, stDev(n.age) AS sd, count(DISTINCT n.age) AS cd",
               {{"", "0", "0", "0", "null", "null", "0", "0"}});
}

TEST_F(CollectBesideAggregatesTest, collectsBesideACountUnderAGroupingKey) {
    expectRows("UNWIND [1, 2, 3] AS x RETURN x % 2 AS g, collect(1) AS k, count(*) AS c", {{"1", "1, 1", "2"}, {"0", "1", "1"}});
    expectRows("UNWIND [1, 2, 3] AS x RETURN x % 2 AS g, count(*) AS c, collect(1) AS k", {{"1", "2", "1, 1"}, {"0", "1", "1"}});
    expectRows("UNWIND [1, 2, 3] AS x RETURN x % 2 AS g, percentileDisc(x, 0) AS p, sum(1) AS s", {{"1", "1", "2"}, {"0", "2", "1"}});
}

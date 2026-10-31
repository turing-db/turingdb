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

const std::vector<StringRowSink::Row> knowsWellOnce = {{"Adam", "1", "1"},
                                                       {"Remy", "1", "1"}};

const std::vector<StringRowSink::Row> knowsWellOfEveryPerson = {{"Adam", "1"},
                                                                {"Cyrus", "0"},
                                                                {"Doruk", "0"},
                                                                {"Luc", "0"},
                                                                {"Martina", "0"},
                                                                {"Maxime", "0"},
                                                                {"Remy", "1"},
                                                                {"Suhas", "0"}};

}

// A projection whose non-aggregate items are all constant groups the rows it gets into one
// group, and gets no group from no row. simpledb has 8 people; only Remy and Adam have an
// age, 32, and only they have a KNOWS_WELL edge to a person. It has 9 labels.
class ConstantKeyCompositionTest : public TuringTest {
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

TEST_F(ConstantKeyCompositionTest, groupsTheOneRowOfAnOptionalMatchOfNothing) {
    expectRows("OPTIONAL MATCH (n:NoSuchLabel) RETURN 1 AS k, count(n)", {{"1", "0"}});
    expectRows("OPTIONAL MATCH (n:NoSuchLabel) RETURN 1 AS k, collect(n)", {{"1", ""}});
    expectRows("OPTIONAL MATCH (n:Person {name: 'Nobody'}) RETURN 1 AS k, count(n), count(*)", {{"1", "0", "1"}});

    expectRows("OPTIONAL MATCH (n:Person {name: 'Nobody'}) "
               "WITH n, 1 AS k "
               "RETURN k, count(n), collect(n.name)",
               {{"1", "0", ""}});

    expectRows("OPTIONAL MATCH (n:NoSuchLabel) RETURN n, count(*)", {{"null", "1"}});
    expectRows("OPTIONAL MATCH (n:NoSuchLabel) RETURN n.name AS k, 1 AS c, count(*)", {{"null", "1", "1"}});
}

TEST_F(ConstantKeyCompositionTest, groupsEveryPersonPastAnOptionalMatchOfNothing) {
    expectRows("MATCH (n:Person) "
               "OPTIONAL MATCH (n)-[:NO_SUCH]->(m) "
               "RETURN 'x' AS k, count(m), count(*)",
               {{"x", "0", "8"}});

    expectRows("MATCH (n:Person) "
               "OPTIONAL MATCH (n)-[:KNOWS_WELL]->(m:Interest) "
               "RETURN 'x' AS k, count(m), count(*)",
               {{"x", "0", "8"}});

    expectRows("MATCH (n:Person {name: 'Nobody'}) "
               "OPTIONAL MATCH (n)-->(m) "
               "RETURN 1 AS k, count(*)",
               {});
}

TEST_F(ConstantKeyCompositionTest, groupsUnderAComputedConstantKey) {
    expectRows("MATCH (n:Person) RETURN 1 < 2 AS k, count(n)", {{"true", "8"}});
    expectRows("MATCH (n:Person {name: 'Nobody'}) RETURN 1 < 2 AS k, count(n)", {});

    expectRows("MATCH (n:Person) RETURN 'a' + 'b' AS k, count(n)", {{"ab", "8"}});
    expectRows("MATCH (n:Person {name: 'Nobody'}) RETURN 'a' + 'b' AS k, count(n)", {});

    expectRows("MATCH (n:Person) RETURN toUpper('a') AS k, count(n)", {{"A", "8"}});
    expectRows("MATCH (n:Person {name: 'Nobody'}) RETURN toUpper('a') AS k, count(n)", {});

    expectRows("MATCH (n:Person) RETURN coalesce(null, 1) AS k, count(n)", {{"1", "8"}});
    expectRows("MATCH (n:Person {name: 'Nobody'}) RETURN coalesce(null, 1) AS k, count(n)", {});

    expectRows("MATCH (n:Person) RETURN CASE WHEN 1 = 1 THEN 'y' ELSE 'n' END AS k, count(n)", {{"y", "8"}});
    expectRows("MATCH (n:Person {name: 'Nobody'}) RETURN CASE WHEN 1 = 1 THEN 'y' ELSE 'n' END AS k, count(n)", {});
}

TEST_F(ConstantKeyCompositionTest, groupsUnderAConstantListMapOrTemporalKey) {
    expectRows("MATCH (n:Person) "
               "WITH [1, 2] AS k, {a: 1} AS m, count(n) AS c "
               "RETURN k, m.a, c",
               {{"1, 2", "1", "8"}});

    expectRows("MATCH (n:Person) "
               "WITH range(1, 3) AS k, "
               "{a: 1, b: 'x'} AS m, "
               "datetime('2020-01-01T00:00:00Z') AS d, "
               "duration(5) AS du, "
               "count(n) AS c "
               "RETURN k, m.a, m.b, d, du, c",
               {{"1, 2, 3", "1", "x", "2020-01-01T00:00:00Z", "PT0.000005S", "8"}});

    expectRows("MATCH (n:Person {name: 'Nobody'}) "
               "RETURN range(1, 3) AS k, datetime('2020-01-01T00:00:00Z') AS d, count(n)",
               {});
}

TEST_F(ConstantKeyCompositionTest, groupsUnderAConstantAndAPropertyKey) {
    expectRows("MATCH (n:Person) RETURN 1 AS k, n.age AS a, count(n) ORDER BY a",
               {{"1", "32", "2"}, {"1", "null", "6"}});

    expectRows("MATCH (n:Person {name: 'Nobody'}) RETURN 1 AS k, n.age AS a, count(n)", {});
}

TEST_F(ConstantKeyCompositionTest, groupsAgainAfterAConstantKeyedWith) {
    expectRows("MATCH (n:Person) WITH 1 AS k, count(n) AS c RETURN k, c, count(*)", {{"1", "8", "1"}});
    expectRows("MATCH (n:Person {name: 'Nobody'}) WITH 1 AS k, count(n) AS c RETURN k, c, count(*)", {});
    expectRows("MATCH (n:Person {name: 'Nobody'}) WITH 1 AS k, count(n) AS c RETURN count(*)", {{"0"}});
    expectRows("MATCH (n:Person {name: 'Nobody'}) WITH count(n) AS c RETURN 1 AS k, count(*)", {{"1", "1"}});

    expectRows("UNWIND [] AS x "
               "WITH 1 AS k, count(x) AS c "
               "WITH 2 AS k2, count(*) AS c2 "
               "RETURN k2, c2",
               {});

    expectRows("UNWIND [] AS x "
               "WITH 1 AS k, count(x) AS c "
               "WITH count(*) AS c2 "
               "RETURN c2",
               {{"0"}});
}

TEST_F(ConstantKeyCompositionTest, readsTheConstantKeyOfAWithInTheClausesAfterIt) {
    expectRows("MATCH (n:Person) WITH 1 AS k, count(n) AS c WHERE c > 100 RETURN k, c", {});
    expectRows("MATCH (n:Person) WITH 1 AS k, count(n) AS c WHERE c > 1 RETURN k, c", {{"1", "8"}});

    expectRows("MATCH (n:Person) "
               "WITH 1 AS k, count(n) AS c "
               "MATCH (m:Person) WHERE m.age = k + 31 "
               "RETURN k, c, m.name AS name ORDER BY name",
               {{"1", "8", "Adam"}, {"1", "8", "Remy"}});

    expectRows("MATCH (n:Person) "
               "WITH 'Remy' AS nm, count(n) AS c "
               "MATCH (m:Person {name: nm}) "
               "RETURN nm, c, m.age",
               {{"Remy", "8", "32"}});

    expectRows("MATCH (n:Person {name: 'Nobody'}) "
               "WITH 1 AS k, count(n) AS c "
               "OPTIONAL MATCH (m:Person) "
               "RETURN k, c, count(m)",
               {});

    expectRows("MATCH (n:Person {name: 'Nobody'}) "
               "WITH 1 AS k, count(n) AS c "
               "OPTIONAL MATCH (m:Person) "
               "RETURN count(m)",
               {{"0"}});
}

TEST_F(ConstantKeyCompositionTest, groupsUnderAConstantBoundBeforeTheMatch) {
    expectRows("WITH 5 AS five MATCH (n:Person) RETURN five + 1 AS k, count(n)", {{"6", "8"}});
    expectRows("WITH 5 AS five MATCH (n:Person {name: 'Nobody'}) RETURN five + 1 AS k, count(n)", {});

    expectRows("WITH 1 AS k UNWIND [] AS x RETURN k, count(x)", {});

    expectRows("UNWIND [1, 2] AS u "
               "WITH 1 AS k, u "
               "MATCH (n:Person {name: 'Nobody'}) "
               "RETURN k, count(n)",
               {});

    expectRows("MATCH (n:Person) WITH *, 1 AS k RETURN k, count(n)", {{"1", "8"}});

    expectRows("WITH 1 AS k "
               "MATCH (n:Person) "
               "WITH *, count(n) AS c "
               "RETURN k, c",
               {{"1", "1"}, {"1", "1"}, {"1", "1"}, {"1", "1"}, {"1", "1"}, {"1", "1"}, {"1", "1"}, {"1", "1"}});
}

TEST_F(ConstantKeyCompositionTest, groupsUnderTheCountOfAKeylessWith) {
    expectRows("MATCH (n:Person) WITH count(n) AS c MATCH (m:Person) RETURN c, count(m)", {{"8", "8"}});
    expectRows("MATCH (n:Person {name: 'Nobody'}) WITH count(n) AS c MATCH (m:Person) RETURN c, count(m)", {{"0", "8"}});
}

TEST_F(ConstantKeyCompositionTest, ordersSkipsAndLimitsTheOneGroup) {
    expectRows("MATCH (n:Person) "
               "WITH 1 AS k, n "
               "RETURN k, count(n) AS c ORDER BY c DESC SKIP 0 LIMIT 1",
               {{"1", "8"}});

    expectRows("MATCH (n:Person) "
               "WITH 1 AS k, n "
               "RETURN k, count(n) AS c ORDER BY c DESC SKIP 1",
               {});

    expectRows("MATCH (n:Person) RETURN 1 AS k, count(n) LIMIT 0", {});

    expectRows("MATCH (n:Person {name: 'Nobody'}) "
               "WITH 1 AS k, count(n) AS c ORDER BY c LIMIT 1 "
               "RETURN k, c",
               {});

    expectRows("MATCH (n:Person) RETURN DISTINCT 1 AS k, count(n)", {{"1", "8"}});
    expectRows("MATCH (n:Person {name: 'Nobody'}) RETURN DISTINCT 1 AS k", {});
}

TEST_F(ConstantKeyCompositionTest, reducesEveryAggregateUnderAConstantKey) {
    expectRows("MATCH (n:Person) RETURN 1 AS k, count(DISTINCT n.age), count(DISTINCT n.isFrench)", {{"1", "1", "2"}});

    expectRows("MATCH (n:Person) RETURN 1 AS k, min(n.age), max(n.age), avg(n.age), sum(n.age)",
               {{"1", "32", "32", "32", "64"}});

    expectRows("MATCH (n:Person) RETURN 'k' AS k, min(n.name), max(n.name), collect(DISTINCT n.age)",
               {{"k", "Adam", "Suhas", "32"}});

    expectRows("MATCH (n:Person) RETURN 1 AS k, count(n) + 1 AS c", {{"1", "9"}});

    expectRows("MATCH (n:Person) RETURN 1 AS k, stDev(n.age), stDevP(n.age), percentileDisc(n.age, 0.5)",
               {{"1", "0", "0", "32"}});

    expectRows("MATCH (n:Person) RETURN 1 AS k, size(collect([n.age])) AS lists, size(collect(n)) AS s",
               {{"1", "8", "8"}});
}

TEST_F(ConstantKeyCompositionTest, reducesNullsAndNoRowUnderAConstantKey) {
    expectRows("MATCH (n:Person) WHERE n.age IS NULL "
               "RETURN 1 AS k, min(n.age), max(n.age), avg(n.age), sum(n.age)",
               {{"1", "null", "null", "null", "0"}});

    expectRows("MATCH (n:Person {name: 'Nobody'}) RETURN 1 AS k, min(n.age), max(n.age), avg(n.age), sum(n.age)", {});
    expectRows("MATCH (n:Person {name: 'Nobody'}) RETURN min(n.age), max(n.age), avg(n.age), sum(n.age)",
               {{"null", "null", "null", "0"}});

    expectRows("MATCH (n:Person {name: 'Nobody'}) RETURN 1 AS k, count(n) + 1 AS c", {});
    expectRows("MATCH (n:Person {name: 'Nobody'}) RETURN count(n) + 1 AS c", {{"1"}});

    expectRows("MATCH (n:Person {name: 'Nobody'}) RETURN 1 AS k, stDev(n.age), percentileDisc(n.age, 0.5)", {});
    expectRows("MATCH (n:Person {name: 'Nobody'}) RETURN stDev(n.age), percentileDisc(n.age, 0.5)", {{"0", "null"}});
}

TEST_F(ConstantKeyCompositionTest, groupsTheRowsOfAnUnwind) {
    expectRows("UNWIND [1, null, 2, null] AS x RETURN 1 AS k, count(x), count(*), collect(x), sum(x)",
               {{"1", "2", "4", "1, 2", "3"}});

    expectRows("UNWIND [1, 1, 2, null] AS x RETURN 1 AS k, count(DISTINCT x), collect(DISTINCT x)",
               {{"1", "2", "1, 2"}});

    expectRows("UNWIND [] AS x WITH 1 AS k, x RETURN k, count(x)", {});

    expectRows("UNWIND [1, 2] AS u MATCH (n:Person) RETURN 1 AS k, count(n)", {{"1", "16"}});
    expectRows("UNWIND [1, 2] AS u MATCH (n:Person) RETURN u, count(n) ORDER BY u", {{"1", "8"}, {"2", "8"}});

    expectRows("RETURN 1 AS k, count(*)", {{"1", "1"}});
    expectRows("MATCH (a), (b), (c) RETURN 1 AS k, count(*)", {{"1", "5832"}});
}

TEST_F(ConstantKeyCompositionTest, groupsRowsOverSeveralChunks) {
    expectRows("UNWIND range(1, 70000) AS i RETURN 1 AS k, count(i), sum(i)", {{"1", "70000", "2450035000"}});

    expectRows("UNWIND range(1, 200000) AS i "
               "WITH i WHERE i % 2 = 0 "
               "RETURN 1 AS k, count(*), size(collect(i))",
               {{"1", "100000", "100000"}});
}

TEST_F(ConstantKeyCompositionTest, groupsInsideACallBodyRunPerRow) {
    expectRows("MATCH (p:Person) "
               "CALL (p) { MATCH (p)-[:KNOWS_WELL]->(f) RETURN 1 AS k, count(f) AS c } "
               "RETURN p.name AS name, k, c ORDER BY name",
               knowsWellOnce);

    expectRows("MATCH (p:Person) "
               "CALL (p) { MATCH (p)-[:KNOWS_WELL]->(f) RETURN p.name AS k, count(f) AS c } "
               "RETURN k, c ORDER BY k",
               {{"Adam", "1"}, {"Remy", "1"}});

    expectRows("MATCH (p:Person) "
               "CALL (p) { MATCH (p)-[:KNOWS_WELL]->(f) RETURN p AS q, count(f) AS c } "
               "RETURN q.name AS k, c ORDER BY k",
               {{"Adam", "1"}, {"Remy", "1"}});

    expectRows("MATCH (p:Person) "
               "CALL (p) { MATCH (p)-[:KNOWS_WELL]->(f) RETURN count(f) AS c } "
               "RETURN p.name AS k, c ORDER BY k",
               knowsWellOfEveryPerson);

    expectRows("UNWIND [32, 1] AS u "
               "CALL (u) { MATCH (n:Person) WHERE n.age = u RETURN u AS k, count(n) AS c } "
               "RETURN k, c",
               {{"32", "2"}});
}

TEST_F(ConstantKeyCompositionTest, groupsTheOptionalMatchOfACallBody) {
    expectRows("MATCH (p:Person) "
               "CALL (p) { OPTIONAL MATCH (p)-[:KNOWS_WELL]->(f) RETURN 1 AS k, count(f) AS c } "
               "RETURN p.name AS name, k, c ORDER BY name",
               {{"Adam", "1", "1"},
                {"Cyrus", "1", "0"},
                {"Doruk", "1", "0"},
                {"Luc", "1", "0"},
                {"Martina", "1", "0"},
                {"Maxime", "1", "0"},
                {"Remy", "1", "1"},
                {"Suhas", "1", "0"}});
}

TEST_F(ConstantKeyCompositionTest, groupsInsideACallBodyRunOnce) {
    expectRows("CALL { MATCH (n:Person {name: 'Nobody'}) RETURN 1 AS k, count(n) AS c } RETURN k, c", {});
    expectRows("CALL { MATCH (n:Person {name: 'Nobody'}) RETURN count(n) AS c } RETURN c", {{"0"}});

    expectRows("CALL { "
               "MATCH (n:Person {name: 'Nobody'}) RETURN 1 AS k, count(n) AS c "
               "UNION ALL "
               "MATCH (n:Person) RETURN 2 AS k, count(n) AS c "
               "} "
               "RETURN k, c",
               {{"2", "8"}});

    expectRows("MATCH (n:Person {name: 'Nobody'}) "
               "RETURN 1 AS k, count(n) AS c "
               "UNION ALL "
               "RETURN 2 AS k, 0 AS c",
               {{"2", "0"}});

    expectRows("CALL { UNWIND [1, 2, 3] AS v RETURN v } RETURN 'k' AS k, sum(v)", {{"k", "6"}});
}

TEST_F(ConstantKeyCompositionTest, groupsUnderAndInsideASubqueryExpression) {
    expectRows("MATCH (p:Person) "
               "RETURN p.name AS name, "
               "EXISTS { MATCH (p)-[:KNOWS_WELL]->(f) RETURN 1 AS k, count(f) } AS e, "
               "COUNT { MATCH (p)-[:KNOWS_WELL]->(f) RETURN 1 AS k, count(f) } AS c "
               "ORDER BY name",
               {{"Adam", "true", "1"},
                {"Cyrus", "false", "0"},
                {"Doruk", "false", "0"},
                {"Luc", "false", "0"},
                {"Martina", "false", "0"},
                {"Maxime", "false", "0"},
                {"Remy", "true", "1"},
                {"Suhas", "false", "0"}});

    expectRows("UNWIND [] AS x RETURN EXISTS { MATCH (n:Person) } AS k, count(x)", {});
    expectRows("UNWIND [1, 2] AS x RETURN EXISTS { MATCH (n:Person) } AS k, count(x)", {{"true", "2"}});

    expectRows("UNWIND [] AS x RETURN COUNT { MATCH (n:Person) } AS k, count(x)", {});
    expectRows("UNWIND [1, 2] AS x RETURN COUNT { MATCH (n:Person) } AS k, count(x)", {{"8", "2"}});
}

TEST_F(ConstantKeyCompositionTest, groupsTheRowsAProcedureYields) {
    expectRows("CALL db.labels() YIELD label RETURN 1 AS k, count(label)", {{"1", "9"}});

    expectRows("CALL db.labels() YIELD label "
               "WITH label WHERE label = 'Nope' "
               "RETURN 1 AS k, count(label)",
               {});
}

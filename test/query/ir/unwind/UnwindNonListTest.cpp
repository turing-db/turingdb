#include <gtest/gtest.h>

#include <algorithm>
#include <string>
#include <string_view>
#include <vector>

#include "ParameterMap.h"
#include "ParameterValue.h"
#include "metadata/DateTime.h"
#include "metadata/Duration.h"

#include "CallV3Test.h"
#include "StringRowSink.h"

using namespace db;
using namespace turing::test;

namespace {

using Rows = std::vector<StringRowSink::Row>;

}

// UNWIND of a value that is not a list gives one row holding that value, of null or an empty
// list no row, and of a list one row per element. Each test writes one value as a literal, a
// parameter, a variable and a computed expression, and every form must give the same rows.
class UnwindNonListTest : public CallV3Test {
protected:
    void expectRows(std::string_view query, const ParameterMap& parameters, const Rows& expected) {
        StringRowSink sink;
        runQuery(query, parameters, sink);

        Rows rows;
        sink.sortedRows(rows);

        Rows sortedExpected = expected;
        std::sort(sortedExpected.begin(), sortedExpected.end());

        EXPECT_EQ(rows, sortedExpected) << query;
    }

    void expectRows(std::string_view query, const Rows& expected) {
        expectRows(query, ParameterMap {}, expected);
    }
};

TEST_F(UnwindNonListTest, unwindsAnIntegerToOneRow) {
    ParameterMap parameters;
    parameters.insert("v").setInt64(5);

    expectRows("UNWIND 5 AS x RETURN x", {{"5"}});
    expectRows("UNWIND $v AS x RETURN x", parameters, {{"5"}});
    expectRows("WITH 5 AS y UNWIND y AS x RETURN x", {{"5"}});
    expectRows("UNWIND 2 + 3 AS x RETURN x", {{"5"}});
}

TEST_F(UnwindNonListTest, unwindsAFloatToOneRow) {
    ParameterMap parameters;
    parameters.insert("v").setDouble(2.5);

    expectRows("UNWIND 2.5 AS x RETURN x", {{"2.5"}});
    expectRows("UNWIND $v AS x RETURN x", parameters, {{"2.5"}});
    expectRows("WITH 2.5 AS y UNWIND y AS x RETURN x", {{"2.5"}});
    expectRows("UNWIND 1.0 + 1.5 AS x RETURN x", {{"2.5"}});
}

TEST_F(UnwindNonListTest, unwindsAStringToOneRow) {
    ParameterMap parameters;
    parameters.insert("v").setString("abc");

    expectRows("UNWIND 'abc' AS x RETURN x", {{"abc"}});
    expectRows("UNWIND $v AS x RETURN x", parameters, {{"abc"}});
    expectRows("WITH 'abc' AS y UNWIND y AS x RETURN x", {{"abc"}});
    expectRows("UNWIND 'ab' + 'c' AS x RETURN x", {{"abc"}});
}

TEST_F(UnwindNonListTest, unwindsABoolToOneRow) {
    ParameterMap parameters;
    parameters.insert("v").setBool(true);

    expectRows("UNWIND true AS x RETURN x", {{"true"}});
    expectRows("UNWIND $v AS x RETURN x", parameters, {{"true"}});
    expectRows("WITH true AS y UNWIND y AS x RETURN x", {{"true"}});
    expectRows("UNWIND NOT false AS x RETURN x", {{"true"}});
}

TEST_F(UnwindNonListTest, unwindsNullToNoRow) {
    ParameterMap parameters;
    parameters.insert("v").setNull();

    expectRows("UNWIND null AS x RETURN x", {});
    expectRows("UNWIND $v AS x RETURN x", parameters, {});
    expectRows("WITH null AS y UNWIND y AS x RETURN x", {});
    expectRows("UNWIND CASE WHEN false THEN 1 END AS x RETURN x", {});
}

TEST_F(UnwindNonListTest, unwindsAMapToOneRow) {
    ParameterMap parameters;
    parameters.insert("v").setMap()["a"].setInt64(1);

    expectRows("UNWIND {a: 1} AS x RETURN x", {{"{a: 1}"}});
    expectRows("UNWIND $v AS x RETURN x", parameters, {{"{a: 1}"}});
    expectRows("WITH {a: 1} AS y UNWIND y AS x RETURN x", {{"{a: 1}"}});
    expectRows("UNWIND {a: 0 + 1} AS x RETURN x", {{"{a: 1}"}});
    expectRows("UNWIND {a: 1} AS x RETURN x.a", {{"1"}});
}

TEST_F(UnwindNonListTest, unwindsADateTimeToOneRow) {
    const DateTime dateTime = DateTime::parse("2020-03-04T00:00:00Z").value();

    ParameterMap parameters;
    parameters.insert("v").setDateTime(dateTime);

    expectRows("UNWIND $v AS x RETURN x", parameters, {{"2020-03-04T00:00:00Z"}});
    expectRows("WITH datetime('2020-03-04T00:00:00Z') AS y UNWIND y AS x RETURN x", {{"2020-03-04T00:00:00Z"}});
    expectRows("UNWIND datetime('2020-03-04T00:00:00Z') AS x RETURN x", {{"2020-03-04T00:00:00Z"}});
}

TEST_F(UnwindNonListTest, unwindsADurationToOneRow) {
    ParameterMap parameters;
    parameters.insert("v").setDuration(Duration {3600000000});

    expectRows("UNWIND $v AS x RETURN x", parameters, {{"PT1H"}});
    expectRows("WITH duration({hours: 1}) AS y UNWIND y AS x RETURN x", {{"PT1H"}});
    expectRows("UNWIND duration({hours: 1}) AS x RETURN x", {{"PT1H"}});
}

TEST_F(UnwindNonListTest, unwindsAPointToOneRow) {
    const Rows point {{"point({x: 1.0, y: 2.0, crs: 'cartesian'})"}};

    expectRows("WITH point({x: 1.0, y: 2.0}) AS y UNWIND y AS x RETURN x", point);
    expectRows("UNWIND point({x: 1.0, y: 2.0}) AS x RETURN x", point);
}

TEST_F(UnwindNonListTest, unwindsAnEmbeddingToOneRow) {
    const std::vector<float> embedding {1.0f, 0.0f};

    ParameterMap parameters;
    parameters.insert("v").setEmbedding(embedding);

    expectRows("UNWIND (1.0, 0.0) AS x RETURN cosine_similarity(x, (1.0, 0.0))", {{"1"}});
    expectRows("UNWIND $v AS x RETURN cosine_similarity(x, (1.0, 0.0))", parameters, {{"1"}});
    expectRows("WITH (1.0, 0.0) AS y UNWIND y AS x RETURN cosine_similarity(x, (1.0, 0.0))", {{"1"}});
    expectRows("UNWIND CASE WHEN true THEN (1.0, 0.0) END AS x RETURN cosine_similarity(x, (1.0, 0.0))", {{"1"}});

    expectRows("UNWIND (1.0, 0.0) AS x RETURN x", {{"[1, 0]"}});
    expectRows("UNWIND [(1.0, 0.0)] AS x RETURN x", {{"[1, 0]"}});
    expectRows("UNWIND $v AS x RETURN x", parameters, {{"1, 0"}});
}

TEST_F(UnwindNonListTest, unwindsANodeToOneRow) {
    expectRows("MATCH (n) WHERE n.name = 'Remy' UNWIND n AS x RETURN x.name", {{"Remy"}});
    expectRows("MATCH (n) WHERE n.name = 'Remy' WITH n AS y UNWIND y AS x RETURN x.name", {{"Remy"}});
    expectRows("MATCH (n:Founder) UNWIND n AS x RETURN x.name", {{"Remy"}, {"Adam"}});
}

TEST_F(UnwindNonListTest, unwindsAnEdgeToOneRow) {
    expectRows("MATCH ()-[e:KNOWS_WELL]->() WHERE e.name = 'Remy -> Adam' UNWIND e AS x RETURN x.name",
               {{"Remy -> Adam"}});
    expectRows("MATCH ()-[e:KNOWS_WELL]->() WHERE e.name = 'Remy -> Adam' WITH e AS y UNWIND y AS x RETURN x.name",
               {{"Remy -> Adam"}});
}

TEST_F(UnwindNonListTest, unwindsAPathToOneRow) {
    expectRows("MATCH p = (a)-[:KNOWS_WELL]->(b) WHERE a.name = 'Ghosts' UNWIND p AS x RETURN length(x)", {{"1"}});
    expectRows("MATCH p = (a)-[:KNOWS_WELL]->(b) WHERE a.name = 'Ghosts' WITH p AS y UNWIND y AS x RETURN length(x)", {{"1"}});
}

TEST_F(UnwindNonListTest, unwindsAnEmptyListToNoRow) {
    ParameterMap parameters;
    parameters.insert("v").setList();

    expectRows("UNWIND [] AS x RETURN x", {});
    expectRows("UNWIND $v AS x RETURN x", parameters, {});
    expectRows("WITH [] AS y UNWIND y AS x RETURN x", {});
    expectRows("UNWIND tail([1]) AS x RETURN x", {});
}

TEST_F(UnwindNonListTest, unwindsANestedListToItsInnerLists) {
    ParameterMap parameters;
    ParameterValue::List& outer = parameters.insert("v").setList();
    ParameterValue::List& first = outer.emplace_back().setList();
    first.emplace_back().setInt64(1);
    first.emplace_back().setInt64(2);
    outer.emplace_back().setList().emplace_back().setInt64(3);

    const Rows innerLists {{"1, 2"}, {"3"}};

    expectRows("UNWIND [[1, 2], [3]] AS x RETURN x", innerLists);
    expectRows("UNWIND $v AS x RETURN x", parameters, innerLists);
    expectRows("WITH [[1, 2], [3]] AS y UNWIND y AS x RETURN x", innerLists);
    expectRows("UNWIND [[1, 2]] + [[3]] AS x RETURN x", innerLists);
}

TEST_F(UnwindNonListTest, unwindsAMixedListToItsElements) {
    ParameterMap parameters;
    ParameterValue::List& list = parameters.insert("v").setList();
    list.emplace_back().setInt64(1);
    list.emplace_back().setString("a");
    list.emplace_back().setBool(true);

    const Rows elements {{"1"}, {"a"}, {"true"}};

    expectRows("UNWIND [1, 'a', true] AS x RETURN x", elements);
    expectRows("UNWIND $v AS x RETURN x", parameters, elements);
    expectRows("WITH [1, 'a', true] AS y UNWIND y AS x RETURN x", elements);
    expectRows("UNWIND [1] + ['a', true] AS x RETURN x", elements);
}

TEST_F(UnwindNonListTest, unwindsATaggedCellToTheValueItHolds) {
    expectRows("UNWIND {a: 1}.a AS x RETURN x", {{"1"}});
    expectRows("WITH {a: 1} AS m UNWIND m.a AS x RETURN x", {{"1"}});
    expectRows("WITH {l: [1, 2]} AS m UNWIND m.l AS x RETURN x", {{"1"}, {"2"}});
    expectRows("WITH {a: null} AS m UNWIND m.a AS x RETURN x", {});
    expectRows("UNWIND [[1, 2], 3, null] AS v UNWIND v AS x RETURN x", {{"1"}, {"2"}, {"3"}});
}

TEST_F(UnwindNonListTest, unwindsAScalarBesideAMatch) {
    ParameterMap parameters;
    parameters.insert("v").setInt64(7);

    expectRows("MATCH (n:Founder) UNWIND 7 AS x RETURN n.name, x", {{"Remy", "7"}, {"Adam", "7"}});
    expectRows("MATCH (n:Founder) UNWIND $v AS x RETURN n.name, x", parameters, {{"Remy", "7"}, {"Adam", "7"}});
    expectRows("UNWIND 7 AS x MATCH (n:Founder) RETURN n.name, x", {{"Remy", "7"}, {"Adam", "7"}});
}

TEST_F(UnwindNonListTest, unwindsAScalarAfterAWith) {
    expectRows("WITH 1 AS a UNWIND 5 AS x RETURN a, x", {{"1", "5"}});
    expectRows("MATCH (n:Founder) WITH n.name AS name UNWIND 'z' AS x RETURN name, x", {{"Remy", "z"}, {"Adam", "z"}});
    expectRows("UNWIND 1 AS a UNWIND [2, 3] AS b RETURN a, b", {{"1", "2"}, {"1", "3"}});
    expectRows("UNWIND [2, 3] AS b UNWIND 1 AS a RETURN a, b", {{"1", "2"}, {"1", "3"}});
    expectRows("MATCH (n) WITH count(n) AS c UNWIND 5 AS x RETURN c, x", {{"18", "5"}});
}

TEST_F(UnwindNonListTest, countsTheOneRowOfAScalar) {
    expectRows("UNWIND 5 AS x RETURN count(x)", {{"1"}});
    expectRows("UNWIND 'abc' AS x RETURN count(*)", {{"1"}});
    expectRows("UNWIND {a: 1} AS x RETURN count(*)", {{"1"}});
}

TEST_F(UnwindNonListTest, unwindsAScalarInACallSubquery) {
    expectRows("CALL { UNWIND 5 AS x RETURN x } RETURN x", {{"5"}});
    expectRows("MATCH (n:Founder) CALL (n) { UNWIND 5 AS x RETURN x } RETURN n.name, x", {{"Remy", "5"}, {"Adam", "5"}});
    expectRows("MATCH (n:Founder) CALL { WITH n UNWIND n.name AS x RETURN x } RETURN x", {{"Remy"}, {"Adam"}});
    expectRows("MATCH (n:Founder) CALL (*) { UNWIND 5 AS x RETURN x } RETURN n.name, x", {{"Remy", "5"}, {"Adam", "5"}});
    expectRows("MATCH (n:Founder) RETURN n.name, COUNT { UNWIND 5 AS x RETURN x }", {{"Remy", "1"}, {"Adam", "1"}});
    expectRows("MATCH (n:Founder) RETURN n.name, COLLECT { UNWIND 5 AS x RETURN x }", {{"Remy", "5"}, {"Adam", "5"}});
}

TEST_F(UnwindNonListTest, looksUpTheNodeAScalarNames) {
    ParameterMap parameters;
    parameters.insert("v").setInt64(1);

    expectRows("UNWIND 1 AS i MATCH (n) WHERE id(n) = i RETURN n.name", {{"Adam"}});
    expectRows("UNWIND $v AS i MATCH (n) WHERE id(n) = i RETURN n.name", parameters, {{"Adam"}});
    expectRows("WITH 1 AS y UNWIND y AS i MATCH (n) WHERE id(n) = i RETURN n.name", {{"Adam"}});
    expectRows("UNWIND 0 + 1 AS i MATCH (n) WHERE id(n) = i RETURN n.name", {{"Adam"}});
}

TEST_F(UnwindNonListTest, seedsAPatternNodeWithAScalar) {
    expectRows("UNWIND 1 AS x MATCH (x) RETURN x.name", {{"Adam"}});
    expectRows("UNWIND 0 AS x MATCH (x)-[:KNOWS_WELL]->(w) RETURN w.name", {{"Adam"}});
}

TEST_F(UnwindNonListTest, matchesAPropertyAgainstAScalar) {
    ParameterMap parameters;
    parameters.insert("v").setString("Remy");

    expectRows("UNWIND 32 AS a MATCH (n) WHERE n.age = a RETURN n.name", {{"Remy"}, {"Adam"}});
    expectRows("UNWIND 'Remy' AS name MATCH (n {name: name}) RETURN n.name", {{"Remy"}});
    expectRows("UNWIND $v AS name MATCH (n {name: name}) RETURN n.name", parameters, {{"Remy"}});
    expectRows("UNWIND 'Remy' AS name MATCH (n)-[:KNOWS_WELL]->(m) WHERE n.name = name RETURN m.name", {{"Adam"}});
}

#include <gtest/gtest.h>

#include "IRTestRows.h"
#include "WriteQueryTest.h"

using namespace db;
using namespace turing::test;

// Every expected row is Neo4j 5.26.30's answer to the same query, except where a test's
// name says the engine departs from it
class ListOutOfBoundsTest : public WriteQueryTest {
};

TEST_F(ListOutOfBoundsTest, readsNullPastEitherEndOfAConstantList) {
    expectRows("RETURN [10, 20, 30][3] AS v", {{"null"}});
    expectRows("RETURN [10, 20, 30][100] AS v", {{"null"}});
    expectRows("RETURN [10, 20, 30][-1] AS v", {{"30"}});
    expectRows("RETURN [10, 20, 30][-3] AS v", {{"10"}});
    expectRows("RETURN [10, 20, 30][-4] AS v", {{"null"}});
    expectRows("RETURN [10, 20, 30][-100] AS v", {{"null"}});
    expectRows("RETURN [][0] AS v", {{"null"}});
    expectRows("RETURN [][-1] AS v", {{"null"}});
}

TEST_F(ListOutOfBoundsTest, readsNullAtTheExtremesOfA64BitIndex) {
    expectRows("RETURN [10, 20, 30][9223372036854775807] AS v", {{"null"}});
    expectRows("RETURN [10, 20, 30][-9223372036854775807] AS v", {{"null"}});
    expectRows("RETURN [10, 20, 30][-9223372036854775807 - 1] AS v", {{"null"}});
    expectRowsInOrder("UNWIND [9223372036854775807, -9223372036854775807 - 1] AS i RETURN [10, 20, 30][i] AS v",
                      {{"null"}, {"null"}});
    expectRows("RETURN [10, 20, 30][[-9223372036854775807 - 1, 'a'][0]] AS v", {{"null"}});
}

TEST_F(ListOutOfBoundsTest, readsEachPerRowIndex) {
    expectRowsInOrder("UNWIND [0, 2, 3, -1, -3, -4, 100, -100] AS i RETURN i, [10, 20, 30][i] AS v",
                      {{"0", "10"},
                       {"2", "30"},
                       {"3", "null"},
                       {"-1", "30"},
                       {"-3", "10"},
                       {"-4", "null"},
                       {"100", "null"},
                       {"-100", "null"}});
}

TEST_F(ListOutOfBoundsTest, readsNullPastTheEndThroughATypeErasedIndexOrList) {
    expectRows("RETURN [10, 20, 30][[5, 'a'][0]] AS v", {{"null"}});
    expectRows("RETURN [10, 20, 30][[-4, 'a'][0]] AS v", {{"null"}});
    expectRows("UNWIND [[10, 20], 'x'] AS l WITH l LIMIT 1 RETURN l[2] AS a, l[-3] AS b", {{"null", "null"}});
    expectRowsInOrder("UNWIND [[1, 2, 3], [4], 'x', null] AS l "
                      "WITH l WHERE l IS NULL OR size(l) >= 0 AND l <> 'x' RETURN l[2] AS a, l[-2] AS b",
                      {{"3", "2"}, {"null", "null"}, {"null", "null"}});
}

TEST_F(ListOutOfBoundsTest, readsNullOffANullListOrThroughANullIndex) {
    expectRows("WITH null AS l RETURN l[0] AS v", {{"null"}});
    expectRows("WITH [1, null][1] AS i RETURN [10, 20][i] AS v", {{"null"}});
    expectRows("RETURN [[1]][1][0] AS v", {{"null"}});
    expectRows("RETURN [[1, 2]][0][5] AS v", {{"null"}});
}

TEST_F(ListOutOfBoundsTest, readsNullPastTheEndOfAListBuiltAtRunTime) {
    expectRows("MATCH (n:Person) WITH collect(n.name) AS names RETURN size(names) AS s, names[8] AS a, names[-9] AS b",
               {{"8", "null", "null"}});
    expectRows("WITH range(0, 99999) AS l RETURN l[99999] AS a, l[100000] AS b, l[-100000] AS c, l[-100001] AS d",
               {{"99999", "null", "0", "null"}});
    expectRows("RETURN range(1, 3)[5] AS v", {{"null"}});
    expectRows("UNWIND range(1, 70000) AS i WITH collect(i) AS l RETURN l[69999] AS a, l[70000] AS b, l[-70001] AS c",
               {{"70000", "null", "null"}});
}

TEST_F(ListOutOfBoundsTest, readsNullPastTheEndOfAStoredList) {
    applyWrite("CREATE (:L {xs: [1, 2, 3]})");

    expectRows("MATCH (n:L) RETURN n.xs[3] AS a, n.xs[-3] AS b, n.xs[-4] AS c, n.xs[1..9] AS d",
               {{"null", "1", "null", "[2, 3]"}});
}

TEST_F(ListOutOfBoundsTest, readsNullPastTheEndOfEntityLists) {
    expectRows("MATCH (n:Person) WITH collect(n) AS ns RETURN ns[8] IS NULL AS a, ns[8].name AS b, ns[-9].name AS c",
               {{"true", "null", "null"}});
    expectRows("MATCH p = (a:Person {name: 'Remy'})-[:KNOWS_WELL]->(b) "
               "RETURN nodes(p)[5] IS NULL AS a, relationships(p)[1] IS NULL AS b, nodes(p)[-3] IS NULL AS c",
               {{"true", "true", "true"}});
    expectRows("MATCH (n:Person {name: 'Remy'}) RETURN labels(n)[5] AS a, keys(n)[100] AS b", {{"null", "null"}});
}

TEST_F(ListOutOfBoundsTest, readsNullPastTheEndOfListsInMapsAndOfSplit) {
    expectRows("RETURN [{a: 1}][1].a AS a, {l: [1]}.l[3] AS b", {{"null", "null"}});
    expectRows("WITH {l: [1, 2]} AS m RETURN m.l[2] AS a, m.l[-3] AS b, m['l'][5] AS c", {{"null", "null", "null"}});
    expectRows("RETURN split('a,b', ',')[5] AS a, split('a,b', ',')[-3] AS b", {{"null", "null"}});
}

TEST_F(ListOutOfBoundsTest, slicesPastEitherEnd) {
    expectRows("RETURN [1, 2, 3][1..10] AS v", {{"[2, 3]"}});
    expectRows("RETURN [1, 2, 3][5..10] AS v", {{"[]"}});
    expectRows("RETURN [1, 2, 3][-10..2] AS v", {{"[1, 2]"}});
    expectRows("RETURN [1, 2, 3][2..1] AS v", {{"[]"}});
    expectRows("RETURN [1, 2, 3][-1..] AS v", {{"[3]"}});
    expectRows("RETURN [1, 2, 3][..-5] AS v", {{"[]"}});
    expectRows("RETURN [1, 2, 3][..100] AS v", {{"[1, 2, 3]"}});
    expectRows("RETURN [1, 2, 3][100..] AS v", {{"[]"}});
    expectRows("RETURN [][0..1] AS v", {{"[]"}});
}

TEST_F(ListOutOfBoundsTest, answersNullForASliceWithANullBound) {
    expectRows("RETURN [1, 2, 3][null..2] AS v", {{"null"}});
    expectRows("RETURN [1, 2, 3][1..null] AS v", {{"null"}});
}

TEST_F(ListOutOfBoundsTest, slicesWithPerRowBounds) {
    expectRowsInOrder("UNWIND [[0, 1], [2, 9], [-9, -1], [5, 9], [9, -9]] AS b RETURN [1, 2, 3][b[0]..b[1]] AS v",
                      {{"[1]"}, {"[3]"}, {"[1, 2]"}, {"[]"}, {"[]"}});
}

TEST_F(ListOutOfBoundsTest, clampsASliceBoundPastThe32BitRangeWhereNeo4jRaises) {
    expectRows("RETURN [1, 2, 3][-9223372036854775807 - 1..9223372036854775807] AS v", {{"[1, 2, 3]"}});
    expectRows("RETURN [1, 2, 3][9223372036854775807..] AS v", {{"[]"}});
    expectRows("RETURN [1, 2, 3][..-9223372036854775807 - 1] AS v", {{"[]"}});
}

TEST_F(ListOutOfBoundsTest, answersHeadLastAndTailOfShortLists) {
    expectRows("RETURN head([]) AS a, last([]) AS b, tail([]) AS c, tail([1]) AS d", {{"null", "null", "[]", "[]"}});
}

TEST_F(ListOutOfBoundsTest, readsNullPastTheEndInsideOtherConstructs) {
    expectRows("UNWIND [[1], []] AS l WITH l WHERE l[0] = 1 RETURN l", {{"[1]"}});
    expectRows("RETURN [x IN [[1, 2], [3]] | x[1]] AS v", {{"[2, null]"}});
    expectRows("RETURN reduce(s = 0, x IN [[1, 2], [3]] | s + coalesce(x[1], 0)) AS v", {{"2"}});
    expectRowsInOrder("UNWIND [[1, 2], [3]] AS x RETURN x[1] AS k, count(*) AS c ORDER BY k",
                      {{"2", "1"}, {"null", "1"}});
    expectRows("RETURN all(x IN [[1], []] WHERE x[0] IS NULL OR x[0] = 1) AS a, any(x IN [[1], []] WHERE x[3] = 1) AS b",
               {{"true", "null"}});
    expectRowsInOrder("UNWIND [[3], [], [1]] AS l RETURN l ORDER BY l[0]", {{"[1]"}, {"[3]"}, {"[]"}});
}

TEST_F(ListOutOfBoundsTest, usesAnOutOfBoundsIndexInAPatternAFilterAndAggregates) {
    expectRows("UNWIND [['Remy'], []] AS l MATCH (p:Person {name: l[0]}) RETURN p.name", {{"Remy"}});
    expectRows("MATCH (n:Person) WHERE [1, 2, 3][5] IS NULL RETURN count(n) AS c", {{"8"}});
    expectRows("UNWIND [[1, 2], [3], []] AS l RETURN count(l[1]) AS a, sum(l[0]) AS b, collect(l[2]) AS c",
               {{"1", "4", "[]"}});
}

TEST_F(ListOutOfBoundsTest, setsAnExistingPropertyToAnOutOfBoundsElement) {
    applyWrite("CREATE (:W {x: 1})");

    expectWriteRows("MATCH (w:W) SET w.x = [1, 2][5] RETURN w.x AS v", {{"null"}});
}

TEST_F(ListOutOfBoundsTest, setsANewPropertyToAnOutOfBoundsElement) {
    applyWrite("CREATE (:W {x: 1})");

    expectWriteRows("MATCH (w:W) SET w.y = [1, 2][5] RETURN w.y AS v", {{"null"}});
}

TEST_F(ListOutOfBoundsTest, readsPerRowIndexesAndSlicesOverManyChunks) {
    expectRows("UNWIND range(0, 199999) AS i WITH [10, 20, 30][i % 9 - 4] AS v "
               "RETURN count(v) AS present, count(*) AS rows, sum(v) AS total",
               {{"133333", "200000", "2666650"}});
    expectRows("UNWIND range(0, 199999) AS i WITH range(0, i % 5)[i % 7 - 3] AS v RETURN count(v) AS present, sum(v) AS total",
               {{"148571", "199999"}});
    expectRows("UNWIND range(0, 199999) AS i WITH range(0, i % 5)[(i % 7 - 3)..(i % 4)] AS s RETURN sum(size(s)) AS total",
               {{"108575"}});
}

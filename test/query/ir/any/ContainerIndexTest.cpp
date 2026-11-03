#include <gtest/gtest.h>

#include <string>
#include <string_view>

#include "QueryInterpreterV3.h"

#include "WriteQueryTest.h"

using namespace db;
using namespace turing::test;

class ContainerIndexTest : public WriteQueryTest {
protected:
    void expectPositionError(std::string_view query) {
        expectError(query, "A list index reads a list, and this row holds a value that is not one");
    }

    void expectKeyError(std::string_view query, std::string_view key) {
        expectError(query, "Type mismatch: key '" + std::string(key) + "' is read off a map, a node or a relationship");
    }

    void expectIndexError(std::string_view query) {
        expectError(query, "An index is an integer position or a string key, and this row holds a value that is neither");
    }
};

TEST_F(ContainerIndexTest, readsAListByAPositionACellHolds) {
    expectRows("RETURN [10, 20, 30][[1, 'a'][0]]", {{"20"}});
    expectRows("UNWIND [1, 2, 'a'] AS i WITH i WHERE i <> 'a' RETURN [10, 20, 30][i]", {{"20"}, {"30"}});
    expectRows("UNWIND [0, 'a'] AS i WITH i WHERE i = 0 RETURN [1, 2][i]", {{"1"}});
}

TEST_F(ContainerIndexTest, readsTheElementTypeOfATypedList) {
    expectRows("WITH [1, 'a'][0] AS i RETURN [10, 20, 30][i] + 1", {{"21"}});
    expectRows("WITH [1, 'a'][0] AS i RETURN ['x', 'y'][i] + 'z'", {{"yz"}});
}

TEST_F(ContainerIndexTest, readsAMapByAKeyACellHolds) {
    expectRows("WITH ['a', 1][0] AS k, {a: 1} AS m RETURN m[k]", {{"1"}});
    expectRows("WITH ['b', 1][0] AS k, {a: 1} AS m RETURN m[k]", {{"null"}});
}

TEST_F(ContainerIndexTest, raisesOnAMapReadByAPosition) {
    expectPositionError("UNWIND ['a', 0] AS k WITH k, {a: 1} AS m RETURN m[k]");
    expectPositionError("WITH {a: 1} AS m, [1, 'a'][0] AS k RETURN m[k]");
}

TEST_F(ContainerIndexTest, raisesOnAListReadByAKey) {
    expectKeyError("UNWIND [1, 'a'] AS b RETURN [1, 2, 3][b]", "a");
}

TEST_F(ContainerIndexTest, readsACellByACell) {
    expectRows("UNWIND [[[10, 20], 1], [{a: 'x'}, 'a']] AS p RETURN p[0][p[1]]", {{"20"}, {"x"}});
    expectRows("WITH [1, 'a'][0] AS k RETURN [[1, 2], [3, 4]][k][k]", {{"4"}});
}

TEST_F(ContainerIndexTest, readsAPropertyOfANodeACellHolds) {
    expectRows("MATCH (p:Person {name: 'Remy'}) WITH [p, 'name'] AS l RETURN l[0][l[1]]", {{"Remy"}});
    expectRows("MATCH (p:Person {name: 'Remy'}) WITH [p, 'age'] AS l RETURN l[0][l[1]]", {{"32"}});
    expectRows("MATCH (p:Person {name: 'Remy'}) WITH [p, 'nope'] AS l RETURN l[0][l[1]]", {{"null"}});
    expectPositionError("MATCH (p:Person {name: 'Remy'}) WITH [p, 0] AS l RETURN l[0][l[1]]");
}

TEST_F(ContainerIndexTest, readsAPropertyOfAnEdgeACellHolds) {
    expectRows("MATCH (:Person {name: 'Remy'})-[e:KNOWS_WELL]->(:Person {name: 'Adam'}) "
               "WITH [e, 'name', 'duration', 'nope'] AS l RETURN l[0][l[1]], l[0][l[2]], l[0][l[3]]",
               {{"Remy -> Adam", "20", "null"}});
    expectPositionError("MATCH (:Person {name: 'Remy'})-[e:KNOWS_WELL]->(:Person {name: 'Adam'}) "
                        "WITH [e, 0] AS l RETURN l[0][l[1]]");
}

TEST_F(ContainerIndexTest, readsAnEntityOutOfAListOfEntities) {
    expectRows("MATCH (:Person {name: 'Remy'})-[e:KNOWS_WELL*1..1]->(b) WITH e, [0, 'x'][0] AS i RETURN e[i].name",
               {{"Remy -> Adam"}});
    expectRows("MATCH (p:Person) WHERE p.name IN ['Remy', 'Adam'] WITH p ORDER BY p.name WITH collect(p) AS l "
               "WITH l, [-1, 'x'][0] AS i RETURN l[i].name, l[i + 5]",
               {{"Remy", "null"}});
    expectRows("MATCH p = (:Person {name: 'Remy'})-[:KNOWS_WELL]->(:Person {name: 'Adam'}) "
               "WITH nodes(p) AS l, [1, 'x'][0] AS i RETURN l[i].name",
               {{"Adam"}});
}

TEST_F(ContainerIndexTest, readsNullThroughANullIndex) {
    expectRows("WITH [null, 'a', 1][0] AS k RETURN [10, 20][k], {a: 1}[k]", {{"null", "null"}});
    expectRows("WITH [1, 'a'][0] AS b, [null, 1][0] AS k RETURN b[k]", {{"null"}});
}

TEST_F(ContainerIndexTest, readsNullThroughANullBase) {
    expectRows("WITH [null, 1, 'a'][0] AS b, [0, 'a'][0] AS k RETURN b[k]", {{"null"}});
    expectRows("WITH [null, 1, 'a'][0] AS b, ['a', 0][0] AS k RETURN b[k]", {{"null"}});
    expectRows("WITH [null, 1, 'a'][0] AS b, [true, 'a'][0] AS k RETURN b[k]", {{"null"}});
}

TEST_F(ContainerIndexTest, countsANegativePositionFromTheEnd) {
    expectRows("WITH [-1, 'a'][0] AS k RETURN [10, 20, 30][k]", {{"30"}});
    expectRows("WITH [-3, 'a'][0] AS k RETURN [10, 20, 30][k]", {{"10"}});
}

TEST_F(ContainerIndexTest, readsNullPastEitherEnd) {
    expectRows("WITH [-4, 'a'][0] AS k RETURN [10, 20, 30][k]", {{"null"}});
    expectRows("WITH [3, 'a'][0] AS k RETURN [10, 20, 30][k]", {{"null"}});
    expectRows("WITH [100, 'a'][0] AS k RETURN [10, 20, 30][k]", {{"null"}});
}

TEST_F(ContainerIndexTest, raisesOnAnIndexThatIsNeitherAPositionNorAKey) {
    expectIndexError("RETURN [10, 20][[1.0, 'a'][0]]");
    expectIndexError("WITH [true, 'a'][0] AS k RETURN [10, 20][k]");
    expectIndexError("WITH [true, 'a'][0] AS k RETURN {a: 1}[k]");
    expectIndexError("WITH [[0], 'a'][0] AS k RETURN [10, 20][k]");
    expectIndexError("WITH [{a: 0}, 'a'][0] AS k RETURN [10, 20][k]");
    expectIndexError("WITH [[0], 'a'][0] AS k RETURN {a: 1}[k]");
    expectIndexError("WITH [[1], 'a'][0] AS b, [true, 1][0] AS k RETURN b[k]");
    expectIndexError("WITH [{a: 1}, 'a'][0] AS b, [1.0, 1][0] AS k RETURN b[k]");
}

TEST_F(ContainerIndexTest, raisesOnACellHoldingNoContainer) {
    expectKeyError("WITH [1, 'a'][0] AS b, ['a', 0][0] AS k RETURN b[k]", "a");
    expectPositionError("WITH [1, 'a'][0] AS b, [0, 'a'][0] AS k RETURN b[k]");
    expectPositionError("WITH ['xyz', 1][0] AS b, [0, 'a'][0] AS k RETURN b[k]");
    expectIndexError("WITH [1, 'a'][0] AS b, [true, 1][0] AS k RETURN b[k]");
}

TEST_F(ContainerIndexTest, raisesOnAKeyOfATemporalCell) {
    expectKeyError("WITH [datetime('2020-01-01T00:00:00Z'), 1][0] AS b, ['year', 0][0] AS k RETURN b[k]", "year");
    expectKeyError("WITH [duration({hours: 1}), 1][0] AS b, ['hours', 0][0] AS k RETURN b[k]", "hours");
}

TEST_F(ContainerIndexTest, raisesOnAListCellReadByAKeyAndAMapCellReadByAPosition) {
    expectKeyError("WITH [[1, 2], 1][0] AS b, ['a', 0][0] AS k RETURN b[k]", "a");
    expectPositionError("WITH [{a: 1}, 1][0] AS b, [0, 'a'][0] AS k RETURN b[k]");
    expectRows("WITH [{a: 1}, 1][0] AS b, ['b', 0][0] AS k RETURN b[k]", {{"null"}});
}

// A chunk of 4 rows puts the null bases, the null indexes and the two kinds of read on
// both sides of several chunk boundaries
TEST_F(ContainerIndexTest, readsEveryRowAcrossChunks) {
    _interpreter->setChunkSize(4);

    expectRowsInOrder("UNWIND range(0, 13) AS r "
                      "WITH r, "
                      "CASE WHEN r % 5 = 0 THEN null WHEN r % 2 = 0 THEN [r, r + 1] ELSE {k: r} END AS c, "
                      "CASE WHEN r % 3 = 0 THEN null WHEN r % 2 = 0 THEN 1 ELSE 'k' END AS i "
                      "RETURN r, c[i]",
                      {{"0", "null"}, {"1", "1"}, {"2", "3"}, {"3", "null"}, {"4", "5"},
                       {"5", "null"}, {"6", "null"}, {"7", "7"}, {"8", "9"}, {"9", "null"},
                       {"10", "null"}, {"11", "11"}, {"12", "null"}, {"13", "13"}});
}

TEST_F(ContainerIndexTest, raisesOnARowPastTheFirstChunk) {
    _interpreter->setChunkSize(4);

    expectKeyError("UNWIND range(0, 13) AS r WITH CASE WHEN r = 9 THEN 'x' ELSE r % 2 END AS i "
                   "RETURN [10, 20][i]",
                   "x");
}

// One in seven rows reads a null index, and every other row reads the value it was built
// with, over more rows than the default chunk holds
TEST_F(ContainerIndexTest, readsEveryRowOfSeveralDefaultChunks) {
    expectRows("UNWIND range(0, 140000) AS r "
               "WITH r, "
               "CASE WHEN r % 2 = 0 THEN [r] ELSE {k: r} END AS c, "
               "CASE WHEN r % 7 = 0 THEN null WHEN r % 2 = 0 THEN 0 ELSE 'k' END AS i "
               "WHERE c[i] = r RETURN count(*)",
               {{"120000"}});

    expectPositionError("UNWIND range(0, 140000) AS r "
                        "WITH CASE WHEN r = 100000 THEN {a: 1} ELSE [r] END AS c, [0, 'a'][0] AS i "
                        "WITH c[i] AS v RETURN count(v)");
}

TEST_F(ContainerIndexTest, readsInsideAListComprehension) {
    expectRows("UNWIND [1, 'a'] AS i WITH i WHERE i = 1 RETURN [l IN [[10, 20], [30, 40]] | l[i]]", {{"[20, 40]"}});
    expectRows("RETURN [p IN [[[10, 20], 1], [{a: 'x'}, 'a']] | p[0][p[1]]]", {{"[20, x]"}});
}

TEST_F(ContainerIndexTest, readsInsideAReduce) {
    expectRows("RETURN reduce(s = 0, p IN [[[10, 20], 1], [[5, 6, 7], -1]] | s + p[0][p[1]])", {{"27"}});
}

TEST_F(ContainerIndexTest, readsOnlyTheRowsACaseBranchTakes) {
    expectRows("UNWIND [[[10, 20], 1], [{a: 'x'}, 'a'], [1, true]] AS p "
               "RETURN CASE WHEN p[1] = true THEN 'none' ELSE p[0][p[1]] END",
               {{"20"}, {"x"}, {"none"}});
}

TEST_F(ContainerIndexTest, readsOnlyTheRowsTheEarlierCoalesceArgumentsLeaveNull) {
    expectRows("UNWIND [[null, [10, 20], 1], [5, 1, true]] AS p RETURN coalesce(p[0], p[1][p[2]])",
               {{"20"}, {"5"}});
}

TEST_F(ContainerIndexTest, filtersOnTheReadValue) {
    expectRows("UNWIND [[[10, 20], 1], [{a: 'x'}, 'a']] AS p WITH p WHERE p[0][p[1]] = 20 RETURN p[1]", {{"1"}});
}

TEST_F(ContainerIndexTest, ordersByTheReadValue) {
    expectRowsInOrder("UNWIND [[[10, 20], 1], [[30, 5], 0], [[1, 2], -1]] AS p RETURN p[1] AS i ORDER BY p[0][p[1]]",
                      {{"-1"}, {"1"}, {"0"}});
}

TEST_F(ContainerIndexTest, groupsByTheReadValue) {
    expectRows("UNWIND [[[10, 20], 1], [[20], 0], [{a: 'x'}, 'a']] AS p RETURN p[0][p[1]] AS v, count(*) AS c",
               {{"20", "2"}, {"x", "1"}});
}

TEST_F(ContainerIndexTest, aggregatesTheReadValue) {
    expectRows("UNWIND [[[10, 20], 1], [[5], 0], [{a: 7}, 'a']] AS p RETURN collect(p[0][p[1]])", {{"[20, 5, 7]"}});
}

TEST_F(ContainerIndexTest, raisesOnAReadNothingUses) {
    expectKeyError("UNWIND [1, 'a'] AS b WITH b, [1, 2, 3][b] AS unused RETURN count(*)", "a");
}

// A pattern seeded off the element a position held in a cell reads, as off the one an
// integer position reads
TEST_F(ContainerIndexTest, seedsAPatternOffTheReadElement) {
    RowSink typedSink;
    const QueryStatus typedStatus = runQuery("WITH [0, 1, 6, 99] AS ss UNWIND range(0, 3) AS i "
                                             "MATCH (m)-[e]->(n:Person) WHERE n = ss[i] RETURN m.name, n.name",
                                             &typedSink);
    ASSERT_TRUE(typedStatus.isOk()) << typedStatus.getError();

    Rows expected;
    typedSink.sortedRows(expected);
    ASSERT_FALSE(expected.empty());

    expectRows("WITH [0, 1, 6, 99] AS ss UNWIND range(0, 3) AS i WITH ss, [i, 'x'][0] AS j "
               "MATCH (m)-[e]->(n:Person) WHERE n = ss[j] RETURN m.name, n.name",
               expected);
}

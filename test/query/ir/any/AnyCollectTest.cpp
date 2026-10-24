#include <gtest/gtest.h>

#include <string_view>

#include "CallV3Test.h"
#include "IRTestRows.h"

using namespace turing::test;

// collect over values of type any gathers each value in its own type, in the order the rows
// arrive, and skips the nulls. Remy is node 0; only Remy and Adam have an age, 32 each;
// Maxime was born on 24/07 and Luc on 28/05.
class AnyCollectTest : public CallV3Test {
protected:
    void expectRows(std::string_view query, const Rows& expected) {
        RowSink sink;
        runQuery(query, sink);

        Rows rows;
        sink.sortedRows(rows);

        EXPECT_EQ(rows, expected) << query;
    }
};

TEST_F(AnyCollectTest, collectsEachValueInItsOwnType) {
    expectRows("UNWIND [1, 'a', true, 2.5, [1], {a: 1}] AS x RETURN collect(x)",
               {{"[1, a, true, 2.500000, [1], {a: 1}]"}});
}

TEST_F(AnyCollectTest, collectsTemporalValues) {
    expectRows("UNWIND [datetime('2020-01-01T00:00:00Z'), duration({days: 1}), 'x'] AS x RETURN collect(x)",
               {{"[2020-01-01T00:00:00Z, PT24H, x]"}});
}

TEST_F(AnyCollectTest, skipsNulls) {
    expectRows("UNWIND [1, null, 'a', null] AS x RETURN collect(x)", {{"[1, a]"}});
}

TEST_F(AnyCollectTest, collectsNothingFromNulls) {
    expectRows("UNWIND [null, 'a'] AS x WITH x WHERE x IS NULL RETURN collect(x)", {{"[]"}});
    expectRows("MATCH (p:Person) WHERE p.name = 'Nobody' RETURN collect(CASE WHEN true THEN p.name ELSE 1 END)",
               {{"[]"}});
}

TEST_F(AnyCollectTest, collectsPerGroup) {
    expectRows("UNWIND [['g', 1], ['g', 'a'], ['h', true], ['g', null]] AS pair RETURN pair[0] AS k, collect(pair[1])",
               {{"g", "[1, a]"}, {"h", "[true]"}});
}

TEST_F(AnyCollectTest, collectsACasePerRow) {
    expectRows("MATCH (p:Person) WITH collect(CASE WHEN p.age IS NULL THEN p.name ELSE p.age END) AS c "
               "RETURN size(c), 32 IN c, 'Luc' IN c, 'Remy' IN c",
               {{"8", "true", "true", "false"}});
}

TEST_F(AnyCollectTest, collectsACoalesce) {
    expectRows("MATCH (p:Person) WITH collect(coalesce(p.age, p.dob)) AS c RETURN size(c), '24/07' IN c, 32 IN c",
               {{"4", "true", "true"}});
}

TEST_F(AnyCollectTest, collectsANodeBesideAScalar) {
    expectRows("MATCH (r:Person {name: 'Remy'}) UNWIND [r, 'x'] AS v RETURN collect(v)", {{"[0, x]"}});
}

TEST_F(AnyCollectTest, collectsDistinctValues) {
    expectRows("UNWIND [1, 'a', 1, 1.0, 'a', null, true] AS x WITH collect(DISTINCT x) AS c "
               "RETURN size(c), 'a' IN c, true IN c, 1 IN c",
               {{"3", "true", "true", "true"}});
}

TEST_F(AnyCollectTest, collectsElementsOfMixedLists) {
    expectRows("UNWIND [[1, 'a'], ['b', 2]] AS pair RETURN collect(pair[0]), collect(pair[1])",
               {{"[1, b]", "[a, 2]"}});
}

TEST_F(AnyCollectTest, readsTheCollectedList) {
    expectRows("UNWIND [3, 'a', 1] AS x WITH collect(x) AS xs RETURN xs[0], xs[1], size(xs)",
               {{"3", "a", "3"}});
}

TEST_F(AnyCollectTest, unwindsTheCollectedList) {
    expectRows("UNWIND [3, 'a', 1] AS x WITH collect(x) AS xs UNWIND xs AS y RETURN y",
               {{"1"}, {"3"}, {"a"}});
}

TEST_F(AnyCollectTest, collectsBesideATypedAggregate) {
    expectRows("UNWIND [1, 'a', 2.5] AS x RETURN count(x), collect(x)", {{"3", "[1, a, 2.500000]"}});
}

int main(int argc, char** argv) {
    return turing::test::turingTestMain(argc, argv);
}

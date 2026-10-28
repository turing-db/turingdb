#include <gtest/gtest.h>

#include <stddef.h>

#include <algorithm>
#include <string>
#include <string_view>

#include "HashJoinQueryTest.h"
#include "StringRowSink.h"
#include "TuringTest.h"

using namespace db;
using namespace turing::test;

// A join whose two keys are columns of different types: a list element, a map value, a
// parsed or computed value, or a float against an integer property. It has to keep the
// rows the product and its filter keep.
class HashJoinCrossTypeKeyTest : public HashJoinQueryTest {
protected:
    void expectJoinMatchesTheProduct(std::string_view query, size_t expectedRows) {
        std::string program;
        dbProgram(query, program);
        EXPECT_TRUE(contains(program, "db.hash_join")) << program;
        EXPECT_FALSE(contains(program, "db.cross_product")) << program;

        StringRowSink joined;
        runQuery(query, joined);

        StringRowSink product;
        runQuery(query, product, false);

        Rows joinedRows = joined.getRows();
        Rows productRows = product.getRows();
        std::sort(joinedRows.begin(), joinedRows.end());
        std::sort(productRows.begin(), productRows.end());

        EXPECT_EQ(joinedRows.size(), expectedRows) << "query: " << query;
        EXPECT_EQ(joinedRows, productRows) << "query: " << query;
    }

    static size_t countOccurrences(std::string_view text, std::string_view part) {
        size_t count = 0;
        for (size_t position = text.find(part); position != std::string_view::npos; position = text.find(part, position + 1)) {
            count++;
        }

        return count;
    }
};

// Two key lookups per unwound pair are two joins, each reading the label once
TEST_F(HashJoinCrossTypeKeyTest, joinsBothKeysOfAnUnwoundPair) {
    const std::string_view query = "UNWIND [['Remy', 'Adam'], ['Luc', 'Maxime'], ['Nobody', 'Remy']] AS p "
                                   "MATCH (a:Person {name: p[0]}) MATCH (b:Person {name: p[1]}) "
                                   "RETURN a.name, b.name";

    std::string program;
    dbProgram(query, program);
    EXPECT_EQ(countOccurrences(program, "db.hash_join"), 2u) << program;
    EXPECT_FALSE(contains(program, "db.cross_product")) << program;

    StringRowSink sink;
    runQuery(query, sink);

    Rows rows = sink.getRows();
    std::sort(rows.begin(), rows.end());

    const Rows expected {{"Luc", "Maxime"}, {"Remy", "Adam"}};
    EXPECT_EQ(rows, expected);
}

// 32 and 32.0 both hold Remy's and Adam's age; the strings hold no age at all
TEST_F(HashJoinCrossTypeKeyTest, joinsAnElementOfAMixedList) {
    expectJoinMatchesTheProduct("UNWIND [[32, 'x'], ['y', 1], [32.0, 2]] AS p "
                                "MATCH (a:Person) WHERE a.age = p[0] RETURN a.name, p[1]",
                                4);
}

TEST_F(HashJoinCrossTypeKeyTest, joinsAMapValue) {
    expectJoinMatchesTheProduct("UNWIND [{src: 'Remy'}, {src: 'Luc'}, {src: 7}] AS row "
                                "MATCH (a:Person {name: row.src}) RETURN a.name",
                                2);
}

TEST_F(HashJoinCrossTypeKeyTest, joinsAParsedKey) {
    expectJoinMatchesTheProduct("UNWIND ['32', 'x'] AS s MATCH (a:Person) WHERE a.age = toInteger(s) RETURN a.name, s",
                                2);
}

// Five edges last 20 and one lasts 200
TEST_F(HashJoinCrossTypeKeyTest, joinsAComputedKey) {
    expectJoinMatchesTheProduct("UNWIND [19, 199] AS x MATCH (a)-[e]->(b) WHERE e.duration = x + 1 RETURN a.name, b.name, x",
                                6);
}

TEST_F(HashJoinCrossTypeKeyTest, joinsAnIntegerPropertyAgainstFloats) {
    expectJoinMatchesTheProduct("UNWIND [32.0, 32.5] AS x MATCH (a:Person) WHERE a.age = x RETURN a.name, x",
                                2);
}

TEST_F(HashJoinCrossTypeKeyTest, joinsOneRowPerRepeatedKey) {
    expectJoinMatchesTheProduct("UNWIND [32, 32] AS x MATCH (a:Person {age: x}) RETURN a.name, x",
                                4);
}

// 2^53 + 1 has no double, so it equals no float - not even the 2^53 it rounds to
TEST_F(HashJoinCrossTypeKeyTest, joinsAnIntegerToAFloatByExactValue) {
    expectJoinMatchesTheProduct("UNWIND [9007199254740993, 1] AS x UNWIND [9007199254740992.0, 1.0] AS y "
                                "WITH x, y WHERE x = y RETURN x, y",
                                1);
}

// [1, null] = [1, null] is null, not true
TEST_F(HashJoinCrossTypeKeyTest, leavesAListHoldingANullUnmatched) {
    expectJoinMatchesTheProduct("UNWIND [[1, null], [2]] AS x UNWIND [[1, null], [2]] AS y "
                                "WITH x, y WHERE x = y RETURN x, y",
                                1);
}

// A null inside a nested list compares as a value: [[1, null]] = [[1, null]] is true
TEST_F(HashJoinCrossTypeKeyTest, matchesAListHoldingANestedNull) {
    expectJoinMatchesTheProduct("UNWIND [[[1, null]], [[2]]] AS x UNWIND [[[1, null]], [[3]]] AS y "
                                "WITH x, y WHERE x = y RETURN x, y",
                                1);
}

TEST_F(HashJoinCrossTypeKeyTest, leavesACellHoldingAListWithANullUnmatched) {
    expectJoinMatchesTheProduct("UNWIND [[1, null], 'a'] AS x UNWIND [[1, null], 'a'] AS y "
                                "WITH x, y WHERE x = y RETURN x, y",
                                1);
}

TEST_F(HashJoinCrossTypeKeyTest, leavesAMapValueHoldingAListWithANullUnmatched) {
    expectJoinMatchesTheProduct("UNWIND [{k: [1, null]}, {k: [2]}] AS r UNWIND [[1, null], [2]] AS y "
                                "WITH r, y WHERE r.k = y RETURN y",
                                1);
}

int main(int argc, char** argv) {
    return turing::test::turingTestMain(argc, argv);
}

#include <gtest/gtest.h>

#include <string>
#include <string_view>

#include "QueryStatus.h"

#include "IRTestRows.h"
#include "WriteQueryTest.h"

using namespace db;
using namespace turing::test;

namespace {

constexpr std::string_view REMY = "MATCH (p:Person {name: 'Remy'}) ";
constexpr std::string_view REMY_KNOWS_ADAM = "MATCH (:Person {name: 'Remy'})-[e:KNOWS_WELL]->(:Person {name: 'Adam'}) ";

}

class IndexTypeErrorTest : public WriteQueryTest {
protected:
    void expectMessage(std::string_view query, std::string_view message) {
        RowSink sink;
        const QueryStatus status = runQuery(query, &sink);

        ASSERT_FALSE(status.isOk()) << "query: " << query;
        EXPECT_EQ(status.getError(), message) << "query: " << query;
    }

    void expectKeyedBy(std::string_view query, std::string_view base, std::string_view index) {
        expectMessage(query, "A " + std::string(base) + " is indexed by a string key, not by '" + std::string(index) + "'");
    }

    void expectListIndexedBy(std::string_view query, std::string_view index) {
        expectMessage(query, "A list index expression must be an integer, not '" + std::string(index) + "'");
    }

    void expectNotIndexable(std::string_view query, std::string_view base) {
        expectMessage(query,
                      "Index operator [] can only be applied to a list, a map, a node or a relationship, not '"
                          + std::string(base) + "'");
    }

    void expectNoProperty(std::string_view query, std::string_view base, std::string_view key) {
        expectMessage(query,
                      "Type mismatch: a value of type '" + std::string(base) + "' has no property '" + std::string(key) + "'");
    }
};

TEST_F(IndexTypeErrorTest, readsAMapByAnIndexTheRowHolds) {
    expectKeyedBy("UNWIND [1, 'k'] AS b RETURN {k: 1}[b]", "map", "Integer");
    expectKeyedBy("UNWIND [1.5, 'k'] AS b RETURN {k: 1}[b]", "map", "Double");
    expectKeyedBy("UNWIND [true, 'k'] AS b RETURN {k: 1}[b]", "map", "Bool");
    expectKeyedBy("UNWIND [[1], 'k'] AS b RETURN {k: 1}[b]", "map", "List");
    expectKeyedBy("UNWIND [{a: 1}, 'k'] AS b RETURN {k: 1}[b]", "map", "Map");
    expectKeyedBy("UNWIND [datetime('2020-01-01T00:00:00Z'), 'k'] AS b RETURN {k: 1}[b]", "map", "DateTime");
    expectKeyedBy("UNWIND [duration({hours: 1}), 'k'] AS b RETURN {k: 1}[b]", "map", "Duration");
    expectKeyedBy(std::string(REMY) + "UNWIND [p, 'k'] AS b RETURN {k: 1}[b]", "map", "NodePattern");
    expectKeyedBy(std::string(REMY_KNOWS_ADAM) + "UNWIND [e, 'k'] AS b RETURN {k: 1}[b]", "map", "EdgePattern");
}

TEST_F(IndexTypeErrorTest, readsAListByAnIndexTheRowHolds) {
    expectListIndexedBy("RETURN [1, 2, 3][['a', 0][0]]", "String");
    expectListIndexedBy("UNWIND [1.5, 0] AS i RETURN [1, 2, 3][i]", "Double");
    expectListIndexedBy("UNWIND [true, 0] AS i RETURN [1, 2, 3][i]", "Bool");
    expectListIndexedBy("UNWIND [[0], 0] AS i RETURN [1, 2, 3][i]", "List");
    expectListIndexedBy("UNWIND [{a: 1}, 0] AS i RETURN [1, 2, 3][i]", "Map");
    expectListIndexedBy(std::string(REMY) + "UNWIND [p, 0] AS i RETURN [1, 2, 3][i]", "NodePattern");
}

TEST_F(IndexTypeErrorTest, readsAKeyedValueTheRowHoldsByAPositionItHolds) {
    expectKeyedBy("WITH [{a: 1}, 0] AS l RETURN l[0][l[1]]", "map", "Integer");
    expectKeyedBy(std::string(REMY) + "WITH [p, 0] AS l RETURN l[0][l[1]]", "node", "Integer");
    expectKeyedBy(std::string(REMY_KNOWS_ADAM) + "WITH [e, 1.5] AS l RETURN l[0][l[1]]", "relationship", "Double");
}

TEST_F(IndexTypeErrorTest, readsAListTheRowHoldsByAKeyItHolds) {
    expectListIndexedBy("WITH [[1, 2], 'a'] AS l RETURN l[0][l[1]]", "String");
    expectListIndexedBy("WITH [[1, 2], true] AS l RETURN l[0][l[1]]", "Bool");
}

TEST_F(IndexTypeErrorTest, indexesAValueTheRowHoldsThatIsNoContainer) {
    expectNotIndexable("WITH [1, 0, 'x'] AS l RETURN l[0][l[1]]", "Integer");
    expectNotIndexable("WITH ['s', 0] AS l RETURN l[0][l[1]]", "String");
    expectNotIndexable("WITH [1.5, 'k'] AS l RETURN l[0][l[1]]", "Double");
    expectNotIndexable("WITH [true, 'k'] AS l RETURN l[0][l[1]]", "Bool");
    expectNotIndexable("WITH [datetime('2020-01-01T00:00:00Z'), 'year'] AS l RETURN l[0][l[1]]", "DateTime");
    expectNotIndexable("WITH [duration({hours: 1}), 'hours'] AS l RETURN l[0][l[1]]", "Duration");
}

TEST_F(IndexTypeErrorTest, readsAPositionOfAValueTheRowHolds) {
    expectKeyedBy("UNWIND [[1], {a: 1}] AS x RETURN x[0]", "map", "Integer");
    expectKeyedBy(std::string(REMY) + "UNWIND [[1], p] AS x RETURN x[0]", "node", "Integer");
    expectKeyedBy(std::string(REMY_KNOWS_ADAM) + "UNWIND [[1], e] AS x RETURN x[0]", "relationship", "Integer");
    expectNotIndexable("UNWIND [[1], 1] AS x RETURN x[0]", "Integer");
    expectNotIndexable("UNWIND [[1], 'a'] AS x RETURN x[0]", "String");
    expectNotIndexable("UNWIND [[1], 1.5] AS x RETURN x[0]", "Double");
    expectNotIndexable("UNWIND [[1], true] AS x RETURN x[0]", "Bool");
    expectNotIndexable("UNWIND [[1], datetime('2020-01-01T00:00:00Z')] AS x RETURN x[-1]", "DateTime");
    expectNotIndexable("UNWIND [[1], duration({hours: 1})] AS x RETURN x[0]", "Duration");
}

TEST_F(IndexTypeErrorTest, readsAKeyOfAValueTheRowHolds) {
    expectListIndexedBy("UNWIND [{a: 1}, [1]] AS m RETURN m['a']", "String");
    expectNotIndexable("UNWIND [{a: 1}, 1] AS m RETURN m['a']", "Integer");
    expectNotIndexable("UNWIND [{a: 1}, 'x'] AS m RETURN m['a']", "String");
    expectNotIndexable("UNWIND [{a: 1}, 1.5] AS m WITH m, 'a' AS k RETURN m[k]", "Double");
    expectNotIndexable("UNWIND [{a: 1}, true] AS m RETURN m['a']", "Bool");
    expectNotIndexable("UNWIND [{a: 1}, datetime('2020-01-01T00:00:00Z')] AS m RETURN m['year']", "DateTime");
    expectNotIndexable("UNWIND [{a: 1}, duration({hours: 1})] AS m RETURN m['hours']", "Duration");
}

TEST_F(IndexTypeErrorTest, readsAPropertyOfAValueTheRowHolds) {
    expectNoProperty("UNWIND [{a: 1}, 1] AS m RETURN m.a", "Integer", "a");
    expectNoProperty("UNWIND [{a: 1}, 'x'] AS m RETURN m.a", "String", "a");
    expectNoProperty("UNWIND [{a: 1}, 1.5] AS m RETURN m.a", "Double", "a");
    expectNoProperty("UNWIND [{a: 1}, true] AS m RETURN m.b", "Bool", "b");
    expectNoProperty("UNWIND [{a: 1}, [1]] AS m RETURN m {.a}", "List", "a");
}

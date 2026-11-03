#include <gtest/gtest.h>

#include <stddef.h>
#include <stdint.h>

#include <algorithm>
#include <string>
#include <string_view>
#include <utility>

#include "IRTestRows.h"
#include "QueryStatus.h"
#include "WriteQueryTest.h"

using namespace db;
using namespace turing::test;

// A read in a region of a CASE or a coalesce shares the read of the same rows made outside
// it, and the regions every row reaches run before the op. The rows have to come out as
// they did when each region read the property again, and a branch or an argument still runs
// only over the rows reaching it.
class SelectionRegionReadsCypherTest : public WriteQueryTest {
protected:
    // More rows than one chunk holds. Item i holds v = i where i % 3 = 0 and no v elsewhere,
    // so the rows with no v straddle every chunk boundary.
    static constexpr int64_t ITEM_COUNT = 3 * 65536 + 7;

    void createItems() {
        applyWrite("UNWIND range(0, " + std::to_string(ITEM_COUNT - 1) + ") AS i "
                   "CREATE (:Item {i: i, v: CASE WHEN i % 3 = 0 THEN i END})");
    }

    // Prints the first row that differs rather than every row
    void expectManyRows(std::string_view query, Rows expected) {
        RowSink sink;
        const QueryStatus status = runQuery(query, &sink);
        ASSERT_TRUE(status.isOk()) << "query: " << query << "\nerror: " << status.getError();

        Rows rows;
        sink.sortedRows(rows);
        std::sort(expected.begin(), expected.end());

        ASSERT_EQ(rows.size(), expected.size()) << "query: " << query;

        const auto [actualIt, expectedIt] = std::mismatch(rows.begin(), rows.end(), expected.begin());
        if (actualIt == rows.end()) {
            return;
        }

        std::string actualText;
        describeRows(Rows {*actualIt}, actualText);

        std::string expectedText;
        describeRows(Rows {*expectedIt}, expectedText);

        ADD_FAILURE() << "query: " << query << "\nfirst differing row: " << actualText << "expected: " << expectedText;
    }

    static void appendValueOrNull(Row& row, bool present, int64_t value) {
        row.push_back(present ? std::to_string(value) : "null");
    }

    static void swapLastTwoColumns(Rows& rows) {
        for (Row& row : rows) {
            std::swap(row[row.size() - 2], row[row.size() - 1]);
        }
    }
};

TEST_F(SelectionRegionReadsCypherTest, coalescesAPropertyReadBesideIt) {
    createItems();

    Rows expected;
    for (int64_t i = 0; i < ITEM_COUNT; i++) {
        const bool hasValue = i % 3 == 0;
        Row row {std::to_string(i)};
        appendValueOrNull(row, hasValue, i + 1);
        row.push_back(std::to_string(hasValue ? i : -i));
        expected.push_back(row);
    }

    expectManyRows("MATCH (n:Item) RETURN n.i, n.v + 1, coalesce(n.v, -n.i)", expected);

    swapLastTwoColumns(expected);
    expectManyRows("MATCH (n:Item) RETURN n.i, coalesce(n.v, -n.i), n.v + 1", expected);
}

TEST_F(SelectionRegionReadsCypherTest, selectsAPropertyReadBesideTheCase) {
    createItems();

    Rows expected;
    for (int64_t i = 0; i < ITEM_COUNT; i++) {
        const bool hasValue = i % 3 == 0;
        const bool even = hasValue && i % 2 == 0;
        Row row {std::to_string(i)};
        appendValueOrNull(row, hasValue, i + 1);
        appendValueOrNull(row, even, i * 2);
        expected.push_back(row);
    }

    expectManyRows("MATCH (n:Item) RETURN n.i, n.v + 1, CASE WHEN n.v % 2 = 0 THEN n.v * 2 END", expected);

    swapLastTwoColumns(expected);
    expectManyRows("MATCH (n:Item) RETURN n.i, CASE WHEN n.v % 2 = 0 THEN n.v * 2 END, n.v + 1", expected);
}

TEST_F(SelectionRegionReadsCypherTest, selectsAPropertyInABranchItsConditionDoesNotRead) {
    createItems();

    Rows expected;
    for (int64_t i = 0; i < ITEM_COUNT; i++) {
        const bool hasValue = i % 3 == 0;
        const bool taken = i % 4 == 0;
        Row row {std::to_string(i)};
        appendValueOrNull(row, hasValue, i);
        appendValueOrNull(row, taken && hasValue, i);
        expected.push_back(row);
    }

    expectManyRows("MATCH (n:Item) RETURN n.i, n.v, CASE WHEN n.i % 4 = 0 THEN n.v END", expected);

    swapLastTwoColumns(expected);
    expectManyRows("MATCH (n:Item) RETURN n.i, CASE WHEN n.i % 4 = 0 THEN n.v END, n.v", expected);
}

TEST_F(SelectionRegionReadsCypherTest, selectsOverBranchesReadingDifferentColumns) {
    createItems();

    Rows expected;
    for (int64_t i = 0; i < ITEM_COUNT; i++) {
        const bool hasValue = i % 3 == 0;
        const int64_t selected = i % 3 == 0 ? i : (i % 3 == 1 ? -i : i * 10);
        Row row {std::to_string(i)};
        appendValueOrNull(row, hasValue, i);
        row.push_back(std::to_string(selected));
        expected.push_back(row);
    }

    expectManyRows("MATCH (n:Item) RETURN n.i, n.v, "
                   "CASE WHEN n.i % 3 = 0 THEN n.v WHEN n.i % 3 = 1 THEN -n.i ELSE n.i * 10 END",
                   expected);
}

TEST_F(SelectionRegionReadsCypherTest, nestsACaseInACoalesceArgument) {
    createItems();

    Rows expected;
    for (int64_t i = 0; i < ITEM_COUNT; i++) {
        const bool hasValue = i % 3 == 0;
        const std::string fallback = i % 2 == 0 ? std::to_string(-i) : "null";
        Row row {std::to_string(i)};
        appendValueOrNull(row, hasValue, i);
        row.push_back(hasValue ? std::to_string(i) : fallback);
        expected.push_back(row);
    }

    expectManyRows("MATCH (n:Item) RETURN n.i, n.v, coalesce(n.v, CASE WHEN n.i % 2 = 0 THEN -n.i END)", expected);
}

TEST_F(SelectionRegionReadsCypherTest, readsAPropertyOverTheElementsOfAList) {
    createItems();

    Rows expected;
    for (int64_t i = 0; i < ITEM_COUNT; i++) {
        const bool hasValue = i % 3 == 0;
        const std::string elements = hasValue ? "[" + std::to_string(i + 1) + ", " + std::to_string(i + 2) + "]"
                                              : "[null, null]";
        Row row {std::to_string(i)};
        appendValueOrNull(row, hasValue, i);
        row.push_back(elements);
        expected.push_back(row);
    }

    expectManyRows("MATCH (n:Item) RETURN n.i, n.v, [x IN range(1, 2) | x + n.v]", expected);

    swapLastTwoColumns(expected);
    expectManyRows("MATCH (n:Item) RETURN n.i, [x IN range(1, 2) | x + n.v], n.v", expected);
}

// Remy and Adam know each other well and are both 32; Ghosts -> Remy starts at an interest,
// so the other six people have no KNOWS_WELL edge out and come back with a null friend.
TEST_F(SelectionRegionReadsCypherTest, readsThePaddedRowsOfAnOptionalMatch) {
    expectRows("MATCH (p:Person) OPTIONAL MATCH (p)-[e:KNOWS_WELL]->(q) "
               "RETURN p.name, q.age, coalesce(q.age, -1), CASE WHEN q.age > 3 THEN q.age + 1 END, "
               "e.duration, coalesce(e.duration, 0)",
               {{"Remy", "32", "32", "33", "20", "20"},
                {"Adam", "32", "32", "33", "20", "20"},
                {"Maxime", "null", "-1", "null", "null", "0"},
                {"Luc", "null", "-1", "null", "null", "0"},
                {"Martina", "null", "-1", "null", "null", "0"},
                {"Suhas", "null", "-1", "null", "null", "0"},
                {"Cyrus", "null", "-1", "null", "null", "0"},
                {"Doruk", "null", "-1", "null", "null", "0"}});
}

// Remy and Adam are 32 before the SET and 33 after it; the six others had no age and get 1
TEST_F(SelectionRegionReadsCypherTest, readsWhatASetWrote) {
    expectWriteRows("MATCH (n:Person) SET n.age = coalesce(n.age, 0) + 1 "
                    "RETURN n.name, n.age, coalesce(n.age, 0), CASE WHEN n.age > 10 THEN n.age END",
                    {{"Remy", "33", "33", "33"},
                     {"Adam", "33", "33", "33"},
                     {"Maxime", "1", "1", "null"},
                     {"Luc", "1", "1", "null"},
                     {"Martina", "1", "1", "null"},
                     {"Suhas", "1", "1", "null"},
                     {"Cyrus", "1", "1", "null"},
                     {"Doruk", "1", "1", "null"}});
}

TEST_F(SelectionRegionReadsCypherTest, computesALaterArgumentOnlyOverTheRowsStillNull) {
    expectRowsInOrder("UNWIND range(0, 8) AS i RETURN i, coalesce(CASE WHEN i % 3 = 0 THEN i END, 100 / (i % 3))",
                      {{"0", "0"}, {"1", "100"}, {"2", "50"}, {"3", "3"}, {"4", "100"}, {"5", "50"},
                       {"6", "6"}, {"7", "100"}, {"8", "50"}});

    createItems();

    Rows expected;
    for (int64_t i = 0; i < ITEM_COUNT; i++) {
        expected.push_back({std::to_string(i), std::to_string(i % 3 == 0 ? i : 100 / (i % 3))});
    }

    expectManyRows("MATCH (n:Item) RETURN n.i, coalesce(CASE WHEN n.i % 3 = 0 THEN n.i END, 100 / (n.i % 3))", expected);
    expectManyRows("MATCH (n:Item) RETURN n.i, coalesce(n.v, 100 / (n.i % 3))", expected);
}

TEST_F(SelectionRegionReadsCypherTest, computesABranchOnlyOverTheRowsItsConditionHoldsFor) {
    expectRowsInOrder("UNWIND range(0, 5) AS i RETURN i, CASE WHEN i % 3 <> 0 THEN 100 / (i % 3) END",
                      {{"0", "null"}, {"1", "100"}, {"2", "50"}, {"3", "null"}, {"4", "100"}, {"5", "50"}});

    createItems();

    Rows expected;
    for (int64_t i = 0; i < ITEM_COUNT; i++) {
        expected.push_back({std::to_string(i), i % 3 == 0 ? "null" : std::to_string(100 / (i % 3))});
    }

    expectManyRows("MATCH (n:Item) RETURN n.i, CASE WHEN n.i % 3 <> 0 THEN 100 / (n.i % 3) END", expected);
    expectManyRows("MATCH (n:Item) RETURN n.i, CASE WHEN n.v IS NULL THEN 100 / (n.i % 3) END", expected);
}

// The CASE an argument holds sees only the rows the earlier arguments left null: the rows
// with a v are the ones its condition would divide by zero on
TEST_F(SelectionRegionReadsCypherTest, computesACaseInALaterArgumentOnlyOverTheRowsStillNull) {
    createItems();

    Rows expected;
    for (int64_t i = 0; i < ITEM_COUNT; i++) {
        const bool hasValue = i % 3 == 0;
        expected.push_back({std::to_string(i), hasValue ? std::to_string(i) : (100 / (i % 3) > 60 ? "1" : "2")});
    }

    expectManyRows("MATCH (n:Item) RETURN n.i, coalesce(n.v, CASE WHEN 100 / (n.i % 3) > 60 THEN 1 ELSE 2 END)",
                   expected);
}

TEST_F(SelectionRegionReadsCypherTest, raisesNothingInAFirstRegionNoRowReaches) {
    expectRows("UNWIND [] AS i RETURN coalesce(100 / 0 + i, 1)", {});
    expectRows("UNWIND [] AS i RETURN coalesce(100 / 0, i)", {});
    expectRows("UNWIND [] AS i RETURN CASE WHEN 100 / 0 > i THEN i END", {});
    expectRows("MATCH (n:Missing) RETURN coalesce(100 / 0 + n.age, 1)", {});
    expectRows("MATCH (n:Person) WHERE n.age > 100 RETURN coalesce(100 / 0 + n.age, 1)", {});
    expectRows("MATCH (n:Person) WHERE n.age > 100 RETURN CASE WHEN 100 / 0 > n.age THEN n.age END", {});
    expectRows("RETURN [i IN [] | coalesce(100 / 0 + i, 1)]", {{"[]"}});
}

TEST_F(SelectionRegionReadsCypherTest, raisesInTheFirstArgumentOfACoalesceNothingReads) {
    createItems();

    expectError("MATCH (n:Item) WITH n, coalesce(100 / (n.i % 3), 0) AS unused RETURN count(*)", "divide by zero");
    expectRows("MATCH (n:Item) WITH n, coalesce(n.v, 100 / (n.i % 3)) AS unused RETURN count(*)",
               {{std::to_string(ITEM_COUNT)}});
}

// Remy and Adam are the people with a four-letter name; nobody's is a hundred letters long
TEST_F(SelectionRegionReadsCypherTest, raisesInAFirstArgumentWhoseValueIsNull) {
    expectError("MATCH (n:Person) RETURN coalesce(null + (1 / (size(n.name) - 4)), 2)", "divide by zero");
    expectRows("MATCH (n:Person) RETURN coalesce(null + (1 / (size(n.name) - 100)), 2)",
               {{"2"}, {"2"}, {"2"}, {"2"}, {"2"}, {"2"}, {"2"}, {"2"}});
}

TEST_F(SelectionRegionReadsCypherTest, raisesInTheConditionOfACaseNothingReads) {
    createItems();

    expectError("MATCH (n:Item) WITH n, CASE WHEN 100 / (n.i % 3) > 1 THEN 1 END AS unused RETURN count(*)",
                "divide by zero");
}

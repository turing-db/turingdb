#include <gtest/gtest.h>

#include <stddef.h>
#include <stdint.h>

#include <algorithm>
#include <string>
#include <string_view>
#include <vector>

#include "IRTestRows.h"
#include "QueryStatus.h"
#include "WriteQueryTest.h"

using namespace db;
using namespace turing::test;

// A read in a body shares the read of the rows the op handed it, and a read past the op
// shares the read of the rows the body handed back. The rows have to come out as they did
// when each read fetched the property again, across chunk boundaries, and a row an
// optional op pads keeps the values of the row it came from.
class SubqueryPropertyReadsCypherTest : public WriteQueryTest {
protected:
    // More items than one chunk holds. Item i has v = i % 5, but none where i % 11 = 0, and
    // two targets: 2i with v = i % 3, then 2i + 1 with v = i % 7.
    static constexpr int64_t ITEM_COUNT = 3 * 65536 + 7;

    void createItems() {
        applyWrite("UNWIND range(0, " + std::to_string(ITEM_COUNT - 1) + ") AS i "
                   "CREATE (n:Item {i: i, v: CASE WHEN i % 11 <> 0 THEN i % 5 END}) "
                   "CREATE (n)-[:R]->(:Target {i: 2 * i, v: i % 3}) "
                   "CREATE (n)-[:R]->(:Target {i: 2 * i + 1, v: i % 7})");
    }

    static bool hasValue(int64_t item) {
        return item % 11 != 0;
    }

    static std::string valueOf(int64_t item) {
        return hasValue(item) ? std::to_string(item % 5) : "null";
    }

    // The targets of item whose v is the item's, in the order its edges were created
    static void collectMatches(int64_t item, std::vector<int64_t>& targets) {
        targets.clear();

        if (!hasValue(item)) {
            return;
        }

        if (item % 3 == item % 5) {
            targets.push_back(2 * item);
        }

        if (item % 7 == item % 5) {
            targets.push_back(2 * item + 1);
        }
    }

    static std::string renderList(const std::vector<int64_t>& elements) {
        std::string list = "[";
        for (size_t index = 0; index < elements.size(); index++) {
            list += index == 0 ? "" : ", ";
            list += std::to_string(elements[index]);
        }

        return list + "]";
    }

    // The rows of `RETURN n.i, n.v, m.i` past a body matching the targets of n, for the
    // items @param keepsItem keeps; a padded row comes once with a null target
    template <typename KeepsItem>
    static void buildMatchRows(KeepsItem keepsItem, bool padded, Rows& rows) {
        std::vector<int64_t> targets;
        for (int64_t item = 0; item < ITEM_COUNT; item++) {
            if (!keepsItem(item)) {
                continue;
            }

            collectMatches(item, targets);
            for (const int64_t target : targets) {
                rows.push_back({std::to_string(item), valueOf(item), std::to_string(target)});
            }

            if (padded && targets.empty()) {
                rows.push_back({std::to_string(item), valueOf(item), "null"});
            }
        }
    }

    static bool keepsEveryItem(int64_t item) {
        return true;
    }

    static bool keepsAValueAboveOne(int64_t item) {
        return hasValue(item) && item % 5 > 1;
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
};

TEST_F(SubqueryPropertyReadsCypherTest, countsTheTargetsMatchingAnOuterValue) {
    createItems();

    Rows counts;
    Rows exists;
    std::vector<int64_t> targets;
    for (int64_t item = 0; item < ITEM_COUNT; item++) {
        collectMatches(item, targets);
        counts.push_back({std::to_string(item), valueOf(item), std::to_string(targets.size())});
        exists.push_back({std::to_string(item), valueOf(item), targets.empty() ? "false" : "true"});
    }

    expectManyRows("MATCH (n:Item) RETURN n.i, n.v, COUNT { MATCH (n)-->(m) WHERE m.v = n.v }", counts);
    expectManyRows("MATCH (n:Item) RETURN n.i, n.v, COUNT { MATCH (n)-->(m) WHERE m.v = n.v RETURN DISTINCT m }",
                   counts);
    expectManyRows("MATCH (n:Item) RETURN n.i, n.v, EXISTS { MATCH (n)-->(m) WHERE m.v = n.v }", exists);
}

// A list collected in no order is summed: the targets of an item are 2i and 2i + 1, so the
// sum still tells which of them matched
TEST_F(SubqueryPropertyReadsCypherTest, collectsTheTargetsMatchingAnOuterValue) {
    createItems();

    Rows lists;
    Rows sums;
    std::vector<int64_t> targets;
    for (int64_t item = 0; item < ITEM_COUNT; item++) {
        collectMatches(item, targets);
        lists.push_back({std::to_string(item), valueOf(item), renderList(targets)});

        int64_t sum = 0;
        for (const int64_t target : targets) {
            sum += target;
        }

        sums.push_back({std::to_string(item), valueOf(item), std::to_string(sum)});
    }

    expectManyRows("MATCH (n:Item) RETURN n.i, n.v, COLLECT { MATCH (n)-->(m) WHERE m.v = n.v RETURN m.i ORDER BY m.i }",
                   lists);
    expectManyRows("MATCH (n:Item) RETURN n.i, n.v, "
                   "reduce(total = 0, target IN COLLECT { MATCH (n)-->(m) WHERE m.v = n.v RETURN m.i } | total + target)",
                   sums);
    expectManyRows("MATCH (n:Item) RETURN n.i, n.v, "
                   "reduce(total = 0, target IN [(n)-->(m) WHERE m.v = n.v | m.i] | total + target)",
                   sums);
}

TEST_F(SubqueryPropertyReadsCypherTest, callsABodyMatchingAnOuterValue) {
    createItems();

    Rows everyItem;
    buildMatchRows(keepsEveryItem, false, everyItem);
    expectManyRows("MATCH (n:Item) CALL (n) { MATCH (n)-->(m) WHERE m.v = n.v RETURN m } RETURN n.i, n.v, m.i",
                   everyItem);

    Rows aboveOne;
    buildMatchRows(keepsAValueAboveOne, false, aboveOne);
    expectManyRows("MATCH (n:Item) WHERE n.v > 1 CALL (n) { MATCH (n)-->(m) WHERE m.v = n.v RETURN m } "
                   "RETURN n.i, n.v, m.i",
                   aboveOne);

    Rows firstMatches;
    std::vector<int64_t> targets;
    for (int64_t item = 0; item < ITEM_COUNT; item++) {
        collectMatches(item, targets);
        if (keepsAValueAboveOne(item) && !targets.empty()) {
            firstMatches.push_back({std::to_string(item), valueOf(item), std::to_string(targets.front())});
        }
    }

    expectManyRows("MATCH (n:Item) WHERE n.v > 1 "
                   "CALL (n) { MATCH (n)-->(m) WHERE m.v = n.v RETURN m ORDER BY m.i LIMIT 1 } "
                   "RETURN n.i, n.v, m.i",
                   firstMatches);
}

// The body runs once per item, so the v read past the CALL is the one its body read; an
// optional CALL keeps an item its body misses, with its v and no target
TEST_F(SubqueryPropertyReadsCypherTest, returnsAnOuterValueTheBodyRunPerRowRead) {
    createItems();

    Rows firstMatches;
    Rows paddedFirstMatches;
    std::vector<int64_t> targets;
    for (int64_t item = 0; item < ITEM_COUNT; item++) {
        collectMatches(item, targets);

        const std::string target = targets.empty() ? "null" : std::to_string(targets.front());
        if (!targets.empty()) {
            firstMatches.push_back({std::to_string(item), valueOf(item), target});
        }

        paddedFirstMatches.push_back({std::to_string(item), valueOf(item), target});
    }

    expectManyRows("MATCH (n:Item) CALL (n) { MATCH (n)-->(m) WHERE m.v = n.v RETURN m ORDER BY m.i LIMIT 1 } "
                   "RETURN n.i, n.v, m.i",
                   firstMatches);
    expectManyRows("MATCH (n:Item) OPTIONAL CALL (n) { MATCH (n)-->(m) WHERE m.v = n.v RETURN m ORDER BY m.i LIMIT 1 } "
                   "RETURN n.i, n.v, m.i",
                   paddedFirstMatches);

    Rows values;
    for (int64_t item = 0; item < ITEM_COUNT; item++) {
        values.push_back({std::to_string(item), valueOf(item), valueOf(item)});
    }

    expectManyRows("MATCH (n:Item) CALL (n) { WITH n LIMIT 1 RETURN n.v AS v } RETURN n.i, n.v, v", values);
}

// The target of item i with the largest v above 0, the first one on a tie
TEST_F(SubqueryPropertyReadsCypherTest, returnsATargetValueTheBodyRead) {
    createItems();

    Rows everyTarget;
    Rows largest;
    for (int64_t item = 0; item < ITEM_COUNT; item++) {
        const int64_t firstValue = item % 3;
        const int64_t secondValue = item % 7;

        if (firstValue > 0) {
            everyTarget.push_back({std::to_string(item), std::to_string(2 * item), std::to_string(firstValue)});
        }

        if (secondValue > 0) {
            everyTarget.push_back({std::to_string(item), std::to_string(2 * item + 1), std::to_string(secondValue)});
        }

        if (firstValue == 0 && secondValue == 0) {
            continue;
        }

        const bool firstIsLargest = firstValue >= secondValue;
        const int64_t target = firstIsLargest ? 2 * item : 2 * item + 1;
        const int64_t value = firstIsLargest ? firstValue : secondValue;
        largest.push_back({std::to_string(item), std::to_string(target), std::to_string(value)});
    }

    expectManyRows("MATCH (n:Item) CALL (n) { MATCH (n)-->(m) WHERE m.v > 0 RETURN m } RETURN n.i, m.i, m.v",
                   everyTarget);
    expectManyRows("MATCH (n:Item) CALL (n) { MATCH (n)-->(m) WHERE m.v > 0 RETURN m ORDER BY m.v DESC, m.i LIMIT 1 } "
                   "RETURN n.i, m.i, m.v",
                   largest);
}

TEST_F(SubqueryPropertyReadsCypherTest, padsTheItemsAnOptionalCallMisses) {
    createItems();

    Rows aboveOne;
    buildMatchRows(keepsAValueAboveOne, true, aboveOne);
    expectManyRows("MATCH (n:Item) WHERE n.v > 1 "
                   "OPTIONAL CALL (n) { MATCH (n)-->(m) WHERE m.v = n.v RETURN m } RETURN n.i, n.v, m.i",
                   aboveOne);

    Rows everyItem;
    buildMatchRows(keepsEveryItem, true, everyItem);
    expectManyRows("MATCH (n:Item) OPTIONAL CALL (n) { MATCH (n)-->(m) WHERE m.v = n.v RETURN m } RETURN n.i, n.v, m.i",
                   everyItem);
}

TEST_F(SubqueryPropertyReadsCypherTest, padsTheItemsAnOptionalMatchMisses) {
    createItems();

    Rows aboveOne;
    buildMatchRows(keepsAValueAboveOne, true, aboveOne);
    expectManyRows("MATCH (n:Item) WHERE n.v > 1 OPTIONAL MATCH (n)-->(m) WHERE m.v = n.v RETURN n.i, n.v, m.i",
                   aboveOne);

    Rows everyItem;
    buildMatchRows(keepsEveryItem, true, everyItem);
    expectManyRows("MATCH (n:Item) OPTIONAL MATCH (n)-->(m) WHERE m.v = n.v RETURN n.i, n.v, m.i", everyItem);
}

TEST_F(SubqueryPropertyReadsCypherTest, matchesAnOuterValueInNestedBodies) {
    createItems();

    Rows everyItem;
    buildMatchRows(keepsEveryItem, false, everyItem);
    expectManyRows("MATCH (n:Item) CALL (n) { CALL (n) { MATCH (n)-->(m) WHERE m.v = n.v RETURN m } RETURN m } "
                   "RETURN n.i, n.v, m.i",
                   everyItem);

    Rows aboveOne;
    buildMatchRows(keepsAValueAboveOne, false, aboveOne);
    expectManyRows("MATCH (n:Item) WHERE n.v > 1 "
                   "CALL (n) { CALL (n) { MATCH (n)-->(m) WHERE m.v = n.v RETURN m } RETURN m } RETURN n.i, n.v, m.i",
                   aboveOne);

    Rows paddedAboveOne;
    buildMatchRows(keepsAValueAboveOne, true, paddedAboveOne);
    expectManyRows("MATCH (n:Item) WHERE n.v > 1 "
                   "CALL (n) { OPTIONAL MATCH (n)-->(m) WHERE m.v = n.v RETURN m } RETURN n.i, n.v, m.i",
                   paddedAboveOne);
}

// Targets 0 and 1 have i < 2, and the items with v = 4 are the ones above 3
TEST_F(SubqueryPropertyReadsCypherTest, returnsAValueAFactorRead) {
    createItems();

    Rows leftFactor;
    Rows rightFactor;
    for (int64_t item = 0; item < ITEM_COUNT; item++) {
        if (!hasValue(item) || item % 5 != 4) {
            continue;
        }

        for (const int64_t target : {0, 1}) {
            leftFactor.push_back({std::to_string(item), "4", std::to_string(target)});
            rightFactor.push_back({std::to_string(target), std::to_string(item), "4"});
        }
    }

    expectManyRows("MATCH (a:Item), (b:Target) WHERE a.v > 3 AND b.i < 2 RETURN a.i, a.v, b.i", leftFactor);
    expectManyRows("MATCH (a:Target), (b:Item) WHERE a.i < 2 AND b.v > 3 RETURN a.i, b.i, b.v", rightFactor);
}

// Each edge counts the edges out of its target, or none where it lasts 15: Adam has 3, Remy
// 4, Ghosts 1, Eighties and the interests of Luc none
TEST_F(SubqueryPropertyReadsCypherTest, readsAnOuterEdgeValueInABody) {
    expectRows("MATCH (a:Person)-[e]->(b) WHERE e.duration > 10 "
               "RETURN e.name, COUNT { MATCH (b)-->(c) WHERE e.duration > 15 }",
               {{"Remy -> Adam", "3"},
                {"Adam -> Remy", "4"},
                {"Remy -> Ghosts", "1"},
                {"Remy -> Eighties", "0"},
                {"Luc -> Animals", "0"},
                {"Luc -> Computers", "0"}});
}

// Remy and Adam are 32 and nobody else has an age. Remy's first interest by name is
// Computers, which he shares with Luc, and Adam's is Bio, which he shares with Maxime.
TEST_F(SubqueryPropertyReadsCypherTest, returnsAnOuterValueTheBodyRunPerRowReadPastABarrier) {
    expectRows("MATCH (n:Person) "
               "CALL (n) { MATCH (n)-[:INTERESTED_IN]->(i) WITH n, i ORDER BY i.name LIMIT 1 "
               "MATCH (i)<-[:INTERESTED_IN]-(q:Person) WHERE q.age = n.age RETURN q } "
               "RETURN n.name, n.age, q.name",
               {{"Remy", "32", "Remy"}, {"Adam", "32", "Adam"}});
    expectRows("MATCH (n:Person) "
               "CALL (n) { MATCH (m:Person) WHERE m.age = n.age RETURN m ORDER BY m.name LIMIT 1 } "
               "RETURN n.name, n.age, m.name",
               {{"Remy", "32", "Adam"}, {"Adam", "32", "Adam"}});
}

// Remy reaches Adam and Ghosts, which both lead back to him, and Adam reaches Remy; the
// others have no age
TEST_F(SubqueryPropertyReadsCypherTest, readsAnOuterValueInANestedBody) {
    expectRows("MATCH (n:Person) RETURN n.name, "
               "COUNT { MATCH (n)-->(m) WHERE EXISTS { MATCH (m)-->(k) WHERE k.age = n.age } }",
               {{"Remy", "2"},
                {"Adam", "1"},
                {"Maxime", "0"},
                {"Luc", "0"},
                {"Martina", "0"},
                {"Suhas", "0"},
                {"Cyrus", "0"},
                {"Doruk", "0"}});
}

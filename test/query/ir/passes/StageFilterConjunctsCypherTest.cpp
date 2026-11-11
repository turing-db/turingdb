#include <gtest/gtest.h>

#include <stdint.h>
#include <stdlib.h>

#include <algorithm>
#include <string>
#include <string_view>

#include "IRTestRows.h"
#include "QueryStatus.h"
#include "WriteQueryTest.h"

using namespace db;
using namespace turing::test;

// A filter split into stages keeps the rows the one filter it replaces kept, across chunks
class StageFilterConjunctsCypherTest : public WriteQueryTest {
protected:
    // More items than one chunk holds. Item i has v = i % 5 and u = i % 4, an R edge to a
    // Target holding the same i, and every third item a second R edge to an Other.
    static constexpr int64_t ITEM_COUNT = 2 * 65536 + 5;

    // Enough papers that the Nature ones crossed with the Science ones fill more than a chunk
    static constexpr int64_t PAPER_COUNT = 600;

    void createItems() {
        applyWrite("UNWIND range(0, " + std::to_string(ITEM_COUNT - 1) + ") AS i "
                   "CREATE (n:Item {i: i, v: i % 5, u: i % 4})-[:R]->(:Target {i: i})");
        applyWrite("MATCH (n:Item) WHERE n.i % 3 = 0 CREATE (n)-[:R]->(:Other {i: n.i})");
    }

    // Paper i appears in Nature when i is even and in Science otherwise, in year 1990 + i % 30
    // and volume i % 17
    void createPapers() {
        applyWrite("UNWIND range(0, " + std::to_string(PAPER_COUNT - 1) + ") AS i "
                   "CREATE (:Paper {i: i, journal: CASE WHEN i % 2 = 0 THEN 'Nature' ELSE 'Science' END, "
                   "year: 1990 + i % 30, volume: i % 17})");
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

TEST_F(StageFilterConjunctsCypherTest, testsTheFarLabelOfTheTypedEdges) {
    createItems();

    Rows targets;
    Rows others;
    for (int64_t item = 0; item < ITEM_COUNT; item++) {
        targets.push_back({std::to_string(item), std::to_string(item)});

        if (item % 3 == 0) {
            others.push_back({std::to_string(item)});
        }
    }

    expectManyRows("MATCH (n:Item)-[:R]-(t:Target) RETURN n.i, t.i", targets);
    expectManyRows("MATCH (n:Item)-[:R]-(o:Other) RETURN o.i", others);
}

// t.i is in [n.u, n.v, 7] for item 4 alone among those whose i % 5 is 4: 4 % 5 is 4
TEST_F(StageFilterConjunctsCypherTest, testsMembershipOverTheRowsTheCheapTestKeeps) {
    createItems();

    Rows kept;
    for (int64_t item = 0; item < ITEM_COUNT; item++) {
        const bool inTheList = item == item % 4 || item == item % 5 || item == 7;
        if (item % 5 == 4 && !inTheList) {
            kept.push_back({std::to_string(item)});
        }
    }

    expectManyRows("MATCH (n:Item)-[:R]->(t:Target) WHERE t.i % 5 = 4 AND NOT t.i IN [n.u, n.v, 7] RETURN t.i", kept);
    expectManyRows("MATCH (n:Item)-[:R]->(t:Target) WHERE NOT t.i IN [n.u, n.v, 7] AND t.i % 5 = 4 RETURN t.i", kept);
}

TEST_F(StageFilterConjunctsCypherTest, comparesThePairsTheFirstConjunctKeeps) {
    createPapers();

    Rows pairs;
    for (int64_t nature = 0; nature < PAPER_COUNT; nature += 2) {
        for (int64_t science = 1; science < PAPER_COUNT; science += 2) {
            const int64_t yearGap = llabs(nature % 30 - science % 30);
            if (yearGap <= 1 && nature % 17 > science % 17) {
                pairs.push_back({std::to_string(nature), std::to_string(science)});
            }
        }
    }

    expectManyRows("MATCH (a:Paper {journal: 'Nature'}), (b:Paper {journal: 'Science'}) "
                   "WHERE abs(a.year - b.year) <= 1 AND a.volume > b.volume RETURN a.i, b.i",
                   pairs);
    expectManyRows("MATCH (a:Paper {journal: 'Nature'}), (b:Paper {journal: 'Science'}) "
                   "WHERE a.volume > b.volume AND abs(a.year - b.year) <= 1 RETURN a.i, b.i",
                   pairs);
}

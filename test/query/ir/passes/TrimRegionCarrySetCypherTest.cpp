#include <gtest/gtest.h>

#include <stddef.h>
#include <stdint.h>

#include <algorithm>
#include <string>
#include <string_view>

#include "IRTestRows.h"
#include "QueryStatus.h"
#include "WriteQueryTest.h"

using namespace db;
using namespace turing::test;

// A walk keeps the rows it had once the hop imports no step reads are gone, and every hop
// still compares against the value of its own seed row, across chunk boundaries.
class TrimRegionCarrySetCypherTest : public WriteQueryTest {
protected:
    // More items than one chunk holds. Item i has v = i % 5, but none where i % 11 = 0, and
    // u = i % 4. It reaches target 2i over an edge with w = i % 3, of type R for an even i
    // and S for an odd one, and target 2i reaches 2i + 1 over an R edge with w = i % 7.
    static constexpr int64_t ITEM_COUNT = 3 * 65536 + 7;

    void createItems() {
        const std::string last = std::to_string(ITEM_COUNT - 1);

        applyWrite("UNWIND range(0, " + last + ", 2) AS i "
                   "CREATE (n:Item {i: i, v: CASE WHEN i % 11 <> 0 THEN i % 5 END, u: i % 4}) "
                   "CREATE (n)-[:R {w: i % 3}]->(t:Target {i: 2 * i}) "
                   "CREATE (t)-[:R {w: i % 7}]->(:Target {i: 2 * i + 1})");
        applyWrite("UNWIND range(1, " + last + ", 2) AS i "
                   "CREATE (n:Item {i: i, v: CASE WHEN i % 11 <> 0 THEN i % 5 END, u: i % 4}) "
                   "CREATE (n)-[:S {w: i % 3}]->(t:Target {i: 2 * i}) "
                   "CREATE (t)-[:R {w: i % 7}]->(:Target {i: 2 * i + 1})");
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

// Remy (0) and Adam (1) are aged 32. The edges lasting less than 32 are Remy's to Adam,
// Ghosts (6) and Eighties (3), and Adam's to Remy
TEST_F(TrimRegionCarrySetCypherTest, comparesEachHopToTheCarriedSeedAge) {
    expectRows("MATCH (n:Person) WHERE n.age > 0 "
               "MATCH (n)(()-[e]->() WHERE e.duration < n.age){1,2}(m) RETURN m",
               {{"1"}, {"6"}, {"3"}, {"0"}, {"0"}, {"1"}, {"6"}, {"3"}});
}

// Remy's and Adam's edges to each other are the KNOWS_WELL edges leaving a person. Remy's
// edges to Adam, Ghosts and Eighties and Adam's to Remy last 20
TEST_F(TrimRegionCarrySetCypherTest, testsEachHopOnTheCarriedEdgeType) {
    expectRows("MATCH (a:Person)-[r]->(n) WHERE r:KNOWS_WELL OR r.duration > 15 "
               "MATCH (n)(()-[e]->() WHERE r:KNOWS_WELL AND e.duration > 15){1}(m) RETURN a, n, m",
               {{"0", "1", "0"}, {"1", "0", "1"}, {"1", "0", "6"}, {"1", "0", "3"}});
}

// Ghosts' edge to Remy lasts 200 and Remy's to Computers has no duration
TEST_F(TrimRegionCarrySetCypherTest, comparesTheFirstStepOfTwoToTheSeedAge) {
    expectRows("MATCH (n:Person) WHERE n.age > 0 "
               "MATCH (n)((a)-[e]->(b)-[f]->(c) WHERE e.duration < n.age AND f.duration > 15){1,2}(m) "
               "RETURN n.name, m.name",
               {{"Remy", "Remy"},
                {"Remy", "Remy"},
                {"Remy", "Remy"},
                {"Remy", "Remy"},
                {"Adam", "Adam"},
                {"Adam", "Ghosts"},
                {"Adam", "Eighties"}});
}

TEST_F(TrimRegionCarrySetCypherTest, comparesTheLastStepOfTwoToTheSeedAge) {
    expectRows("MATCH (n:Person) WHERE n.age > 0 "
               "MATCH (n)((a)-[e]->(b)-[f]->(c) WHERE f.duration < n.age){1,2}(m) RETURN n.name, m.name",
               {{"Remy", "Remy"},
                {"Adam", "Adam"},
                {"Adam", "Ghosts"},
                {"Adam", "Eighties"},
                {"Adam", "Adam"},
                {"Adam", "Eighties"}});
}

// The edges into a person lasting more than 15 are Remy's to Adam and Adam's to Remy, which
// last 20, and Ghosts' to Remy, which lasts 200
TEST_F(TrimRegionCarrySetCypherTest, comparesEachStepToItsOwnImport) {
    expectRows("MATCH (a)-[r]->(n:Person) WHERE r.duration > 15 AND n.age > 0 "
               "MATCH (n)((x)-[e]->(y)-[f]->(z) WHERE e.duration < n.age AND f.duration <= r.duration){1}(m) "
               "RETURN a.name, m.name",
               {{"Remy", "Adam"},
                {"Remy", "Ghosts"},
                {"Remy", "Eighties"},
                {"Adam", "Remy"},
                {"Ghosts", "Remy"},
                {"Ghosts", "Remy"}});
}

TEST_F(TrimRegionCarrySetCypherTest, comparesEachHopToEveryImportTheRegionReads) {
    expectRows("MATCH (a)-[r]->(n:Person) "
               "MATCH (n)(()-[e]->() WHERE e.duration < r.duration AND e.duration < n.age){1,2}(m) RETURN a.name, m.name",
               {{"Ghosts", "Adam"}, {"Ghosts", "Ghosts"}, {"Ghosts", "Eighties"}, {"Ghosts", "Remy"}});
}

TEST_F(TrimRegionCarrySetCypherTest, testsEachHopOnAFusedListPredicate) {
    const Rows matches {{"Remy", "Adam"},
                        {"Remy", "Ghosts"},
                        {"Remy", "Eighties"},
                        {"Remy", "Remy"},
                        {"Adam", "Remy"},
                        {"Adam", "Adam"},
                        {"Adam", "Ghosts"},
                        {"Adam", "Eighties"}};

    expectRows("MATCH (n:Person) WHERE n.age > 0 "
               "MATCH p = (n)-[*1..2]->(m) WHERE all(x IN relationships(p) WHERE x.duration < n.age) RETURN n.name, m.name",
               matches);
    expectRows("MATCH (n:Person) "
               "MATCH p = (n)-[*1..2]->(m) WHERE all(x IN relationships(p) WHERE x.duration < n.age) RETURN n.name, m.name",
               matches);
}

TEST_F(TrimRegionCarrySetCypherTest, comparesEachHopToItsSeedAcrossChunks) {
    createItems();

    Rows hopMatches;
    Rows stepMatches;
    Rows typeMatches;
    for (int64_t item = 0; item < ITEM_COUNT; item++) {
        const std::string itemText = std::to_string(item);
        const std::string first = std::to_string(2 * item);
        const std::string second = std::to_string(2 * item + 1);

        if (item % 2 == 0 && item % 7 < 5) {
            typeMatches.push_back({itemText, second});
        }

        const int64_t value = item % 5;
        const int64_t other = item % 4;
        if (item % 11 == 0 || value == 0) {
            continue;
        }

        if (item % 3 < value) {
            hopMatches.push_back({itemText, first});

            if (item % 7 < value) {
                hopMatches.push_back({itemText, second});
            }
        }

        if (other > 0 && item % 3 < value && item % 7 < other) {
            stepMatches.push_back({itemText, second});
        }
    }

    expectManyRows("MATCH (n:Item) WHERE n.v > 0 "
                   "MATCH (n)(()-[e]->() WHERE e.w < n.v){1,2}(m) RETURN n.i, m.i",
                   hopMatches);
    expectManyRows("MATCH (n:Item) WHERE n.v > 0 AND n.u > 0 "
                   "MATCH (n)(()-[e]->()-[f]->() WHERE e.w < n.v AND f.w < n.u){1}(m) RETURN n.i, m.i",
                   stepMatches);
    expectManyRows("MATCH (n:Item)-[r]->(t:Target) WHERE r:R OR r.w > 0 "
                   "MATCH (t)(()-[e]->() WHERE r:R AND e.w < 5){1}(m) RETURN n.i, m.i",
                   typeMatches);
}

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

// A hop predicate reading a property of an imported node or edge reads the column read
// before the walk, imported into the hop region. Each hop has to see the value of its own
// seed row, across chunk boundaries and in every step of a repeated body.
class HopImportPropertyReadsCypherTest : public WriteQueryTest {
protected:
    // More items than one chunk holds. Item i has v = i % 5, but none where i % 11 = 0, and
    // a chain of two targets: 2i reached over an edge with w = i % 3, then 2i + 1 over an
    // edge with w = i % 7. Target 2i has v = i % 3 and target 2i + 1 has v = i % 7.
    static constexpr int64_t ITEM_COUNT = 3 * 65536 + 7;

    void createItems() {
        applyWrite("UNWIND range(0, " + std::to_string(ITEM_COUNT - 1) + ") AS i "
                   "CREATE (n:Item {i: i, v: CASE WHEN i % 11 <> 0 THEN i % 5 END}) "
                   "CREATE (n)-[:R {w: i % 3}]->(t:Target {i: 2 * i, v: i % 3}) "
                   "CREATE (t)-[:R {w: i % 7}]->(:Target {i: 2 * i + 1, v: i % 7})");
    }

    static bool hasValue(int64_t item) {
        return item % 11 != 0;
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

TEST_F(HopImportPropertyReadsCypherTest, comparesEachHopToItsSeedValue) {
    createItems();

    Rows edgeMatches;
    Rows nodeMatches;
    Rows bodyMatches;
    Rows edgeCounts;
    for (int64_t item = 0; item < ITEM_COUNT; item++) {
        const int64_t value = item % 5;
        if (!hasValue(item) || value == 0) {
            continue;
        }

        const std::string itemText = std::to_string(item);
        const std::string valueText = std::to_string(value);
        const std::string first = std::to_string(2 * item);
        const std::string second = std::to_string(2 * item + 1);

        const bool firstMatches = item % 3 < value;
        const bool secondMatches = firstMatches && item % 7 < value;
        edgeCounts.push_back({itemText, std::to_string(int64_t {firstMatches} + int64_t {secondMatches})});

        if (firstMatches) {
            edgeMatches.push_back({itemText, valueText, first});
        }

        if (secondMatches) {
            edgeMatches.push_back({itemText, valueText, second});
            bodyMatches.push_back({itemText, valueText, second});
        }

        if (item % 3 <= value) {
            nodeMatches.push_back({itemText, first});

            if (item % 7 <= value) {
                nodeMatches.push_back({itemText, second});
            }
        }
    }

    expectManyRows("MATCH (n:Item) WHERE n.v > 0 "
                   "MATCH (n)(()-[e]->() WHERE e.w < n.v){1,2}(m) RETURN n.i, n.v, m.i",
                   edgeMatches);
    expectManyRows("MATCH (n:Item) WHERE n.v > 0 "
                   "MATCH (n)(()-[]->(b) WHERE b.v <= n.v){1,2}(m) RETURN n.i, m.i",
                   nodeMatches);
    expectManyRows("MATCH (n:Item) WHERE n.v > 0 "
                   "MATCH (n)(()-[e]->()-[f]->() WHERE e.w < n.v AND f.w < n.v){1}(m) RETURN n.i, n.v, m.i",
                   bodyMatches);
    expectManyRows("MATCH (n:Item) WHERE n.v > 0 "
                   "RETURN n.i, COUNT { MATCH (n)(()-[e]->() WHERE e.w < n.v){1,2}(m) }",
                   edgeCounts);
}

TEST_F(HopImportPropertyReadsCypherTest, comparesEachHopToItsImportedEdgeValue) {
    createItems();

    Rows matches;
    for (int64_t item = 0; item < ITEM_COUNT; item++) {
        const int64_t weight = item % 3;
        if (weight > 0 && item % 7 < weight) {
            matches.push_back({std::to_string(2 * item), std::to_string(weight), std::to_string(2 * item + 1)});
        }
    }

    expectManyRows("MATCH (:Item)-[r]->(t:Target) WHERE r.w > 0 "
                   "MATCH (t)(()-[e]->() WHERE e.w < r.w){1,2}(m) RETURN t.i, r.w, m.i",
                   matches);
}

// The edges lasting less than 32 are Remy's to Adam, Ghosts and Eighties, Adam's to Remy,
// Luc's and Martina's; Ghosts' edge to Remy lasts 200
TEST_F(HopImportPropertyReadsCypherTest, comparesEachHopToTheSeedAge) {
    expectRows("MATCH (n:Person) WHERE n.age > 0 "
               "MATCH (n)(()-[e]->() WHERE e.duration < n.age){1,2}(m) RETURN n.name, m.name",
               {{"Remy", "Adam"},
                {"Remy", "Ghosts"},
                {"Remy", "Eighties"},
                {"Remy", "Remy"},
                {"Adam", "Remy"},
                {"Adam", "Adam"},
                {"Adam", "Ghosts"},
                {"Adam", "Eighties"}});
}

// Only Ghosts' edge into Remy lasts longer than the 20 every edge leaving Adam or Remy lasts
TEST_F(HopImportPropertyReadsCypherTest, comparesEachHopToTheImportedEdgeDuration) {
    expectRows("MATCH (a)-[r]->(n:Person) WHERE r.duration > 15 "
               "MATCH (n)(()-[e]->() WHERE e.duration < r.duration){1,2}(m) RETURN a.name, r.duration, m.name",
               {{"Ghosts", "200", "Adam"},
                {"Ghosts", "200", "Ghosts"},
                {"Ghosts", "200", "Eighties"},
                {"Ghosts", "200", "Remy"}});
}

// Remy and Adam are the only nodes aged 32
TEST_F(HopImportPropertyReadsCypherTest, comparesEachHopNodeToTheSeedAge) {
    expectRows("MATCH (n:Person) WHERE n.age > 0 "
               "MATCH (n)(()-[e]->(b) WHERE b.age = n.age){1,2}(m) RETURN n.name, n.age, m.name",
               {{"Remy", "32", "Adam"}, {"Remy", "32", "Remy"}, {"Adam", "32", "Remy"}, {"Adam", "32", "Adam"}});
}

TEST_F(HopImportPropertyReadsCypherTest, comparesTheLastStepOfABodyToTheSeedAge) {
    expectRows("MATCH (n:Person) WHERE n.age > 0 "
               "MATCH (n)((a)-[e]->(b)-[f]->(c) WHERE f.duration < n.age){1,2}(m) RETURN n.name, m.name",
               {{"Remy", "Remy"},
                {"Adam", "Adam"},
                {"Adam", "Ghosts"},
                {"Adam", "Eighties"},
                {"Adam", "Adam"},
                {"Adam", "Eighties"}});
}

TEST_F(HopImportPropertyReadsCypherTest, comparesTheFirstStepOfABodyToTheSeedAge) {
    expectRows("MATCH (n:Person) WHERE n.age > 0 "
               "MATCH (n)((a)-[e]->(b)-[f]->(c) WHERE e.duration < n.age){1,2}(m) RETURN n.name, m.name",
               {{"Remy", "Remy"},
                {"Remy", "Remy"},
                {"Remy", "Remy"},
                {"Remy", "Remy"},
                {"Remy", "Bio"},
                {"Remy", "Bio"},
                {"Remy", "Cooking"},
                {"Remy", "Cooking"},
                {"Adam", "Adam"},
                {"Adam", "Ghosts"},
                {"Adam", "Computers"},
                {"Adam", "Eighties"}});
}

TEST_F(HopImportPropertyReadsCypherTest, comparesEveryStepOfABodyToTheSeedAge) {
    expectRows("MATCH (n:Person) WHERE n.age > 0 "
               "MATCH (n)((a)-[e]->(b)-[f]->(c) WHERE e.duration < n.age AND f.duration < n.age){1,2}(m) "
               "RETURN n.name, n.age, m.name",
               {{"Remy", "32", "Remy"}, {"Adam", "32", "Adam"}, {"Adam", "32", "Ghosts"}, {"Adam", "32", "Eighties"}});
}

// Nobody but Remy and Adam has an age, and no edge into a person but Ghosts' lasts more than 20
TEST_F(HopImportPropertyReadsCypherTest, comparesEachHopToAValueNothingReadBefore) {
    expectRows("MATCH (n:Person) MATCH (n)(()-[e]->() WHERE e.duration < n.age){1,2}(m) RETURN n.name, m.name",
               {{"Remy", "Adam"},
                {"Remy", "Ghosts"},
                {"Remy", "Eighties"},
                {"Remy", "Remy"},
                {"Adam", "Remy"},
                {"Adam", "Adam"},
                {"Adam", "Ghosts"},
                {"Adam", "Eighties"}});
    expectRows("MATCH (a)-[r]->(n:Person) MATCH (n)(()-[e]->() WHERE e.duration < r.duration){1,2}(m) RETURN a.name, m.name",
               {{"Ghosts", "Adam"}, {"Ghosts", "Ghosts"}, {"Ghosts", "Eighties"}, {"Ghosts", "Remy"}});
}

// Remy and Adam are the founders, and Remy alone a software engineer. The edges lasting more
// than 15 are Remy's to Adam, Ghosts and Eighties, Adam's and Ghosts' to Remy, and Luc's
TEST_F(HopImportPropertyReadsCypherTest, testsEachHopOnTheImportedNodeLabels) {
    expectRows("MATCH (n:Person) WHERE n:Founder OR n.age > 40 WITH n "
               "MATCH (n)(()-[e]->() WHERE n:SoftwareEngineering AND e.duration > 15){1,2}(m) RETURN n.name, m.name",
               {{"Remy", "Adam"}, {"Remy", "Ghosts"}, {"Remy", "Eighties"}, {"Remy", "Remy"}, {"Remy", "Remy"}});
}

// The edges between Remy and Adam are the only KNOWS_WELL edges leaving a person; Remy's
// edges to Ghosts and Eighties and Luc's to Animals last 20
TEST_F(HopImportPropertyReadsCypherTest, testsEachHopOnTheImportedEdgeType) {
    expectRows("MATCH (a:Person)-[r]->(n) WITH a, r, n WHERE r:KNOWS_WELL OR r.duration > 15 "
               "MATCH (n)(()-[e]->() WHERE r:KNOWS_WELL AND e.duration > 15){1}(m) RETURN a.name, n.name, m.name",
               {{"Remy", "Adam", "Remy"}, {"Adam", "Remy", "Adam"}, {"Adam", "Remy", "Ghosts"}, {"Adam", "Remy", "Eighties"}});
}

// Remy and Adam each reach 4 nodes over edges lasting less than their age
TEST_F(HopImportPropertyReadsCypherTest, countsTheHopsComparedToTheSeedAgeInACountBody) {
    expectRows("MATCH (n:Person) WHERE n.age > 0 "
               "RETURN n.name, COUNT { MATCH (n)(()-[e]->() WHERE e.duration < n.age){1,2}(m) }",
               {{"Remy", "4"}, {"Adam", "4"}});
}

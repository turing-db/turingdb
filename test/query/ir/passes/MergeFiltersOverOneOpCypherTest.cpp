#include <gtest/gtest.h>

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

// The conditions over the rows of one op keep the same rows whatever order the query writes
// them in, and a condition that divides still sees only the rows the ones before it kept
class MergeFiltersOverOneOpCypherTest : public WriteQueryTest {
protected:
    // More items than one chunk holds. Item i has v = i % 5 and u = i % 4, and an R edge to
    // a target holding the same i.
    static constexpr int64_t ITEM_COUNT = 2 * 65536 + 5;

    void createItems() {
        applyWrite("UNWIND range(0, " + std::to_string(ITEM_COUNT - 1) + ") AS i "
                   "CREATE (n:Item {i: i, v: i % 5, u: i % 4})-[:R]->(:Target {i: i})");
    }

    void expectEveryOrder(const std::vector<std::string_view>& queries, const Rows& expected) {
        for (const std::string_view query : queries) {
            expectRows(query, expected);
        }
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

// Remy reaches Adam, Ghosts, Computers and Eighties in one hop, and Remy, Bio and Cooking
// through Adam and Remy again through Ghosts in two
TEST_F(MergeFiltersOverOneOpCypherTest, walksFromTheScannedNode) {
    const Rows reached {{"Adam"}, {"Ghosts"}, {"Computers"}, {"Eighties"}, {"Remy"}, {"Bio"}, {"Cooking"}, {"Remy"}};

    expectEveryOrder({"MATCH (n:Person)-[*1..2]->(m) WHERE n.name = 'Remy' AND n.age > 3 RETURN m.name",
                      "MATCH (n:Person)-[*1..2]->(m) WHERE n.age > 3 AND n.name = 'Remy' RETURN m.name",
                      "MATCH (n:Person)-[*1..2]->(m) WHERE n.name = 'Remy' WITH n, m WHERE n.age > 3 RETURN m.name",
                      "MATCH (n:Person)-[*1..2]->(m) WHERE n.age > 3 WITH n, m WHERE n.name = 'Remy' RETURN m.name",
                      "MATCH (n:Person)-[*1..2]->(m) WHERE id(n) = 0 AND n.age > 3 RETURN m.name",
                      "MATCH (n:Person)-[*1..2]->(m) WHERE n.age > 3 AND id(n) = 0 RETURN m.name"},
                     reached);

    expectEveryOrder({"MATCH (n:Person {name: 'Remy'})-[]->(m)-[]->(k) WHERE n.age > 3 RETURN k.name",
                      "MATCH (n:Person)-[]->(m)-[]->(k) WHERE n.age > 3 AND n.name = 'Remy' RETURN k.name"},
                     {{"Remy"}, {"Bio"}, {"Cooking"}, {"Remy"}});
}

// Ghosts and Gym are the interests whose name starts with a G. Remy is interested in Ghosts,
// which knows Remy, and Gym has no edge of its own
TEST_F(MergeFiltersOverOneOpCypherTest, hopsByTypeAndLabel) {
    expectEveryOrder({"MATCH (a:Person)-[:INTERESTED_IN]->(e:Interest)-[]->(x) WHERE e.name STARTS WITH 'G' RETURN a.name, x.name",
                      "MATCH (a:Person)-[r]->(e)-[]->(x) WHERE r:INTERESTED_IN AND e:Interest AND e.name STARTS WITH 'G' RETURN a.name, x.name",
                      "MATCH (a:Person)-[r]->(e)-[]->(x) WHERE e.name STARTS WITH 'G' AND e:Interest AND r:INTERESTED_IN RETURN a.name, x.name"},
                     {{"Remy", "Remy"}});
}

// Remy's edges lasting more than 15 are the 20 of his to Adam, Ghosts and Eighties. Adam's
// to Remy lasts 20 as well, Ghosts' to Remy 200, and Luc's to Animals 20.
TEST_F(MergeFiltersOverOneOpCypherTest, keepsTheTypedAndLabelledEdges) {
    expectEveryOrder({"MATCH (a:Person {name: 'Remy'})-[e]->(b) WHERE e.duration > 15 AND e:KNOWS_WELL RETURN b.name",
                      "MATCH (a:Person {name: 'Remy'})-[e]->(b) WHERE e:KNOWS_WELL AND e.duration > 15 RETURN b.name"},
                     {{"Adam"}});

    expectEveryOrder({"MATCH (a:Person {name: 'Remy'})-[e]->(b) WHERE e.duration > 15 AND b:Interest RETURN b.name",
                      "MATCH (a:Person {name: 'Remy'})-[e]->(b) WHERE b:Interest AND e.duration > 15 RETURN b.name"},
                     {{"Ghosts"}, {"Eighties"}});

    expectEveryOrder({"MATCH ()-[e]->() WHERE e.duration > 15 AND e:KNOWS_WELL RETURN e.name",
                      "MATCH ()-[e]->() WHERE e:KNOWS_WELL AND e.duration > 15 RETURN e.name"},
                     {{"Remy -> Adam"}, {"Adam -> Remy"}, {"Ghosts -> Remy"}});

    expectEveryOrder({"MATCH (a)-[e]->(b) WHERE e.duration > 15 AND b:Interest RETURN a.name, b.name",
                      "MATCH (a)-[e]->(b) WHERE b:Interest AND e.duration > 15 RETURN a.name, b.name"},
                     {{"Remy", "Ghosts"}, {"Remy", "Eighties"}, {"Luc", "Animals"}});
}

// Remy and Adam are the two Founders and the only people with an age, 32. Each reaches the
// other in one hop and himself in two, Remy also through Ghosts.
TEST_F(MergeFiltersOverOneOpCypherTest, endsTheWalkOnTheFounders) {
    expectEveryOrder({"MATCH (a:Person)-[*1..2]->(b) WHERE b.age > 3 AND b:Founder RETURN a.name, b.name",
                      "MATCH (a:Person)-[*1..2]->(b) WHERE b:Founder AND b.age > 3 RETURN a.name, b.name"},
                     {{"Remy", "Adam"}, {"Remy", "Remy"}, {"Remy", "Remy"}, {"Adam", "Remy"}, {"Adam", "Adam"}});

    expectEveryOrder({"MATCH (a:Person)-[:KNOWS_WELL]->(b:Person)-[*1..2]->(c) WHERE c = a AND c.age >= b.age RETURN a.name, b.name",
                      "MATCH (a:Person)-[:KNOWS_WELL]->(b:Person)-[*1..2]->(c) WHERE c.age >= b.age AND c = a RETURN a.name, b.name"},
                     {{"Remy", "Adam"}, {"Adam", "Remy"}});

    expectEveryOrder({"MATCH (a:Person), (b:Person) WHERE a.age = b.age AND a.name <> b.name RETURN a.name, b.name",
                      "MATCH (a:Person), (b:Person) WHERE a.name <> b.name AND a.age = b.age RETURN a.name, b.name"},
                     {{"Remy", "Adam"}, {"Adam", "Remy"}});
}

TEST_F(MergeFiltersOverOneOpCypherTest, keepsTheSameRowsAcrossChunks) {
    createItems();

    Rows items;
    Rows targets;
    for (int64_t item = 0; item < ITEM_COUNT; item++) {
        if (item % 4 == 1 && item % 5 > 2) {
            items.push_back({std::to_string(item)});
            targets.push_back({std::to_string(item), std::to_string(item)});
        }
    }

    for (const std::string_view query : {"MATCH (n:Item) WHERE n.u = 1 AND n.v > 2 RETURN n.i",
                                         "MATCH (n:Item) WHERE n.v > 2 AND n.u = 1 RETURN n.i",
                                         "MATCH (n:Item) WHERE n.v > 2 WITH n WHERE n.u = 1 RETURN n.i"}) {
        expectManyRows(query, items);
    }

    for (const std::string_view query : {"MATCH (n:Item)-[r]->(t) WHERE n.u = 1 AND t:Target AND n.v > 2 AND r:R RETURN n.i, t.i",
                                         "MATCH (n:Item)-[r]->(t) WHERE r:R AND n.v > 2 AND t:Target AND n.u = 1 RETURN n.i, t.i"}) {
        expectManyRows(query, targets);
    }
}

// v is 0 on every fifth item, which a division by v raises on, unless a filter before it
// removed that item. 10 / v is more than 2 for v from 1 to 3.
TEST_F(MergeFiltersOverOneOpCypherTest, dividesOnlyTheRowsTheFiltersBeforeKept) {
    createItems();

    Rows divided;
    Rows dividedOnU;
    for (int64_t item = 0; item < ITEM_COUNT; item++) {
        const int64_t value = item % 5;
        if (value >= 1 && value <= 3) {
            divided.push_back({std::to_string(item)});

            if (item % 4 == 1) {
                dividedOnU.push_back({std::to_string(item)});
            }
        }
    }

    expectManyRows("MATCH (n:Item) WHERE n.v <> 0 WITH n WHERE 10 / n.v > 2 RETURN n.i", divided);
    expectManyRows("MATCH (n:Item) WHERE n.v <> 0 AND 10 / n.v > 2 RETURN n.i", divided);
    expectManyRows("MATCH (n:Item) WHERE n.v <> 0 AND 10 / n.v > 2 AND n.u = 1 RETURN n.i", dividedOnU);
    expectManyRows("MATCH (n:Item) WHERE n.v <> 0 WITH n WHERE n.u = 1 AND 10 / n.v > 2 RETURN n.i", dividedOnU);
    expectManyRows("MATCH (n:Item)-[:R]->(t) WHERE n.v <> 0 WITH n, t WHERE 10 / n.v > 2 AND t:Target RETURN n.i", divided);

    expectError("MATCH (n:Item) WITH n WHERE 10 / n.v > 2 RETURN n.i", "divide by zero");
    expectError("MATCH (n:Item) WHERE 10 / n.v > 2 AND n.v <> 0 RETURN n.i", "divide by zero");
    expectError("MATCH (n:Item) WHERE n.u = 1 WITH n WHERE 10 / n.v > 2 RETURN n.i", "divide by zero");
    expectError("MATCH (n:Item)-[:R]->(t) WHERE t:Target WITH n, t WHERE 10 / n.v > 2 AND n.u = 1 RETURN n.i", "divide by zero");
}

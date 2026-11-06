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

// A type test over edges a scan or a hop output tests the types it output beside them,
// carried past a WITH, a filter, a sort, a CASE and into a hop region. Each row has to keep
// the type of its own edge, across chunk boundaries.
class EdgeTypeColumnReadsCypherTest : public WriteQueryTest {
protected:
    // More items than one chunk holds. Item i reaches target i over an edge of type A where
    // i % 3 = 0 and of type B otherwise, and target i reaches an end over an edge with w = i % 5.
    static constexpr int64_t ITEM_COUNT = 3 * 65536 + 7;

    void createItems() {
        const std::string items = "UNWIND range(0, " + std::to_string(ITEM_COUNT - 1) + ") AS i ";
        applyWrite(items + "WITH i WHERE i % 3 = 0 "
                           "CREATE (:Item {i: i})-[:A {i: i}]->(t:Target {i: i})-[:C {w: i % 5}]->(:End {i: i})");
        applyWrite(items + "WITH i WHERE i % 3 <> 0 "
                           "CREATE (:Item {i: i})-[:B {i: i}]->(t:Target {i: i})-[:C {w: i % 5}]->(:End {i: i})");
    }

    static std::string isA(int64_t item) {
        return item % 3 == 0 ? "true" : "false";
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

TEST_F(EdgeTypeColumnReadsCypherTest, testsTheTypeOfEachEdgeAcrossChunks) {
    createItems();

    Rows types;
    Rows evenTypes;
    Rows lastTypes;
    Rows hopMatches;
    for (int64_t item = 0; item < ITEM_COUNT; item++) {
        const std::string itemText = std::to_string(item);
        types.push_back({itemText, isA(item)});

        if (item % 2 == 0) {
            evenTypes.push_back({itemText, isA(item)});
        }

        if (item >= ITEM_COUNT - 70000) {
            lastTypes.push_back({itemText, isA(item)});
        }

        if (item % 3 == 0 && item % 5 > 1) {
            hopMatches.push_back({itemText, itemText});
        }
    }

    expectManyRows("MATCH (n:Item)-[r]->() WITH n, r RETURN n.i, r:A", types);
    expectManyRows("MATCH (n:Item)-[r]->() WITH n, r WHERE r.i % 2 = 0 RETURN n.i, r:A", evenTypes);
    expectManyRows("MATCH (n:Item)-[r]->() WITH n, r ORDER BY n.i DESC LIMIT 70000 RETURN n.i, r:A", lastTypes);
    expectManyRows("MATCH (n:Item)-[r]->(t) WHERE r:A OR r.i % 2 = 0 "
                   "MATCH (t)(()-[e]->() WHERE r:A AND e.w > 1){1}(m) RETURN n.i, m.i",
                   hopMatches);
}

// The 3 KNOWS_WELL edges are Remy -> Adam, Adam -> Remy and Ghosts -> Remy
TEST_F(EdgeTypeColumnReadsCypherTest, testsTheScannedTypesAfterAWith) {
    expectRows("MATCH ()-[e]->() WITH e RETURN e.name, e:KNOWS_WELL",
               {{"Adam -> Bio", "false"},
                {"Adam -> Cooking", "false"},
                {"Adam -> Remy", "true"},
                {"Cyrus -> Gym", "false"},
                {"Cyrus -> Travel", "false"},
                {"Doruk -> Gym", "false"},
                {"Ghosts -> Remy", "true"},
                {"Luc -> Animals", "false"},
                {"Luc -> Computers", "false"},
                {"Martina -> Cooking", "false"},
                {"Maxime -> Bio", "false"},
                {"Maxime -> Padel", "false"},
                {"Remy -> Adam", "true"},
                {"Remy -> Computers", "false"},
                {"Remy -> Eighties", "false"},
                {"Remy -> Ghosts", "false"},
                {"Suhas -> Gym", "false"},
                {"Suhas -> Jiutjitsu", "false"}});
}

// Remy -> Adam, Adam -> Remy and Ghosts -> Remy are the KNOWS_WELL edges. Remy has 2 edges
// in, and Adam and Ghosts 1 each.
TEST_F(EdgeTypeColumnReadsCypherTest, filtersOnTheScannedTypesAfterAWith) {
    expectRows("MATCH (n)-[e]->(m) WITH e, n WHERE e:KNOWS_WELL RETURN n.name", {{"Adam"}, {"Ghosts"}, {"Remy"}});
    expectRows("MATCH (a)-->(n)-[e]->(m) WITH e, m WHERE e:KNOWS_WELL RETURN m.name",
               {{"Adam"}, {"Adam"}, {"Remy"}, {"Remy"}});
}

// The 3 KNOWS_WELL edges last 20, 20 and 200. Remy -> Ghosts, Remy -> Eighties and
// Luc -> Animals are the INTERESTED_IN edges lasting over 15.
TEST_F(EdgeTypeColumnReadsCypherTest, testsTheScannedTypesAcrossAFilter) {
    expectRows("MATCH ()-[e]->() WITH e WHERE e.duration > 15 RETURN e.name, e:KNOWS_WELL",
               {{"Adam -> Remy", "true"},
                {"Ghosts -> Remy", "true"},
                {"Luc -> Animals", "false"},
                {"Remy -> Adam", "true"},
                {"Remy -> Eighties", "false"},
                {"Remy -> Ghosts", "false"}});
}

TEST_F(EdgeTypeColumnReadsCypherTest, testsTheScannedTypesAcrossAnOrderByAndALimit) {
    expectRows("MATCH ()-[e]->() WITH e ORDER BY e.name LIMIT 3 RETURN e.name, e:KNOWS_WELL",
               {{"Adam -> Bio", "false"}, {"Adam -> Cooking", "false"}, {"Adam -> Remy", "true"}});
}

// Luc -> Computers lasts 15 and Martina -> Cooking 10, and the 10 other edges have no
// duration, so the CASE is null for them
TEST_F(EdgeTypeColumnReadsCypherTest, testsTheScannedTypesInACaseBranch) {
    expectRows("MATCH ()-[e]->() WITH e RETURN e.name, CASE WHEN e.duration > 15 THEN e:KNOWS_WELL END",
               {{"Adam -> Bio", "null"},
                {"Adam -> Cooking", "null"},
                {"Adam -> Remy", "true"},
                {"Cyrus -> Gym", "null"},
                {"Cyrus -> Travel", "null"},
                {"Doruk -> Gym", "null"},
                {"Ghosts -> Remy", "true"},
                {"Luc -> Animals", "false"},
                {"Luc -> Computers", "null"},
                {"Martina -> Cooking", "null"},
                {"Maxime -> Bio", "null"},
                {"Maxime -> Padel", "null"},
                {"Remy -> Adam", "true"},
                {"Remy -> Computers", "null"},
                {"Remy -> Eighties", "false"},
                {"Remy -> Ghosts", "false"},
                {"Suhas -> Gym", "null"},
                {"Suhas -> Jiutjitsu", "null"}});
}

// Remy -> Adam and Adam -> Remy are the KNOWS_WELL edges leaving a person. Adam's one edge
// lasting over 15 goes to Remy, and Remy's go to Adam, Ghosts and Eighties.
TEST_F(EdgeTypeColumnReadsCypherTest, testsTheScannedTypesInAHopRegion) {
    expectRows("MATCH (a:Person)-[r]->(n) WHERE r:KNOWS_WELL OR r.duration > 15 "
               "MATCH (n)(()-[e]->() WHERE r:KNOWS_WELL AND e.duration > 15){1}(m) RETURN a.name, n.name, m.name",
               {{"Adam", "Remy", "Adam"}, {"Adam", "Remy", "Eighties"}, {"Adam", "Remy", "Ghosts"}, {"Remy", "Adam", "Remy"}});
}

// Remy's KNOWS_WELL edge leads to Adam, whose walk reaches Remy, then Adam, Ghosts and
// Eighties. Adam's leads to Remy, whose walk reaches Adam, Ghosts and Eighties, then Remy
// through Adam and through Ghosts.
TEST_F(EdgeTypeColumnReadsCypherTest, testsTheScannedTypesInAHopRegionWithNoTestBefore) {
    expectRows("MATCH (a:Person)-[r]->(n) "
               "MATCH (n)(()-[e]->() WHERE r:KNOWS_WELL AND e.duration > 15){1,2}(m) RETURN a.name, m.name",
               {{"Adam", "Adam"},
                {"Adam", "Eighties"},
                {"Adam", "Ghosts"},
                {"Adam", "Remy"},
                {"Adam", "Remy"},
                {"Remy", "Adam"},
                {"Remy", "Eighties"},
                {"Remy", "Ghosts"},
                {"Remy", "Remy"}});
}

// Remy's edges to Adam, Ghosts, Computers and Eighties, then Adam's and Ghosts' edges to him
TEST_F(EdgeTypeColumnReadsCypherTest, testsTheTypesAnUndirectedHopOutputs) {
    expectRows("MATCH (a)-[e]-(b) WHERE a.name = 'Remy' WITH b, e RETURN b.name, e:KNOWS_WELL",
               {{"Adam", "true"},
                {"Adam", "true"},
                {"Computers", "false"},
                {"Eighties", "false"},
                {"Ghosts", "false"},
                {"Ghosts", "true"}});
}

TEST_F(EdgeTypeColumnReadsCypherTest, testsTheTypesAByTypeHopOutputs) {
    expectRows("MATCH (a)-[e:KNOWS_WELL|INTERESTED_IN]->(b) WHERE a.name = 'Remy' WITH b, e RETURN b.name, e:INTERESTED_IN",
               {{"Adam", "false"}, {"Computers", "true"}, {"Eighties", "true"}, {"Ghosts", "true"}});
}

// The KNOWS_WELL edges into a person
TEST_F(EdgeTypeColumnReadsCypherTest, testsTheTypesAByLabelEdgeScanOutputs) {
    expectRows("MATCH (a:Person)<-[e]-(b) WITH a, b, e RETURN a.name, b.name, e:KNOWS_WELL",
               {{"Adam", "Remy", "true"}, {"Remy", "Adam", "true"}, {"Remy", "Ghosts", "true"}});
}

// The hops of a walk output no types, so its hop region reads them. The 3 KNOWS_WELL edges
// last over 15, as do Remy -> Ghosts, Remy -> Eighties and Luc -> Animals.
TEST_F(EdgeTypeColumnReadsCypherTest, testsTheTypesOfEdgesNothingOutputThemBeside) {
    expectRows("MATCH (n)(()-[e]->() WHERE e:KNOWS_WELL OR e.duration > 15){1}(m) RETURN n.name, m.name",
               {{"Adam", "Remy"},
                {"Ghosts", "Remy"},
                {"Luc", "Animals"},
                {"Remy", "Adam"},
                {"Remy", "Eighties"},
                {"Remy", "Ghosts"}});
}

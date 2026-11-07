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

// A read past a variable-length walk shares the read of the rows the walk started from. The
// rows have to come out as they did when each read fetched the property again, across chunk
// boundaries, whatever else the walk holds: a hop predicate, a hop import, a set of ends.
class WalkPropertyReadsCypherTest : public WriteQueryTest {
protected:
    // More items than one chunk holds. Item i has v = i % 5, but none where i % 11 = 0, and
    // a chain of two targets: 2i with v = i % 3, then 2i + 1 with v = i % 7.
    static constexpr int64_t ITEM_COUNT = 3 * 65536 + 7;

    void createItems() {
        applyWrite("UNWIND range(0, " + std::to_string(ITEM_COUNT - 1) + ") AS i "
                   "CREATE (n:Item {i: i, v: CASE WHEN i % 11 <> 0 THEN i % 5 END}) "
                   "CREATE (n)-[:R]->(t:Target {i: 2 * i, v: i % 3}) "
                   "CREATE (t)-[:R]->(:Target {i: 2 * i + 1, v: i % 7})");
    }

    static bool hasValue(int64_t item) {
        return item % 11 != 0;
    }

    static std::string valueOf(int64_t item) {
        return hasValue(item) ? std::to_string(item % 5) : "null";
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

TEST_F(WalkPropertyReadsCypherTest, matchesTheEndsOfAWalkToTheSeedValue) {
    createItems();

    Rows matches;
    Rows evenItems;
    for (int64_t item = 0; item < ITEM_COUNT; item++) {
        const std::string itemText = std::to_string(item);
        const std::string value = valueOf(item);

        if (hasValue(item) && item % 5 > 1) {
            if (item % 3 == item % 5) {
                matches.push_back({itemText, value, std::to_string(2 * item)});
            }

            if (item % 7 == item % 5) {
                matches.push_back({itemText, value, std::to_string(2 * item + 1)});
            }
        }

        if (item % 2 == 0) {
            evenItems.push_back({itemText, value, std::to_string(2 * item)});
            evenItems.push_back({itemText, value, std::to_string(2 * item + 1)});
        }
    }

    expectManyRows("MATCH (n:Item) WHERE n.v > 1 MATCH (n)-[*1..2]->(m) WHERE m.v = n.v RETURN n.i, n.v, m.i", matches);
    expectManyRows("MATCH (n:Item) WHERE n.i % 2 = 0 MATCH (n)-[*1..2]->(m) RETURN n.i, n.v, m.i", evenItems);
}

// Remy and Adam are 32 and nobody else has an age. Remy reaches Adam, and himself through
// Adam and through Ghosts; Adam reaches Remy, and himself through Remy.
TEST_F(WalkPropertyReadsCypherTest, matchesTheEndsOfAWalkToTheSeedAge) {
    expectRows("MATCH (n:Person) WHERE n.age > 0 MATCH (n)-[*1..2]->(m) WHERE m.age = n.age RETURN n.name, n.age, m.name",
               {{"Remy", "32", "Adam"},
                {"Remy", "32", "Remy"},
                {"Remy", "32", "Remy"},
                {"Adam", "32", "Remy"},
                {"Adam", "32", "Adam"}});
    expectRows("MATCH (n:Person) WHERE n.age > 0 RETURN n.name, COUNT { MATCH (n)-[*1..2]->(m) WHERE m.age = n.age }",
               {{"Remy", "3"}, {"Adam", "2"}});
}

// The edges into Remy come from Adam and Ghosts, each reached from Remy; the one into Adam
// comes from Remy, reached from Adam and Ghosts
TEST_F(WalkPropertyReadsCypherTest, returnsTheSeedAgePastABackwardWalk) {
    expectRows("MATCH (n:Person) WHERE n.age > 0 MATCH (n)<-[*1..2]-(m) RETURN n.name, n.age, m.name",
               {{"Remy", "32", "Adam"},
                {"Remy", "32", "Ghosts"},
                {"Remy", "32", "Remy"},
                {"Remy", "32", "Remy"},
                {"Adam", "32", "Remy"},
                {"Adam", "32", "Adam"},
                {"Adam", "32", "Ghosts"}});
}

// The edges lasting more than 15 are Remy's to Adam, Ghosts and Eighties, Adam's and Ghosts'
// to Remy, and Luc's to Animals
TEST_F(WalkPropertyReadsCypherTest, returnsTheSeedAgePastAWalkWithAHopPredicate) {
    expectRows("MATCH (n:Person) WHERE n.age > 0 "
               "MATCH (n)((a)-[e]->(b) WHERE e.duration > 15){1,2}(m) RETURN n.name, n.age, m.name",
               {{"Remy", "32", "Adam"},
                {"Remy", "32", "Ghosts"},
                {"Remy", "32", "Eighties"},
                {"Remy", "32", "Remy"},
                {"Remy", "32", "Remy"},
                {"Adam", "32", "Remy"},
                {"Adam", "32", "Adam"},
                {"Adam", "32", "Ghosts"},
                {"Adam", "32", "Eighties"}});
}

// Ghosts' edge to Remy lasts 200, past the age of 32, and an edge with no duration fails the
// predicate; Remy's edges to Adam, Ghosts and Eighties and Adam's to Remy last 20
TEST_F(WalkPropertyReadsCypherTest, returnsTheSeedAgePastAWalkImportingIt) {
    expectRows("MATCH (n:Person) WHERE n.age > 0 "
               "MATCH (n)(()-[e]->() WHERE e.duration < n.age){1,2}(m) RETURN n.name, n.age, m.name",
               {{"Remy", "32", "Adam"},
                {"Remy", "32", "Ghosts"},
                {"Remy", "32", "Eighties"},
                {"Remy", "32", "Remy"},
                {"Adam", "32", "Remy"},
                {"Adam", "32", "Adam"},
                {"Adam", "32", "Ghosts"},
                {"Adam", "32", "Eighties"}});
}

TEST_F(WalkPropertyReadsCypherTest, returnsTheSeedAgePastAWalkEndingOnANamedNode) {
    expectRows("MATCH (n:Person) WHERE n.age > 0 MATCH (n)-[*1..2]->(m {name: 'Remy'}) RETURN n.name, n.age, m.name",
               {{"Remy", "32", "Remy"}, {"Remy", "32", "Remy"}, {"Adam", "32", "Remy"}});
}

TEST_F(WalkPropertyReadsCypherTest, returnsTheSeedAgePastAWalkEndingOnItsSeed) {
    expectRows("MATCH (n:Person) WHERE n.age > 0 MATCH (n)-[*1..3]->(n) RETURN n.name, n.age",
               {{"Remy", "32"}, {"Remy", "32"}, {"Adam", "32"}});
}

// Remy and Adam reach the same seven nodes between them
TEST_F(WalkPropertyReadsCypherTest, returnsTheSeedAgePastAWalkOfDistinctEnds) {
    expectRows("MATCH (n:Person) WHERE n.age > 0 MATCH (n)-[*1..2]->(m) RETURN DISTINCT n.age, m.name",
               {{"32", "Adam"},
                {"32", "Ghosts"},
                {"32", "Computers"},
                {"32", "Eighties"},
                {"32", "Remy"},
                {"32", "Bio"},
                {"32", "Cooking"}});
}

// 7 walks leave Adam, 8 leave Remy and 5 leave Ghosts; none leave Eighties or Animals
TEST_F(WalkPropertyReadsCypherTest, returnsACarriedEdgeValuePastAWalk) {
    expectRows("MATCH (a:Person)-[e]->(b) WHERE e.duration > 15 MATCH (b)-[*1..2]->(c) RETURN e.name, e.duration, count(c)",
               {{"Remy -> Adam", "20", "7"}, {"Adam -> Remy", "20", "8"}, {"Remy -> Ghosts", "20", "5"}});
}

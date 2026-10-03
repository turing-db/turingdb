#include <gtest/gtest.h>

#include <algorithm>
#include <string>
#include <vector>

#include "CallV3Test.h"
#include "StringRowSink.h"

using namespace turing::test;

namespace {

using Rows = std::vector<StringRowSink::Row>;

Rows sorted(Rows rows) {
    std::sort(rows.begin(), rows.end());
    return rows;
}

// variable-length-paths-0.json, the edge list rendered as its IDs joined by ", "
const Rows personToPersonForward {
    {"Remy", "0", "Adam"},
    {"Adam", "4", "Remy"},
    {"Remy", "0, 4", "Remy"},
    {"Remy", "1, 7", "Remy"},
    {"Adam", "4, 0", "Adam"},
    {"Remy", "1, 7, 0", "Adam"},
    {"Adam", "4, 1, 7", "Remy"},
    {"Remy", "0, 4, 1, 7", "Remy"},
    {"Remy", "1, 7, 0, 4", "Remy"},
    {"Adam", "4, 1, 7, 0", "Adam"},
};

// variable-length-paths-1.json
const Rows personToPersonBackward {
    {"Remy", "4", "Adam"},
    {"Adam", "0", "Remy"},
    {"Remy", "4, 0", "Remy"},
    {"Remy", "7, 1", "Remy"},
    {"Adam", "0, 4", "Adam"},
    {"Remy", "7, 1, 4", "Adam"},
    {"Adam", "0, 7, 1", "Remy"},
    {"Remy", "4, 0, 7, 1", "Remy"},
    {"Remy", "7, 1, 4, 0", "Remy"},
    {"Adam", "0, 7, 1, 4", "Adam"},
};

// The zero-length path of every Person, which a `*` adds to the rows above
const Rows everyPersonToItself {
    {"Remy", "", "Remy"},
    {"Adam", "", "Adam"},
    {"Maxime", "", "Maxime"},
    {"Luc", "", "Luc"},
    {"Martina", "", "Martina"},
    {"Suhas", "", "Suhas"},
    {"Cyrus", "", "Cyrus"},
    {"Doruk", "", "Doruk"},
};

Rows concatenated(const Rows& first, const Rows& second) {
    Rows rows = first;
    rows.insert(rows.end(), second.begin(), second.end());
    return rows;
}

}

// The eight variable-length oracles of the query test suite, run through the MLIR engine, and
// the shapes the legacy engine turns away
class ExplorePathsCypherTest : public CallV3Test {
};

TEST_F(ExplorePathsCypherTest, walksForwardOneOrMoreHops) {
    StringRowSink sink;
    runQuery("MATCH (n:Person)-[e]->+(m:Person) RETURN n.name, e, m.name", sink);

    Rows rows;
    sink.sortedRows(rows);
    EXPECT_EQ(rows, sorted(personToPersonForward));
}

TEST_F(ExplorePathsCypherTest, walksBackwardOneOrMoreHops) {
    StringRowSink sink;
    runQuery("MATCH (n:Person)<-[e]-+(m:Person) RETURN n.name, e, m.name", sink);

    Rows rows;
    sink.sortedRows(rows);
    EXPECT_EQ(rows, sorted(personToPersonBackward));
}

TEST_F(ExplorePathsCypherTest, walksForwardZeroOrMoreHops) {
    StringRowSink sink;
    runQuery("MATCH (n:Person)-[e]->*(m:Person) RETURN n.name, e, m.name", sink);

    Rows rows;
    sink.sortedRows(rows);
    EXPECT_EQ(rows, sorted(concatenated(everyPersonToItself, personToPersonForward)));
}

TEST_F(ExplorePathsCypherTest, walksBackwardZeroOrMoreHops) {
    StringRowSink sink;
    runQuery("MATCH (n:Person)<-[e]-*(m:Person) RETURN n.name, e, m.name", sink);

    Rows rows;
    sink.sortedRows(rows);
    EXPECT_EQ(rows, sorted(concatenated(everyPersonToItself, personToPersonBackward)));
}

TEST_F(ExplorePathsCypherTest, cutsAnUndirectedExplorationWithSkipAndLimit) {
    StringRowSink sink;
    runQuery("MATCH (n:Person)-[e]-*(m) RETURN n.name, e, m.name SKIP 50 LIMIT 5", sink);

    EXPECT_EQ(sink.getRows().size(), 5u);
}

TEST_F(ExplorePathsCypherTest, boundsTheHopsAndFiltersTheEndLabel) {
    StringRowSink sink;
    runQuery("MATCH (n:Person)-[e]->{2,4}(m:Interest) RETURN n.name, e, m.name", sink);

    Rows rows;
    sink.sortedRows(rows);

    // variable-length-paths-5.json
    const Rows expected {
        {"Remy", "0, 5", "Bio"},
        {"Remy", "0, 6", "Cooking"},
        {"Adam", "4, 1", "Ghosts"},
        {"Adam", "4, 2", "Computers"},
        {"Adam", "4, 3", "Eighties"},
        {"Remy", "0, 4, 1", "Ghosts"},
        {"Remy", "0, 4, 2", "Computers"},
        {"Remy", "0, 4, 3", "Eighties"},
        {"Remy", "1, 7, 2", "Computers"},
        {"Remy", "1, 7, 3", "Eighties"},
        {"Adam", "4, 0, 5", "Bio"},
        {"Adam", "4, 0, 6", "Cooking"},
        {"Remy", "1, 7, 0, 5", "Bio"},
        {"Remy", "1, 7, 0, 6", "Cooking"},
        {"Adam", "4, 1, 7, 2", "Computers"},
        {"Adam", "4, 1, 7, 3", "Eighties"},
    };
    EXPECT_EQ(rows, sorted(expected));
}

TEST_F(ExplorePathsCypherTest, continuesWithAPlainHopAfterTheExploration) {
    StringRowSink sink;
    runQuery("MATCH (n:Person)-[e]->{2,4}(m:Interest)-->(p:Person) RETURN n.name, e, m.name, p.name", sink);

    Rows rows;
    sink.sortedRows(rows);

    // variable-length-paths-6.json
    const Rows expected {
        {"Adam", "4, 1", "Ghosts", "Remy"},
        {"Remy", "0, 4, 1", "Ghosts", "Remy"},
    };
    EXPECT_EQ(rows, sorted(expected));
}

TEST_F(ExplorePathsCypherTest, exploresFromTheEndOfAPlainHop) {
    StringRowSink sink;
    runQuery("MATCH (i:Supernatural)-->(n:Person)-[e]->*(m:Person) RETURN i.name, n.name, e, m.name", sink);

    Rows rows;
    sink.sortedRows(rows);

    // variable-length-paths-7.json
    const Rows expected {
        {"Ghosts", "Remy", "", "Remy"},
        {"Ghosts", "Remy", "0", "Adam"},
        {"Ghosts", "Remy", "0, 4", "Remy"},
        {"Ghosts", "Remy", "1, 7", "Remy"},
        {"Ghosts", "Remy", "1, 7, 0", "Adam"},
        {"Ghosts", "Remy", "0, 4, 1, 7", "Remy"},
        {"Ghosts", "Remy", "1, 7, 0, 4", "Remy"},
    };
    EXPECT_EQ(rows, sorted(expected));
}

TEST_F(ExplorePathsCypherTest, filtersEveryHopByEdgeType) {
    StringRowSink sink;
    runQuery("MATCH (n:Person)-[e:KNOWS_WELL]->+(m) RETURN n.name, e, m.name", sink);

    Rows rows;
    sink.sortedRows(rows);

    // KNOWS_WELL edges: Remy->Adam (0), Adam->Remy (4), Ghosts->Remy (7)
    const Rows expected {
        {"Remy", "0", "Adam"},
        {"Remy", "0, 4", "Remy"},
        {"Adam", "4", "Remy"},
        {"Adam", "4, 0", "Adam"},
    };
    EXPECT_EQ(rows, sorted(expected));
}

TEST_F(ExplorePathsCypherTest, countsThePaths) {
    StringRowSink sink;
    runQuery("MATCH (n:Person)-[e]->+(m:Person) RETURN count(*)", sink);

    const Rows expected {{"10"}};
    EXPECT_EQ(sink.getRows(), expected);
}

TEST_F(ExplorePathsCypherTest, filtersTheEndAfterTheExploration) {
    StringRowSink sink;
    runQuery("MATCH (n:Person)-[e]->+(m:Person) WHERE m.name = 'Adam' RETURN n.name, e", sink);

    Rows rows;
    sink.sortedRows(rows);

    const Rows expected {
        {"Remy", "0"},
        {"Adam", "4, 0"},
        {"Remy", "1, 7, 0"},
        {"Adam", "4, 1, 7, 0"},
    };
    EXPECT_EQ(rows, sorted(expected));
}

TEST_F(ExplorePathsCypherTest, readsThePathLength) {
    StringRowSink sink;
    runQuery("MATCH (n:Person)-[e]->+(m:Person) RETURN size(e)", sink);

    Rows rows;
    sink.sortedRows(rows);

    const Rows expected {{"1"}, {"1"}, {"2"}, {"2"}, {"2"}, {"3"}, {"3"}, {"4"}, {"4"}, {"4"}};
    EXPECT_EQ(rows, expected);
}

TEST_F(ExplorePathsCypherTest, comparesThePathLength) {
    StringRowSink sink;
    runQuery("MATCH (n:Person)-[e]->+(m:Person) WHERE size(e) > 2 RETURN e", sink);

    Rows rows;
    sink.sortedRows(rows);

    const Rows expected {
        {"0, 4, 1, 7"},
        {"1, 7, 0"},
        {"1, 7, 0, 4"},
        {"4, 1, 7"},
        {"4, 1, 7, 0"},
    };
    EXPECT_EQ(rows, expected);
}

TEST_F(ExplorePathsCypherTest, dedupsPathsAsLists) {
    StringRowSink sink;
    runQuery("MATCH (n:Person)-[e]->+(m:Person) RETURN DISTINCT e", sink);

    Rows rows;
    sink.sortedRows(rows);

    Rows expected;
    for (const StringRowSink::Row& row : personToPersonForward) {
        expected.push_back({row[1]});
    }
    EXPECT_EQ(rows, sorted(expected));
}

TEST_F(ExplorePathsCypherTest, ordersPathsAsLists) {
    StringRowSink sink;
    runQuery("MATCH (n:Person)-[e]->+(m:Person) RETURN e ORDER BY e", sink);

    const Rows expected {
        {"0"},
        {"0, 4"},
        {"0, 4, 1, 7"},
        {"1, 7"},
        {"1, 7, 0"},
        {"1, 7, 0, 4"},
        {"4"},
        {"4, 0"},
        {"4, 1, 7"},
        {"4, 1, 7, 0"},
    };
    EXPECT_EQ(sink.getRows(), expected);
}

TEST_F(ExplorePathsCypherTest, limitsThePathsEmitted) {
    StringRowSink sink;
    runQuery("MATCH (n:Person)-[e]->+(m:Person) RETURN e LIMIT 3", sink);

    EXPECT_EQ(sink.getRows().size(), 3u);
}

TEST_F(ExplorePathsCypherTest, carriesAPathThroughAWith) {
    StringRowSink sink;
    runQuery("MATCH (n:Person)-[e]->+(m:Person) WITH e, m.name AS name RETURN name, e", sink);

    Rows rows;
    sink.sortedRows(rows);

    Rows expected;
    for (const StringRowSink::Row& row : personToPersonForward) {
        expected.push_back({row[2], row[1]});
    }
    EXPECT_EQ(rows, sorted(expected));
}

TEST_F(ExplorePathsCypherTest, closesAJoinOnABoundEnd) {
    StringRowSink sink;
    runQuery("MATCH (a:Person)-[e:KNOWS_WELL]->+(b), (a)-[:KNOWS_WELL]->(b) RETURN a.name, e, b.name", sink);

    Rows rows;
    sink.sortedRows(rows);

    // Of the KNOWS_WELL trails between two Persons, only the single hops end where a direct
    // KNOWS_WELL edge lands
    const Rows expected {
        {"Adam", "4", "Remy"},
        {"Remy", "0", "Adam"},
    };
    EXPECT_EQ(rows, expected);
}

TEST_F(ExplorePathsCypherTest, rejectsAPropertyOfThePath) {
    runQueryExpectingError("MATCH (n:Person)-[e]->+(m:Person) RETURN e.name", "binds the list");
}

TEST_F(ExplorePathsCypherTest, rejectsATypeCheckOfThePath) {
    runQueryExpectingError("MATCH (n:Person)-[e]->+(m:Person) WHERE e:KNOWS_WELL RETURN n.name", "binds the list");
}

// Hop predicates: the three spellings constrain every hop of the path, not its end

TEST_F(ExplorePathsCypherTest, filtersEveryHopByAnInlineWhere) {
    StringRowSink sink;
    runQuery("MATCH (n:Person)-[e:INTERESTED_IN WHERE e.duration > 15]->{1,2}(m) RETURN n.name, e, m.name", sink);

    Rows rows;
    sink.sortedRows(rows);

    // Remy->Ghosts (1) and Remy->Eighties (3) last 20, Luc->Animals (10) too; Luc->Computers
    // lasts 15 and Martina->Cooking 10; Adam's interests carry no duration
    const Rows expected {
        {"Remy", "1", "Ghosts"},
        {"Remy", "3", "Eighties"},
        {"Luc", "10", "Animals"},
    };
    EXPECT_EQ(rows, sorted(expected));
}

TEST_F(ExplorePathsCypherTest, filtersEveryHopByAPropertyMap) {
    StringRowSink sink;
    runQuery("MATCH (n:Person)-[e:INTERESTED_IN {duration: 20}]->{1,2}(m) RETURN n.name, e, m.name", sink);

    Rows rows;
    sink.sortedRows(rows);

    const Rows expected {
        {"Remy", "1", "Ghosts"},
        {"Remy", "3", "Eighties"},
        {"Luc", "10", "Animals"},
    };
    EXPECT_EQ(rows, sorted(expected));
}

TEST_F(ExplorePathsCypherTest, filtersEveryHopByTheEndLabelOfAParenthesizedPattern) {
    StringRowSink sink;
    runQuery("MATCH (n:Person)((a)-[e]->(b:Person)){1,3}(m) RETURN n.name, e, m.name", sink);

    Rows rows;
    sink.sortedRows(rows);

    // Remy->Ghosts fails the hop, so nothing goes through Ghosts: the Remy-Adam 2-cycle is all
    // that is left, and its third hop would reuse an edge
    const Rows expected {
        {"Remy", "0", "Adam"},
        {"Adam", "4", "Remy"},
        {"Remy", "0, 4", "Remy"},
        {"Adam", "4, 0", "Adam"},
    };
    EXPECT_EQ(rows, sorted(expected));
}

TEST_F(ExplorePathsCypherTest, filtersEveryHopByAParenthesizedWhere) {
    StringRowSink sink;
    runQuery("MATCH (n:Person)((a)-[e]->(b) WHERE b.age > 30){1,2}(m) RETURN n.name, e, m.name", sink);

    Rows rows;
    sink.sortedRows(rows);

    // Only Remy and Adam carry an age
    const Rows expected {
        {"Remy", "0", "Adam"},
        {"Adam", "4", "Remy"},
        {"Remy", "0, 4", "Remy"},
        {"Adam", "4, 0", "Adam"},
    };
    EXPECT_EQ(rows, sorted(expected));
}

TEST_F(ExplorePathsCypherTest, aHopPredicateNoEdgePassesLeavesTheZeroLengthPaths) {
    StringRowSink sink;
    runQuery("MATCH (n:Person)-[e WHERE e.duration > 1000]->*(m) RETURN n.name, e, m.name", sink);

    Rows rows;
    sink.sortedRows(rows);
    EXPECT_EQ(rows, sorted(everyPersonToItself));
}

TEST_F(ExplorePathsCypherTest, bindsTheGroupVariablesToTheNodeLists) {
    StringRowSink ends;
    runQuery("MATCH (n:Person)((a)-[e]->(b:Person)){1,3}(m) RETURN n.name, b", ends);

    Rows endRows;
    ends.sortedRows(endRows);

    const Rows expectedEnds {
        {"Remy", "1"},
        {"Adam", "0"},
        {"Remy", "1, 0"},
        {"Adam", "0, 1"},
    };
    EXPECT_EQ(endRows, sorted(expectedEnds));

    StringRowSink sources;
    runQuery("MATCH (n:Person)((a)-[e]->(b:Person)){1,3}(m) RETURN n.name, a", sources);

    Rows sourceRows;
    sources.sortedRows(sourceRows);

    const Rows expectedSources {
        {"Remy", "0"},
        {"Adam", "1"},
        {"Remy", "0, 1"},
        {"Adam", "1, 0"},
    };
    EXPECT_EQ(sourceRows, sorted(expectedSources));
}

TEST_F(ExplorePathsCypherTest, anInlineWhereOnAPlainPatternIsAMatchPredicate) {
    StringRowSink sink;
    runQuery("MATCH (n:Person WHERE n.age = 32)-->(m) RETURN n.name, m.name", sink);

    Rows rows;
    sink.sortedRows(rows);

    // Remy and Adam are 32; their neighbours
    const Rows expected {
        {"Remy", "Adam"},
        {"Remy", "Ghosts"},
        {"Remy", "Computers"},
        {"Remy", "Eighties"},
        {"Adam", "Remy"},
        {"Adam", "Bio"},
        {"Adam", "Cooking"},
    };
    EXPECT_EQ(rows, sorted(expected));
}

TEST_F(ExplorePathsCypherTest, rejectsAPropertyOfAGroupVariable) {
    runQueryExpectingError("MATCH (n:Person)((a)-[e]->(b)){1,3}(m) RETURN b.name", "binds the list");
}

TEST_F(ExplorePathsCypherTest, unwindsAPathIntoItsEdges) {
    StringRowSink sink;
    runQuery("MATCH (n:Person)-[e:KNOWS_WELL]->+(m) UNWIND e AS x RETURN n.name, x", sink);

    Rows rows;
    sink.sortedRows(rows);

    // The paths [0], [0, 4] from Remy and [4], [4, 0] from Adam, one row per edge
    const Rows expected {
        {"Adam", "0"},
        {"Adam", "4"},
        {"Adam", "4"},
        {"Remy", "0"},
        {"Remy", "0"},
        {"Remy", "4"},
    };
    EXPECT_EQ(rows, expected);
}

TEST_F(ExplorePathsCypherTest, readsAPropertyOfAnUnwoundPathEdge) {
    StringRowSink sink;
    runQuery("MATCH (n:Person)-[e:KNOWS_WELL]->{2,2}(m) UNWIND e AS x RETURN x.name", sink);

    Rows rows;
    sink.sortedRows(rows);

    const Rows expected {
        {"Adam -> Remy"},
        {"Adam -> Remy"},
        {"Remy -> Adam"},
        {"Remy -> Adam"},
    };
    EXPECT_EQ(rows, expected);
}

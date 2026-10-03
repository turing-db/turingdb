#include <gtest/gtest.h>

#include <algorithm>
#include <string>
#include <string_view>
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

bool contains(std::string_view text, std::string_view part) {
    return text.find(part) != std::string_view::npos;
}

}

// length(p), nodes(p) and relationships(p) read a named path: the hop count, the nodes it
// runs through in written order and its relationships, null where an OPTIONAL MATCH bound
// no path
class NamedPathFunctionsTest : public CallV3Test {
protected:
    void optimisedDump(std::string_view query, std::string& dump) {
        StringRowSink sink;
        runQuery(query, sink);

        for (const StringRowSink::Row& row : sink.getRows()) {
            if (row.front() == "db") {
                dump = row.back();
            }
        }
    }
};

TEST_F(NamedPathFunctionsTest, readsAFixedLengthHop) {
    StringRowSink sink;
    runQuery("MATCH p = (n:Person)-[e]->(m:Person) RETURN n.name, length(p), nodes(p), relationships(p)", sink);

    Rows rows;
    sink.sortedRows(rows);

    EXPECT_EQ(rows, sorted(Rows {
        {"Remy", "1", "0, 1", "0"},
        {"Adam", "1", "1, 0", "4"},
    }));
}

TEST_F(NamedPathFunctionsTest, readsEveryHopOfAWalk) {
    StringRowSink sink;
    runQuery("MATCH p = (n:Person)-[e]->+(m:Person) RETURN n.name, length(p), nodes(p), relationships(p)", sink);

    Rows rows;
    sink.sortedRows(rows);

    EXPECT_EQ(rows, sorted(Rows {
        {"Remy", "1", "0, 1", "0"},
        {"Adam", "1", "1, 0", "4"},
        {"Remy", "2", "0, 1, 0", "0, 4"},
        {"Remy", "2", "0, 6, 0", "1, 7"},
        {"Adam", "2", "1, 0, 1", "4, 0"},
        {"Remy", "3", "0, 6, 0, 1", "1, 7, 0"},
        {"Adam", "3", "1, 0, 6, 0", "4, 1, 7"},
        {"Remy", "4", "0, 1, 0, 6, 0", "0, 4, 1, 7"},
        {"Remy", "4", "0, 6, 0, 1, 0", "1, 7, 0, 4"},
        {"Adam", "4", "1, 0, 6, 0, 1", "4, 1, 7, 0"},
    }));
}

TEST_F(NamedPathFunctionsTest, readsAZeroLengthWalkAsItsSeedAlone) {
    StringRowSink sink;
    runQuery("MATCH p = (n:Person)-[e]->*(m:Person) WHERE n.name = 'Luc' RETURN length(p), nodes(p), relationships(p)", sink);

    EXPECT_EQ(sink.getRows(), (Rows {{"0", "9", ""}}));
}

TEST_F(NamedPathFunctionsTest, readsAWalkThatRanAgainstThePatternInWrittenOrder) {
    StringRowSink sink;
    runQuery("MATCH p = (n:Person)-[e]->+(m:Person {name: 'Adam'}) RETURN p, length(p), nodes(p), relationships(p)", sink);

    Rows rows;
    sink.sortedRows(rows);

    EXPECT_EQ(rows, sorted(Rows {
        {"(0), [0], (1)", "1", "0, 1", "0"},
        {"(1), [4], (0), [0], (1)", "2", "1, 0, 1", "4, 0"},
        {"(0), [1], (6), [7], (0), [0], (1)", "3", "0, 6, 0, 1", "1, 7, 0"},
        {"(1), [4], (0), [1], (6), [7], (0), [0], (1)", "4", "1, 0, 6, 0, 1", "4, 1, 7, 0"},
    }));
}

TEST_F(NamedPathFunctionsTest, readsAPathThroughAFixedHopAndAWalk) {
    StringRowSink sink;
    runQuery("MATCH p = (n:Person)-[x]->(k:Person)-[e]->+(m:Person) WHERE length(p) = 2 RETURN nodes(p), relationships(p)", sink);

    Rows rows;
    sink.sortedRows(rows);

    EXPECT_EQ(rows, sorted(Rows {
        {"0, 1, 0", "0, 4"},
        {"1, 0, 1", "4, 0"},
    }));
}

TEST_F(NamedPathFunctionsTest, readsNullWhereAnOptionalMatchMissed) {
    StringRowSink sink;
    runQuery("MATCH (n:Person) OPTIONAL MATCH p = (n)-[e]->(m:Person) RETURN n.name, length(p), nodes(p), relationships(p)", sink);

    Rows rows;
    sink.sortedRows(rows);

    EXPECT_EQ(rows, sorted(Rows {
        {"Remy", "1", "0, 1", "0"},
        {"Adam", "1", "1, 0", "4"},
        {"Maxime", "null", "null", "null"},
        {"Luc", "null", "null", "null"},
        {"Martina", "null", "null", "null"},
        {"Suhas", "null", "null", "null"},
        {"Cyrus", "null", "null", "null"},
        {"Doruk", "null", "null", "null"},
    }));
}

TEST_F(NamedPathFunctionsTest, readsNullWhereAnOptionalWalkMissed) {
    StringRowSink sink;
    runQuery("MATCH (n:Person) OPTIONAL MATCH p = (n)-[e]->+(m:Person) WITH n, p WHERE p IS NULL OR length(p) = 1 RETURN n.name, length(p), nodes(p)", sink);

    Rows rows;
    sink.sortedRows(rows);

    EXPECT_EQ(rows, sorted(Rows {
        {"Remy", "1", "0, 1"},
        {"Adam", "1", "1, 0"},
        {"Maxime", "null", "null"},
        {"Luc", "null", "null"},
        {"Martina", "null", "null"},
        {"Suhas", "null", "null"},
        {"Cyrus", "null", "null"},
        {"Doruk", "null", "null"},
    }));
}

TEST_F(NamedPathFunctionsTest, readsAPathCarriedThroughAWith) {
    StringRowSink sink;
    runQuery("MATCH p = (n:Person)-[e]->+(m:Person) WITH p, n WHERE length(p) = 2 RETURN n.name, nodes(p), relationships(p)", sink);

    Rows rows;
    sink.sortedRows(rows);

    EXPECT_EQ(rows, sorted(Rows {
        {"Remy", "0, 1, 0", "0, 4"},
        {"Remy", "0, 6, 0", "1, 7"},
        {"Adam", "1, 0, 1", "4, 0"},
    }));
}

TEST_F(NamedPathFunctionsTest, iteratesTheNodesOfAPath) {
    StringRowSink sink;
    runQuery("MATCH p = (n:Person)-[e]->+(m:Person) WHERE length(p) = 2 RETURN [x IN nodes(p) | x.name]", sink);

    Rows rows;
    sink.sortedRows(rows);

    EXPECT_EQ(rows, sorted(Rows {
        {"Remy, Adam, Remy"},
        {"Remy, Ghosts, Remy"},
        {"Adam, Remy, Adam"},
    }));
}

TEST_F(NamedPathFunctionsTest, unwindsTheRelationshipsOfAPath) {
    StringRowSink sink;
    runQuery("MATCH p = (n:Person)-[e]->+(m:Person) WHERE length(p) = 2 UNWIND relationships(p) AS r RETURN r.name", sink);

    Rows rows;
    sink.sortedRows(rows);

    EXPECT_EQ(rows, sorted(Rows {
        {"Remy -> Adam"},
        {"Adam -> Remy"},
        {"Remy -> Ghosts"},
        {"Ghosts -> Remy"},
        {"Adam -> Remy"},
        {"Remy -> Adam"},
    }));
}

TEST_F(NamedPathFunctionsTest, filtersPathsByAPredicateOverTheirRelationships) {
    StringRowSink sink;
    runQuery("MATCH p = (n:Person)-[e]->+(m:Person) WHERE all(x IN relationships(p) WHERE x.duration = 20) RETURN n.name, length(p)", sink);

    Rows rows;
    sink.sortedRows(rows);

    EXPECT_EQ(rows, sorted(Rows {
        {"Remy", "1"},
        {"Adam", "1"},
        {"Remy", "2"},
        {"Adam", "2"},
    }));
}

TEST_F(NamedPathFunctionsTest, readsTheLengthOffTheWalksHandles) {
    std::string dump;
    optimisedDump("EXPLAIN (db) MATCH p = (n:Person)-[e]->+(m:Person) RETURN length(p)", dump);

    EXPECT_TRUE(contains(dump, "db.path_length")) << dump;
    EXPECT_FALSE(contains(dump, "db.path_elements")) << dump;
    EXPECT_FALSE(contains(dump, "db.make_path")) << dump;
}

TEST_F(NamedPathFunctionsTest, readsTheNodesOffTheWalksHandles) {
    std::string dump;
    optimisedDump("EXPLAIN (db) MATCH p = (n:Person)-[e]->+(m:Person) RETURN nodes(p)", dump);

    EXPECT_TRUE(contains(dump, "kind nodes")) << dump;
    EXPECT_FALSE(contains(dump, "db.path_elements")) << dump;
    EXPECT_FALSE(contains(dump, "db.make_path")) << dump;
}

TEST_F(NamedPathFunctionsTest, readsTheRelationshipsOffTheWalksHandles) {
    std::string dump;
    optimisedDump("EXPLAIN (db) MATCH p = (n:Person)-[e]->+(m:Person) RETURN relationships(p)", dump);

    EXPECT_TRUE(contains(dump, "kind edges")) << dump;
    EXPECT_FALSE(contains(dump, "db.path_elements")) << dump;
    EXPECT_FALSE(contains(dump, "db.make_path")) << dump;
}

TEST_F(NamedPathFunctionsTest, buildsThePathAFixedHopNeeds) {
    std::string dump;
    optimisedDump("EXPLAIN (db) MATCH p = (n:Person)-[e]->(m:Person) RETURN nodes(p)", dump);

    EXPECT_TRUE(contains(dump, "db.path_elements")) << dump;
    EXPECT_TRUE(contains(dump, "db.make_path")) << dump;
}

TEST_F(NamedPathFunctionsTest, readsNoPathOffANode) {
    runQueryExpectingError("MATCH (n:Person) RETURN nodes(n)", "Invalid arguments for function 'nodes'");
    runQueryExpectingError("MATCH (n:Person)-[e]->(m) RETURN relationships(e)", "Invalid arguments for function 'relationships'");
}

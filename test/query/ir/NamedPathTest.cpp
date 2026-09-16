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

}

// MATCH p = ... binds the whole element: the nodes and the edges it runs through, read
// here as (node id) and [edge id]
class NamedPathTest : public CallV3Test {
};

TEST_F(NamedPathTest, bindsTheNodeOfAnElementWithNoHop) {
    StringRowSink sink;
    runQuery("MATCH p = (n:Person) RETURN n.name, p", sink);

    Rows rows;
    sink.sortedRows(rows);

    EXPECT_EQ(rows.size(), 8u);
    EXPECT_EQ(rows, sorted(Rows {
        {"Remy", "(0)"},
        {"Adam", "(1)"},
        {"Maxime", "(8)"},
        {"Luc", "(9)"},
        {"Martina", "(11)"},
        {"Suhas", "(12)"},
        {"Cyrus", "(15)"},
        {"Doruk", "(17)"},
    }));
}

TEST_F(NamedPathTest, bindsAFixedLengthHop) {
    StringRowSink sink;
    runQuery("MATCH p = (n:Person)-[e]->(m:Person) RETURN n.name, p, m.name", sink);

    Rows rows;
    sink.sortedRows(rows);

    EXPECT_EQ(rows, sorted(Rows {
        {"Remy", "(0), [0], (1)", "Adam"},
        {"Adam", "(1), [4], (0)", "Remy"},
    }));
}

TEST_F(NamedPathTest, bindsEveryHopOfAVariableLengthWalk) {
    StringRowSink sink;
    runQuery("MATCH p = (n:Person)-[e]->+(m:Person) RETURN n.name, p, m.name", sink);

    Rows rows;
    sink.sortedRows(rows);

    EXPECT_EQ(rows, sorted(Rows {
        {"Remy", "(0), [0], (1)", "Adam"},
        {"Adam", "(1), [4], (0)", "Remy"},
        {"Remy", "(0), [0], (1), [4], (0)", "Remy"},
        {"Remy", "(0), [1], (6), [7], (0)", "Remy"},
        {"Adam", "(1), [4], (0), [0], (1)", "Adam"},
        {"Remy", "(0), [1], (6), [7], (0), [0], (1)", "Adam"},
        {"Adam", "(1), [4], (0), [1], (6), [7], (0)", "Remy"},
        {"Remy", "(0), [0], (1), [4], (0), [1], (6), [7], (0)", "Remy"},
        {"Remy", "(0), [1], (6), [7], (0), [0], (1), [4], (0)", "Remy"},
        {"Adam", "(1), [4], (0), [1], (6), [7], (0), [0], (1)", "Adam"},
    }));
}

TEST_F(NamedPathTest, readsAZeroLengthWalkAsItsSeedAlone) {
    StringRowSink sink;
    runQuery("MATCH p = (n:Person)-[e]->*(m:Person) RETURN n.name, p, m.name", sink);

    Rows rows;
    sink.sortedRows(rows);

    EXPECT_EQ(rows.size(), 18u);

    const auto isSeedAlone = [](const StringRowSink::Row& row) {
        return row[1] == "(0)";
    };
    EXPECT_EQ(std::ranges::count_if(rows, isSeedAlone), 1);
}

TEST_F(NamedPathTest, runsThroughAFixedHopAndAWalk) {
    StringRowSink sink;
    runQuery("MATCH p = (n:Person)-[x]->(k:Person)-[e]->+(m:Person) RETURN n.name, p, m.name", sink);

    Rows rows;
    sink.sortedRows(rows);

    EXPECT_EQ(rows, sorted(Rows {
        {"Remy", "(0), [0], (1), [4], (0)", "Remy"},
        {"Adam", "(1), [4], (0), [0], (1)", "Adam"},
        {"Remy", "(0), [0], (1), [4], (0), [0], (1)", "Adam"},
        {"Adam", "(1), [4], (0), [0], (1), [4], (0)", "Remy"},
        {"Remy", "(0), [0], (1), [4], (0), [1], (6), [7], (0)", "Remy"},
        {"Adam", "(1), [4], (0), [1], (6), [7], (0)", "Remy"},
        {"Adam", "(1), [4], (0), [1], (6), [7], (0), [0], (1)", "Adam"},
        {"Remy", "(0), [0], (1), [4], (0), [1], (6), [7], (0), [0], (1)", "Adam"},
        {"Adam", "(1), [4], (0), [0], (1), [4], (0), [1], (6), [7], (0)", "Remy"},
        {"Adam", "(1), [4], (0), [1], (6), [7], (0), [0], (1), [4], (0)", "Remy"},
    }));
}

TEST_F(NamedPathTest, walksBackwardInTheOrderItIsWritten) {
    StringRowSink sink;
    runQuery("MATCH p = (n:Person)<-[e]-(m:Person) RETURN n.name, p, m.name", sink);

    Rows rows;
    sink.sortedRows(rows);

    EXPECT_EQ(rows, sorted(Rows {
        {"Remy", "(0), [4], (1)", "Adam"},
        {"Adam", "(1), [0], (0)", "Remy"},
    }));
}

TEST_F(NamedPathTest, carriesAPathThroughAWith) {
    StringRowSink sink;
    runQuery("MATCH p = (n:Person)-[e]->(m:Person) WITH p, n RETURN n.name, p", sink);

    Rows rows;
    sink.sortedRows(rows);

    EXPECT_EQ(rows, sorted(Rows {
        {"Remy", "(0), [0], (1)"},
        {"Adam", "(1), [4], (0)"},
    }));
}

TEST_F(NamedPathTest, sortsAndCutsTheRowsAPathRidesOn) {
    StringRowSink sink;
    runQuery("MATCH p = (n:Person)-[e]->+(m:Person) RETURN n.name, p ORDER BY n.name LIMIT 3", sink);

    Rows rows;
    sink.sortedRows(rows);

    EXPECT_EQ(rows, sorted(Rows {
        {"Adam", "(1), [4], (0)"},
        {"Adam", "(1), [4], (0), [1], (6), [7], (0)"},
        {"Adam", "(1), [4], (0), [1], (6), [7], (0), [0], (1)"},
    }));
}

TEST_F(NamedPathTest, countsThePathsItBound) {
    StringRowSink sink;
    runQuery("MATCH p = (n:Person)-[e]->+(m:Person) RETURN count(p)", sink);

    EXPECT_EQ(sink.getRows(), Rows {{"10"}});
}

TEST_F(NamedPathTest, readsNoPropertyOffAPath) {
    runQueryExpectingError("MATCH p = (n:Person)-[e]->+(m:Person) RETURN p.name",
                           "Variable 'p' is 'GraphPath' it must be a node or edge");
}

TEST_F(NamedPathTest, readsNoLabelOffAPath) {
    runQueryExpectingError("MATCH p = (n:Person)-[e]->+(m:Person) RETURN labels(p)",
                           "Invalid arguments for function 'labels'");
}

TEST_F(NamedPathTest, refusesToNameThePathOfAnOptionalMatch) {
    runQueryExpectingError("MATCH (n:Person) OPTIONAL MATCH p = (n)-[e]->(m:Interest) RETURN n.name, p",
                           "Variable 'p' names the path of an OPTIONAL MATCH, which is not supported yet");
}

TEST_F(NamedPathTest, refusesToNameThePathOfAWrittenPattern) {
    runWriteExpectingError("CREATE p = (n:Person {name: 'Zoe'})-[:KNOWS]->(m:Person {name: 'Yann'}) RETURN p",
                           "Variable 'p' names the path of a written pattern, which is not supported yet");
}

TEST_F(NamedPathTest, refusesANameAnEntityAlreadyBinds) {
    runQueryExpectingError("MATCH n = (n:Person)-[e]->+(m:Person) RETURN n",
                           "Variable 'n' is already bound: a named path takes a name of its own");
}

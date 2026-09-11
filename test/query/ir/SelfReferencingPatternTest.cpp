#include <gtest/gtest.h>

#include <algorithm>
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

// Remy -0-> Adam -4-> Remy and Remy -1-> Ghosts -7-> Remy are simpledb's only cycles, and
// either can be walked on from the other, so each of the three nodes closes a two-hop
// trail and a four-hop one. The edges are listed in the order the pattern walks them.
const Rows personCycles {
    {"Adam", "4, 0"},
    {"Adam", "4, 1, 7, 0"},
    {"Remy", "0, 4"},
    {"Remy", "0, 4, 1, 7"},
    {"Remy", "1, 7"},
    {"Remy", "1, 7, 0, 4"},
};

// The same cycles under a pattern that bound the node they close on first: Remy -0-> Adam
// and Adam -4-> Remy and Remy -1-> Ghosts are the edges landing on one of them
const Rows cyclesUnderABoundStart {
    {"Adam", "4", "Remy", "0, 4"},
    {"Adam", "4", "Remy", "0, 4, 1, 7"},
    {"Adam", "4", "Remy", "1, 7"},
    {"Adam", "4", "Remy", "1, 7, 0, 4"},
    {"Remy", "0", "Adam", "4, 0"},
    {"Remy", "0", "Adam", "4, 1, 7, 0"},
    {"Remy", "1", "Ghosts", "7, 0, 4, 1"},
    {"Remy", "1", "Ghosts", "7, 1"},
};

const Rows shortCyclesUnderABoundStart {
    {"Adam", "4", "Remy", "0, 4"},
    {"Adam", "4", "Remy", "1, 7"},
    {"Remy", "0", "Adam", "4, 0"},
    {"Remy", "1", "Ghosts", "7, 1"},
};

}

// A pattern whose walk ends on the variable it started from. Breaking the cycle leaves
// that variable behind one merge edge per end, so the walk is opened at one of those ends:
// at the one the pattern traverses out of, or the traversal comes out reversed. A pattern
// that bound the variable earlier has to cross the merge, which it can only do once both
// ends carry rows.
class SelfReferencingPatternTest : public CallV3Test {
};

TEST_F(SelfReferencingPatternTest, walksTheCycleTheWayThePatternIsWritten) {
    StringRowSink sink;
    runQuery("MATCH (a:Person)-[e]->+(a) RETURN a.name, e", sink);

    Rows rows;
    sink.sortedRows(rows);
    EXPECT_EQ(rows, sorted(personCycles));
}

TEST_F(SelfReferencingPatternTest, walksBackToAStartASecondMatchBound) {
    StringRowSink sink;
    runQuery("MATCH (x:Person)-[r]->(a) MATCH (a)-[e]->+(a) RETURN x.name, r, a.name, e", sink);

    Rows rows;
    sink.sortedRows(rows);
    EXPECT_EQ(rows, sorted(cyclesUnderABoundStart));
}

TEST_F(SelfReferencingPatternTest, walksBackToAStartTheSameMatchBound) {
    StringRowSink sink;
    runQuery("MATCH (x:Person)-[r]->(a), (a)-[e]->+(a) RETURN x.name, r, a.name, e", sink);

    Rows rows;
    sink.sortedRows(rows);
    EXPECT_EQ(rows, sorted(cyclesUnderABoundStart));
}

TEST_F(SelfReferencingPatternTest, boundsTheHopsWalkedBackToABoundStart) {
    StringRowSink sink;
    runQuery("MATCH (x:Person)-[r]->(a) MATCH (a)-[e]->{1,3}(a) RETURN x.name, r, a.name, e", sink);

    Rows rows;
    sink.sortedRows(rows);
    EXPECT_EQ(rows, sorted(shortCyclesUnderABoundStart));
}

TEST_F(SelfReferencingPatternTest, findsNoSelfLoopOnABoundStart) {
    StringRowSink sink;
    runQuery("MATCH (x:Person)-[r]->(a) MATCH (a)-[e]->(a) RETURN x.name, r, a.name", sink);

    Rows rows;
    sink.sortedRows(rows);
    EXPECT_TRUE(rows.empty());
}

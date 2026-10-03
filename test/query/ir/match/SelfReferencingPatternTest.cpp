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

bool contains(std::string_view text, std::string_view needle) {
    return text.find(needle) != std::string_view::npos;
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
protected:
    std::string_view dumpOf(const StringRowSink& sink, std::string_view stage) {
        for (const StringRowSink::Row& row : sink.getRows()) {
            if (row.front() == stage) {
                return row.back();
            }
        }

        return {};
    }
};

// The end a self-referencing pattern walks back to is the seed of its own row, which is the
// per-row target fuse_explore_end_nodes exists to fold into the exploration
TEST_F(SelfReferencingPatternTest, foldsTheSeedItWalksBackToIntoTheExploration) {
    StringRowSink sink;
    runQuery("EXPLAIN (around fuse_explore_end_nodes) MATCH (a:Person)-[e]->{1,7}(a) RETURN count(a)", sink);

    const std::string_view before = dumpOf(sink, "before fuse_explore_end_nodes");
    EXPECT_TRUE(contains(before, "db.eq")) << before;

    const std::string_view after = dumpOf(sink, "after fuse_explore_end_nodes");
    EXPECT_TRUE(contains(after, "ends_on_seed")) << after;
    EXPECT_FALSE(contains(after, "db.eq")) << after;
}

// Breaking the cycle leaves the walk opening on an end of its own, which carries the labels
// of the variable it merges into or opens by reading every node in the graph
TEST_F(SelfReferencingPatternTest, opensTheCycleOnALabelScan) {
    StringRowSink sink;
    runQuery("EXPLAIN (db) MATCH (a:Person)-[e]->{1,7}(a) RETURN count(a)", sink);

    const std::string_view program = dumpOf(sink, "db");
    EXPECT_TRUE(contains(program, "db.scan_nodes_by_label")) << program;
    EXPECT_FALSE(contains(program, "db.scan_nodes(")) << program;
}

// A walk ending where it began holds the seed at both ends, so a predicate on the end is one
// on the seed and sinks to the scan that opened it
TEST_F(SelfReferencingPatternTest, sinksAPredicateOnTheCycleToItsScan) {
    StringRowSink sink;
    runQuery("EXPLAIN (db) MATCH (a:Person {name: 'Remy'})-[e]->{1,7}(a) RETURN count(a)", sink);

    const std::string_view program = dumpOf(sink, "db");
    EXPECT_TRUE(contains(program, "db.scan_nodes_by_property_value")) << program;
}

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

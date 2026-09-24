#include <gtest/gtest.h>

#include <string>
#include <string_view>
#include <vector>

#include "CallV3Test.h"
#include "StringRowSink.h"

using namespace turing::test;

// Each pattern closing a cycle is checked against the same pattern with the repeated node
// spelled as a fresh variable and tied back with an id() equality, which the dependency
// graph sees as no cycle at all. The T graph holds the triangle a->b->c->a, the 2-cycle
// c->d->c and the self-loop d->d.
class CycleShapesTest : public CallV3Test {
protected:
    void initialize() override {
        CallV3Test::initialize();

        runWrite("CREATE (a:T {name: 'a'}), (b:T {name: 'b'}), (c:T {name: 'c'}), (d:T {name: 'd'}), "
                 "(a)-[:R]->(b), (b)-[:R]->(c), (c)-[:R]->(a), "
                 "(c)-[:R]->(d), (d)-[:R]->(c), (d)-[:R]->(d)");
    }

    void expectSameRows(std::string_view cycle, std::string_view reference) {
        std::vector<StringRowSink::Row> cycleRows;
        collectRows(cycle, cycleRows);

        std::vector<StringRowSink::Row> referenceRows;
        collectRows(reference, referenceRows);

        EXPECT_FALSE(referenceRows.empty()) << reference;
        EXPECT_EQ(cycleRows, referenceRows) << cycle;
    }

private:
    void collectRows(std::string_view query, std::vector<StringRowSink::Row>& rows) {
        StringRowSink sink;
        runQuery(query, sink);
        sink.sortedRows(rows);
    }
};

TEST_F(CycleShapesTest, closesATriangle) {
    expectSameRows("MATCH (x:T)-->(y:T)-->(z:T)-->(x) RETURN x.name, y.name, z.name",
                   "MATCH (x:T)-->(y:T)-->(z:T)-->(w:T) WHERE id(w) = id(x) RETURN x.name, y.name, z.name");
}

TEST_F(CycleShapesTest, closesATriangleAgainstTheHops) {
    expectSameRows("MATCH (x:T)-->(y:T)<--(z:T)<--(x) RETURN x.name, y.name, z.name",
                   "MATCH (x:T)-->(y:T)<--(z:T)<--(w:T) WHERE id(w) = id(x) RETURN x.name, y.name, z.name");
}

TEST_F(CycleShapesTest, closesAnUndirectedTriangle) {
    expectSameRows("MATCH (x:T)--(y:T)--(z:T)--(x) RETURN x.name, y.name, z.name",
                   "MATCH (x:T)--(y:T)--(z:T)--(w:T) WHERE id(w) = id(x) RETURN x.name, y.name, z.name");
}

TEST_F(CycleShapesTest, closesASquare) {
    expectSameRows("MATCH (x:T)-->(y:T)-->(z:T)-->(v:T)-->(x) RETURN x.name, y.name, z.name, v.name",
                   "MATCH (x:T)-->(y:T)-->(z:T)-->(v:T)-->(w:T) WHERE id(w) = id(x) RETURN x.name, y.name, z.name, v.name");
}

TEST_F(CycleShapesTest, reachesATriangleOverAHop) {
    expectSameRows("MATCH (s:T)-->(x:T)-->(y:T)-->(z:T)-->(x) RETURN s.name, x.name, y.name, z.name",
                   "MATCH (s:T)-->(x:T)-->(y:T)-->(z:T)-->(w:T) WHERE id(w) = id(x) RETURN s.name, x.name, y.name, z.name");
}

TEST_F(CycleShapesTest, leavesATriangleOverAHop) {
    expectSameRows("MATCH (x:T)-->(y:T)-->(z:T)-->(x)-->(s:T) RETURN s.name, x.name, y.name, z.name",
                   "MATCH (x:T)-->(y:T)-->(z:T)-->(w:T)-->(s:T) WHERE id(w) = id(x) RETURN s.name, x.name, y.name, z.name");
}

TEST_F(CycleShapesTest, reachesTheMiddleOfATriangleFromAnotherPattern) {
    expectSameRows("MATCH (x:T)-->(y:T)-->(z:T)-->(x), (s:T)-->(y) RETURN s.name, x.name, y.name, z.name",
                   "MATCH (x:T)-->(y:T)-->(z:T)-->(w:T), (s:T)-->(y) WHERE id(w) = id(x) RETURN s.name, x.name, y.name, z.name");
}

TEST_F(CycleShapesTest, chainsASelfLoop) {
    expectSameRows("MATCH (x:T)-->(x)-->(x) RETURN x.name",
                   "MATCH (x:T)-->(y:T)-->(z:T) WHERE id(y) = id(x) AND id(z) = id(x) RETURN x.name");
}

TEST_F(CycleShapesTest, passesThroughASelfLoop) {
    expectSameRows("MATCH (s:T)-->(x:T)-->(x)-->(t:T) RETURN s.name, x.name, t.name",
                   "MATCH (s:T)-->(x:T)-->(y:T)-->(t:T) WHERE id(y) = id(x) RETURN s.name, x.name, t.name");
}

TEST_F(CycleShapesTest, closesTheThetaOfThreePaths) {
    expectSameRows("MATCH (x:T)-->(y:T)-->(z:T), (x)-->(v:T)-->(z), (x)-->(z) RETURN x.name, y.name, v.name, z.name",
                   "MATCH (x:T)-->(y:T)-->(z:T), (x2:T)-->(v:T)-->(z2:T), (x3:T)-->(z3:T) "
                   "WHERE id(x2) = id(x) AND id(z2) = id(z) AND id(x3) = id(x) AND id(z3) = id(z) "
                   "RETURN x.name, y.name, v.name, z.name");
}

TEST_F(CycleShapesTest, readsTheLabelOfTheRepeatedNode) {
    expectSameRows("MATCH (x)-->(y:T)-->(x:T) RETURN x.name, y.name",
                   "MATCH (x)-->(y:T)-->(w:T) WHERE id(w) = id(x) RETURN x.name, y.name");
}

TEST_F(CycleShapesTest, readsThePropertyOfTheRepeatedNode) {
    expectSameRows("MATCH (x:T)-->(y:T)-->(x {name: 'c'}) RETURN x.name, y.name",
                   "MATCH (x:T)-->(y:T)-->(w {name: 'c'}) WHERE id(w) = id(x) RETURN x.name, y.name");
}

// Two cycles through x: the dependency graph merges each one into a copy of x of its own,
// and x is then the merge of those two copies.
TEST_F(CycleShapesTest, closesTwoCyclesThroughOneNode) {
    expectSameRows("MATCH (x:T)-->(y:T)-->(x)-->(z:T)-->(x) RETURN x.name, y.name, z.name",
                   "MATCH (x:T)-->(y:T)-->(w:T)-->(z:T)-->(v:T) WHERE id(w) = id(x) AND id(v) = id(x) "
                   "RETURN x.name, y.name, z.name");
}

TEST_F(CycleShapesTest, closesTwoCyclesThroughOneNodeInTwoPatterns) {
    expectSameRows("MATCH (x:T)-->(y:T)-->(x), (x)-->(z:T)-->(x) RETURN x.name, y.name, z.name",
                   "MATCH (x:T)-->(y:T)-->(w:T), (u:T)-->(z:T)-->(v:T) "
                   "WHERE id(w) = id(x) AND id(u) = id(x) AND id(v) = id(x) RETURN x.name, y.name, z.name");
}

TEST_F(CycleShapesTest, closesTwoCyclesThroughOneNodeInTwoClauses) {
    expectSameRows("MATCH (x:T)-->(y:T)-->(x) MATCH (x)-->(z:T)-->(x) RETURN x.name, y.name, z.name",
                   "MATCH (x:T)-->(y:T)-->(w:T) MATCH (u:T)-->(z:T)-->(v:T) "
                   "WHERE id(w) = id(x) AND id(u) = id(x) AND id(v) = id(x) RETURN x.name, y.name, z.name");
}

TEST_F(CycleShapesTest, closesTwoUndirectedCyclesThroughOneNode) {
    expectSameRows("MATCH (x:T)--(y:T)--(x)--(z:T)--(x) RETURN x.name, y.name, z.name",
                   "MATCH (x:T)--(y:T)--(w:T)--(z:T)--(v:T) WHERE id(w) = id(x) AND id(v) = id(x) "
                   "RETURN x.name, y.name, z.name");
}

TEST_F(CycleShapesTest, closesTwoTrianglesThroughOneNode) {
    expectSameRows("MATCH (x:T)-->(y:T)-->(z:T)-->(x)-->(u:T)-->(v:T)-->(x) RETURN x.name, y.name, z.name, u.name, v.name",
                   "MATCH (x:T)-->(y:T)-->(z:T)-->(w:T)-->(u:T)-->(v:T)-->(t:T) WHERE id(w) = id(x) AND id(t) = id(x) "
                   "RETURN x.name, y.name, z.name, u.name, v.name");
}

TEST_F(CycleShapesTest, closesThreeCyclesThroughOneNode) {
    expectSameRows("MATCH (x:T)-->(y:T)-->(x)-->(z:T)-->(x)-->(u:T)-->(x) RETURN x.name, y.name, z.name, u.name",
                   "MATCH (x:T)-->(y:T)-->(w:T)-->(z:T)-->(v:T)-->(u:T)-->(t:T) "
                   "WHERE id(w) = id(x) AND id(v) = id(x) AND id(t) = id(x) RETURN x.name, y.name, z.name, u.name");
}

TEST_F(CycleShapesTest, closesTwoCyclesThroughANodeTheyReachAtTheirMiddle) {
    expectSameRows("MATCH (y:T)-->(x:T)-->(y), (z:T)-->(x)-->(z) RETURN x.name, y.name, z.name",
                   "MATCH (y:T)-->(x:T)-->(w:T), (z:T)-->(u:T)-->(v:T) "
                   "WHERE id(w) = id(y) AND id(u) = id(x) AND id(v) = id(z) RETURN x.name, y.name, z.name");
}

// The two pairs of parallel hops and the triangle they sit on share their hops, so each
// cycle is only found in the graph that detaching the one before it left.
TEST_F(CycleShapesTest, closesCyclesThatShareTheirHops) {
    expectSameRows("MATCH (x:T)-->(y:T), (x)--(y)<--(z:T)<--(x), (z)<--(x) RETURN x.name, y.name, z.name",
                   "MATCH (x:T)-->(y:T), (x2:T)--(y2:T)<--(z:T)<--(x3:T), (z2:T)<--(x4:T) "
                   "WHERE id(x2) = id(x) AND id(y2) = id(y) AND id(x3) = id(x) AND id(z2) = id(z) AND id(x4) = id(x) "
                   "RETURN x.name, y.name, z.name");
}

// A cycle left once the others are detached can run through a merge edge at every node, so
// its head is split at its one hop.
TEST_F(CycleShapesTest, closesACycleThroughMergeEdges) {
    expectSameRows("MATCH (x:T)--(y:T)-->(x)--(z:T), (x)-->(y)--(z)--(x)--(x), (y)-->(z)--(y)<--(z) RETURN count(*)",
                   "MATCH (x:T)--(y:T)-->(x1:T)--(z:T), (x2:T)-->(y1:T)--(z1:T)--(x3:T)--(x4:T), (y2:T)-->(z2:T)--(y3:T)<--(z3:T) "
                   "WHERE id(x1) = id(x) AND id(x2) = id(x) AND id(y1) = id(y) AND id(z1) = id(z) AND id(x3) = id(x) "
                   "AND id(x4) = id(x) AND id(y2) = id(y) AND id(z2) = id(z) AND id(y3) = id(y) AND id(z3) = id(z) "
                   "RETURN count(*)");
}

// Enough cycles through x that merging their copies pairwise adds variables to the graph
// while it walks them.
TEST_F(CycleShapesTest, closesManyCyclesThroughOneNode) {
    expectSameRows("MATCH (x:T)-->(x)-->(y:T)-->(z:T)<--(y), (z)<--(z)<--(x)-->(x)--(x)<--(x), (y)-->(x)--(x)--(x), (x)<--(z)--(y) "
                   "RETURN count(*)",
                   "MATCH (x:T)-->(x1:T)-->(y:T)-->(z:T)<--(y1:T), (z1:T)<--(z2:T)<--(x2:T)-->(x3:T)--(x4:T)<--(x5:T), "
                   "(y2:T)-->(x6:T)--(x7:T)--(x8:T), (x9:T)<--(z3:T)--(y3:T) "
                   "WHERE id(x1) = id(x) AND id(y1) = id(y) AND id(z1) = id(z) AND id(z2) = id(z) AND id(x2) = id(x) "
                   "AND id(x3) = id(x) AND id(x4) = id(x) AND id(x5) = id(x) AND id(y2) = id(y) AND id(x6) = id(x) "
                   "AND id(x7) = id(x) AND id(x8) = id(x) AND id(x9) = id(x) AND id(z3) = id(z) AND id(y3) = id(y) "
                   "RETURN count(*)");
}

TEST_F(CycleShapesTest, closesTwoCyclesThroughANodeAWithBound) {
    expectSameRows("MATCH (x:T) WITH x MATCH (x)-->(y:T)-->(x)-->(z:T)-->(x) RETURN x.name, y.name, z.name",
                   "MATCH (x:T)-->(y:T)-->(w:T)-->(z:T)-->(v:T) WHERE id(w) = id(x) AND id(v) = id(x) "
                   "RETURN x.name, y.name, z.name");
}

TEST_F(CycleShapesTest, closesACycleOverAWith) {
    expectSameRows("MATCH (x:T)-->(y:T) WITH x, y MATCH (y)-->(z:T)-->(x) RETURN x.name, y.name, z.name",
                   "MATCH (x:T)-->(y:T)-->(z:T)-->(w:T) WHERE id(w) = id(x) RETURN x.name, y.name, z.name");
}

TEST_F(CycleShapesTest, closesACycleInAnOptionalMatch) {
    expectSameRows("MATCH (x:T)-->(y:T) OPTIONAL MATCH (y)-->(z:T)-->(x) RETURN x.name, y.name, z.name",
                   "MATCH (x:T)-->(y:T) OPTIONAL MATCH (y)-->(z:T)-->(w:T) WHERE id(w) = id(x) "
                   "RETURN x.name, y.name, z.name");
}

int main(int argc, char** argv) {
    return turing::test::turingTestMain(argc, argv);
}

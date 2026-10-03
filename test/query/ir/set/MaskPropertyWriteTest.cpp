#include <gtest/gtest.h>

#include "WriteQueryTest.h"

using namespace db;
using namespace turing::test;

// A label test or an edge type test is a mask, and a write takes it as the boolean it is:
// CREATE and SET store it as a Bool property, and MERGE keys a pattern on it against the
// Bool property the graph holds.
class MaskPropertyWriteTest : public WriteQueryTest {
};

TEST_F(MaskPropertyWriteTest, createsAPropertyFromTheLabelTest) {
    applyWrite("MATCH (n) WHERE n.name = 'Remy' OR n.name = 'Ghosts' "
               "CREATE (t:Tag {name: n.name, isPerson: n:Person})");

    expectRows("MATCH (t:Tag) RETURN t.name, t.isPerson", {{"Ghosts", "false"}, {"Remy", "true"}});
}

TEST_F(MaskPropertyWriteTest, setsAPropertyFromTheLabelTest) {
    applyWrite("MATCH (n) WHERE n.name = 'Remy' OR n.name = 'Ghosts' SET n.isPerson = n:Person");

    expectRows("MATCH (n) WHERE n.name = 'Remy' OR n.name = 'Ghosts' RETURN n.name, n.isPerson",
               {{"Ghosts", "false"}, {"Remy", "true"}});
}

// Three of the eighteen simpledb edges are KNOWS_WELL
TEST_F(MaskPropertyWriteTest, setsAPropertyFromTheEdgeTypeTest) {
    applyWrite("MATCH ()-[e]->() SET e.knowsWell = e:KNOWS_WELL");

    expectRows("MATCH ()-[e]->() RETURN e.knowsWell, count(*)", {{"true", "3"}, {"false", "15"}});
}

// Eighteen rows key two patterns: the one every person merges into and the one every
// interest merges into
TEST_F(MaskPropertyWriteTest, keysANodeOnTheLabelTestOfAMatchedNode) {
    expectWriteRowCount("MATCH (n) MERGE (t:Tag {isPerson: n:Person})", 0);

    expectRows("MATCH (t:Tag) RETURN t.isPerson", {{"false"}, {"true"}});
}

// The second merge reads the Bool property the first one wrote off the graph and keys
// the mask against it, so it binds the two tags rather than writing two more
TEST_F(MaskPropertyWriteTest, bindsTheNodeAnEarlierMaskKeyedMergeWrote) {
    expectWriteRowCount("MATCH (n) MERGE (t:Tag {isPerson: n:Person})", 0);
    expectWriteRowCount("MATCH (n) MERGE (t:Tag {isPerson: n:Person})", 0);

    expectRows("MATCH (t:Tag) RETURN count(t)", {{"2"}});
}

TEST_F(MaskPropertyWriteTest, keysANodeOnTheNegatedLabelTest) {
    expectWriteRowCount("MATCH (n) WHERE n.name = 'Remy' MERGE (t:Tag {isPerson: NOT n:Person})", 0);

    expectRows("MATCH (t:Tag) RETURN t.isPerson", {{"false"}});
}

TEST_F(MaskPropertyWriteTest, keysANodeOnTheTypeTestOfAMatchedEdge) {
    expectWriteRowCount("MATCH ()-[e]->() MERGE (t:Tag {knowsWell: e:KNOWS_WELL})", 0);

    expectRows("MATCH (t:Tag) RETURN t.knowsWell", {{"false"}, {"true"}});
}

int main(int argc, char** argv) {
    return turing::test::turingTestMain(argc, argv);
}

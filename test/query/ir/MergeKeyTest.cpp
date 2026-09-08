#include <gtest/gtest.h>

#include "WriteQueryTest.h"

using namespace db;
using namespace turing::test;

// What a merge keys an entity on: the property values it asks for, told apart from the
// values another spec asks for, and compared against the graph's own whatever type the
// schema holds them as.
class MergeKeyTest : public WriteQueryTest {
};

// The two hops constrain different properties to values with the same bytes, so nothing
// but the property they name tells the patterns apart: the second pattern is not the
// first, so it is not found and is written whole - a second chain, ends and all
TEST_F(MergeKeyTest, writesAHopConstrainingAnotherPropertyToTheSameValue) {
    expectWriteRowCount("MERGE (a:Tag {name: 'x'})-[:LINKS {alpha: 1}]->(b:Tag {name: 'y'}) "
                        "MERGE (c:Tag {name: 'x'})-[:LINKS {beta: 1}]->(d:Tag {name: 'y'})",
                        0);

    expectRows("MATCH (a:Tag)-[:LINKS]->(b:Tag) RETURN count(*)", {{"2"}});
    expectRows("MATCH (t:Tag) RETURN count(t)", {{"4"}});
}

// The nodes of those two merges do name the same property, so the second binds what the
// first wrote rather than writing a second copy
TEST_F(MergeKeyTest, bindsTheNodeAnEarlierMergeOfTheSameSpecWrote) {
    expectWriteRowCount("MERGE (a:Tag {name: 'x'}) MERGE (b:Tag {name: 'x'})", 0);

    expectRows("MATCH (t:Tag) RETURN count(t)", {{"1"}});
}

// The schema holds score as a double, so the integer the pattern asks for keys as the
// double the graph reads back: 2 is the 2.0 the node carries, and the merge binds it
TEST_F(MergeKeyTest, bindsADoubleTypedPropertyAnIntegerConstrains) {
    expectWriteRowCount("CREATE (s:Score {score: 2.0})", 0);

    expectWriteRowCount("MERGE (s:Score {score: 2}) RETURN s.score", 1);

    expectRows("MATCH (s:Score) RETURN count(s)", {{"1"}});
}

// The same widening the other way round: nothing carries 3.0, so the merge writes it,
// and merging the integer again binds what it wrote
TEST_F(MergeKeyTest, bindsAPendingNodeAnIntegerAndADoubleBothConstrain) {
    expectWriteRowCount("CREATE (s:Score {score: 2.0})", 0);

    expectWriteRowCount("MERGE (s:Score {score: 3}) MERGE (t:Score {score: 3.0})", 0);

    expectRows("MATCH (s:Score) RETURN count(s)", {{"2"}});
}

// labels() owns the characters of the string it hands back, where a property column
// borrows them from the graph. That owned string is a merge key like any other value: the
// one Person matched writes one Tag under its label set.
TEST_F(MergeKeyTest, keysANodeOnTheLabelsOfAMatchedNode) {
    expectWriteRowCount("MATCH (p:Person {name: 'Remy'}) MERGE (t:Tag {name: labels(p)})", 0);

    expectRows("MATCH (t:Tag) RETURN t.name", {{"Person, SoftwareEngineering, Founder"}});
}

// The edge sibling of the same key: edgeType() is owned the way labels() is
TEST_F(MergeKeyTest, keysANodeOnTheTypeOfAMatchedEdge) {
    expectWriteRowCount("MATCH (:Person {name: 'Remy'})-[e:KNOWS_WELL]->() "
                        "MERGE (t:Tag {name: edgeType(e)})",
                        0);

    expectRows("MATCH (t:Tag) RETURN t.name", {{"KNOWS_WELL"}});
}

int main(int argc, char** argv) {
    return turing::test::turingTestMain(argc, argv);
}

#include <gtest/gtest.h>

#include "WriteQueryTest.h"

using namespace db;
using namespace turing::test;

// MERGE of a path pattern with a hop: the rows it binds when the graph holds the whole
// pattern, and the entities it writes - all of them, not only the hop - when it does not.
class MergeEdgeTest : public WriteQueryTest {
};

// Remy knows Adam well already, so the merge binds that hop and writes nothing
TEST_F(MergeEdgeTest, bindsAHopThePatternFinds) {
    expectWriteRows("MATCH (a:Person {name: 'Remy'}), (b:Person {name: 'Adam'}) "
                    "MERGE (a)-[e:KNOWS_WELL]->(b) "
                    "RETURN e.name",
                    {{"Remy -> Adam"}});
}

// There is no such hop, so the merge writes one between the two bound ends
TEST_F(MergeEdgeTest, writesAHopBetweenTwoBoundEnds) {
    expectWriteRowCount("MATCH (a:Person {name: 'Remy'}), (b:Person {name: 'Luc'}) "
                        "MERGE (a)-[e:KNOWS_WELL]->(b)",
                        0);

    expectRows("MATCH (a:Person {name: 'Remy'})-[:KNOWS_WELL]->(b) RETURN b.name",
               {{"Adam"}, {"Luc"}});
}

// A hop pointing the other way is a different pattern, so the merge writes it
TEST_F(MergeEdgeTest, writesAHopThePatternFindsOnlyTheOtherWayRound) {
    expectWriteRowCount("MATCH (a:Person {name: 'Remy'}), (g:Interest {name: 'Ghosts'}) "
                        "MERGE (a)-[e:KNOWS_WELL]->(g)",
                        0);

    expectRows("MATCH (a:Person {name: 'Remy'})-[:KNOWS_WELL]->(b) RETURN b.name",
               {{"Adam"}, {"Ghosts"}});
}

// An undirected hop takes the one the graph holds either way round, so this binds the
// Ghosts -> Remy edge and writes nothing
TEST_F(MergeEdgeTest, bindsAnUndirectedHopEitherWayRound) {
    expectWriteRows("MATCH (a:Person {name: 'Remy'}), (g:Interest {name: 'Ghosts'}) "
                    "MERGE (a)-[e:KNOWS_WELL]-(g) "
                    "RETURN e.name",
                    {{"Ghosts -> Remy"}});
}

// A hop the pattern reads backwards binds the edge that points that way
TEST_F(MergeEdgeTest, bindsABackwardHop) {
    expectWriteRows("MATCH (a:Person {name: 'Remy'}), (g:Interest {name: 'Ghosts'}) "
                    "MERGE (a)<-[e:KNOWS_WELL]-(g) "
                    "RETURN e.name",
                    {{"Ghosts -> Remy"}});
}

// The property is part of the pattern, so a hop of that type carrying another value is
// not the pattern and a second hop is written
TEST_F(MergeEdgeTest, writesAHopWhoseFoundSiblingCarriesAnotherPropertyValue) {
    expectWriteRowCount("MATCH (a:Person {name: 'Remy'}), (b:Person {name: 'Adam'}) "
                        "MERGE (a)-[e:KNOWS_WELL {duration: 5}]->(b)",
                        0);

    expectRows("MATCH (a:Person {name: 'Remy'})-[e:KNOWS_WELL]->(b:Person {name: 'Adam'}) "
               "RETURN e.duration",
               {{"20"}, {"5"}});
}

TEST_F(MergeEdgeTest, bindsAHopCarryingThePropertyValueThePatternAsksFor) {
    expectWriteRows("MATCH (a:Person {name: 'Remy'}), (b:Person {name: 'Adam'}) "
                    "MERGE (a)-[e:KNOWS_WELL {duration: 20}]->(b) "
                    "RETURN e.name",
                    {{"Remy -> Adam"}});
}

// Neither end is bound and no such path exists, so the merge writes the whole pattern -
// both nodes and the hop - rather than reusing a node that matches one end
TEST_F(MergeEdgeTest, writesTheWholePatternItDoesNotFind) {
    expectWriteRows("MERGE (a:Tag {name: 'x'})-[e:LINKS]->(b:Tag {name: 'y'}) "
                    "RETURN a.name, b.name",
                    {{"x", "y"}});

    expectRows("MATCH (a:Tag)-[:LINKS]->(b:Tag) RETURN a.name, b.name", {{"x", "y"}});
}

// The pattern is found this time, so nothing is written and the found path is bound
TEST_F(MergeEdgeTest, bindsTheWholePatternItFinds) {
    expectWriteRows("MERGE (a:Tag {name: 'x'})-[e:LINKS]->(b:Tag {name: 'y'}) RETURN a.name", {{"x"}});
    expectWriteRows("MERGE (a:Tag {name: 'x'})-[e:LINKS]->(b:Tag {name: 'y'}) RETURN b.name", {{"y"}});

    expectRows("MATCH (t:Tag) RETURN count(t)", {{"2"}});
}

// The canonical shape: two node merges then a hop merge between what they bound. The
// hop's ends are entities this change wrote, which no graph the match reads holds, so
// the pattern is written - once.
TEST_F(MergeEdgeTest, writesAHopBetweenTwoNodesTheSameQueryWrote) {
    expectWriteRowCount("MERGE (a:Tag {name: 'x'}) "
                        "MERGE (b:Tag {name: 'y'}) "
                        "MERGE (a)-[e:LINKS]->(b)",
                        0);

    expectRows("MATCH (a:Tag)-[:LINKS]->(b:Tag) RETURN a.name, b.name", {{"x", "y"}});
    expectRows("MATCH (t:Tag) RETURN count(t)", {{"2"}});
}

// Two hop merges over the same pair write one edge: the second binds the pending one the
// first wrote
TEST_F(MergeEdgeTest, writesOneHopForTwoMergesOfOnePattern) {
    expectWriteRowCount("MATCH (a:Person {name: 'Remy'}), (b:Person {name: 'Luc'}) "
                        "MERGE (a)-[:KNOWS_WELL]->(b) "
                        "MERGE (a)-[:KNOWS_WELL]->(b)",
                        0);

    expectRows("MATCH (a:Person {name: 'Remy'})-[:KNOWS_WELL]->(b) RETURN b.name",
               {{"Adam"}, {"Luc"}});
}

// A three-node chain is matched and written as one pattern
TEST_F(MergeEdgeTest, writesAChainOfTwoHops) {
    expectWriteRows("MERGE (a:Tag {name: 'x'})-[:LINKS]->(b:Tag {name: 'y'})-[:LINKS]->(c:Tag {name: 'z'}) "
                    "RETURN a.name, b.name, c.name",
                    {{"x", "y", "z"}});

    expectRows("MATCH (a:Tag)-[:LINKS]->(b:Tag)-[:LINKS]->(c:Tag) RETURN a.name, c.name",
               {{"x", "z"}});
}

// Each hop of a chain is logged under its own property values, so merging the chain
// again binds every hop of it rather than writing a second chain
TEST_F(MergeEdgeTest, writesOneChainForTwoMergesOfOnePattern) {
    expectWriteRowCount("MERGE (a:Tag {name: 'x'})-[:LINKS {weight: 1}]->(b:Tag {name: 'y'})"
                        "-[:LINKS {weight: 2}]->(c:Tag {name: 'z'}) "
                        "MERGE (d:Tag {name: 'x'})-[:LINKS {weight: 1}]->(e:Tag {name: 'y'})"
                        "-[:LINKS {weight: 2}]->(f:Tag {name: 'z'})",
                        0);

    expectRows("MATCH (a:Tag)-[:LINKS]->(b:Tag) RETURN count(*)", {{"2"}});
    expectRows("MATCH (t:Tag) RETURN count(t)", {{"3"}});
}

// ON CREATE over a hop merge writes the property of the edge it wrote
TEST_F(MergeEdgeTest, setsThePropertyOfTheHopItWrote) {
    expectWriteRowCount("MATCH (a:Person {name: 'Remy'}), (b:Person {name: 'Luc'}) "
                        "MERGE (a)-[e:KNOWS_WELL]->(b) ON CREATE SET e.duration = 3",
                        0);

    expectRows("MATCH (a:Person {name: 'Remy'})-[e:KNOWS_WELL]->(b:Person {name: 'Luc'}) "
               "RETURN e.duration",
               {{"3"}});
}

// ON MATCH over a hop merge writes the property of the edge it bound
TEST_F(MergeEdgeTest, setsThePropertyOfTheHopItBound) {
    expectWriteRowCount("MATCH (a:Person {name: 'Remy'}), (b:Person {name: 'Adam'}) "
                        "MERGE (a)-[e:KNOWS_WELL]->(b) ON MATCH SET e.duration = 3",
                        0);

    expectRows("MATCH (a:Person {name: 'Remy'})-[e:KNOWS_WELL]->(b:Person {name: 'Adam'}) "
               "RETURN e.duration",
               {{"3"}});
}

int main(int argc, char** argv) {
    return turing::test::turingTestMain(argc, argv);
}

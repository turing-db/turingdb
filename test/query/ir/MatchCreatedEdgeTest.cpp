#include <gtest/gtest.h>

#include <string>
#include <vector>

#include "iterators/ChunkConfig.h"

#include "CallV3Test.h"
#include "StringRowSink.h"

using namespace db;
using namespace turing::test;

class MatchCreatedEdgeTest : public CallV3Test {
};

// MATCH (a)-->(m) below the cut walks an edge the CREATE above it wrote: the graph holds
// neither the edge nor the node it lands on until the commit, so the hop reads them out of
// the write buffer.
TEST_F(MatchCreatedEdgeTest, walksAnEdgeCreatedAboveTheCut) {
    StringRowSink sink;
    runWrite("CREATE (a:Person {name: 'Ana'})-[:KNOWS_WELL]->(b:Person {name: 'Bo'}) WITH a MATCH (a)-->(m) RETURN m.name",
             sink);

    const std::vector<StringRowSink::Row> expected {{"Bo"}};
    EXPECT_EQ(sink.getRows(), expected);
}

// A node the CREATE wrote no edge for has none to walk.
TEST_F(MatchCreatedEdgeTest, walksNothingFromACreatedNodeWithoutEdges) {
    StringRowSink sink;
    runWrite("CREATE (n:Person {name: 'Zed'}) WITH n MATCH (n)-->(m) RETURN m.name", sink);

    EXPECT_TRUE(sink.getRows().empty());
}

// The hop starts from a node the graph holds, so its rows mix the edges of the commit with
// the one this change wrote off the same node.
TEST_F(MatchCreatedEdgeTest, walksACreatedEdgeOffACommittedNode) {
    StringRowSink sink;
    runWrite("MATCH (p:Person {name: 'Remy'}) CREATE (p)-[:KNOWS_WELL]->(b:Person {name: 'Bo'}) WITH p MATCH (p)-->(m) RETURN m.name",
             sink);

    std::vector<StringRowSink::Row> rows;
    sink.sortedRows(rows);

    const std::vector<StringRowSink::Row> expected {{"Adam"}, {"Bo"}, {"Computers"}, {"Eighties"}, {"Ghosts"}};
    EXPECT_EQ(rows, expected);
}

// The edge type is the change's own: the graph's schema does not have it, so the hop
// resolves the name against what the change wrote and matches the pending edge with it.
TEST_F(MatchCreatedEdgeTest, walksACreatedEdgeOfATypeTheChangeIntroduced) {
    StringRowSink sink;
    runWrite("CREATE (a:Person {name: 'Ana'})-[:MENTORS]->(b:Person {name: 'Bo'}) WITH a MATCH (a)-[:MENTORS]->(m) RETURN m.name",
             sink);

    const std::vector<StringRowSink::Row> expected {{"Bo"}};
    EXPECT_EQ(sink.getRows(), expected);
}

// A type the pattern names and the change did not write matches none of its edges.
TEST_F(MatchCreatedEdgeTest, walksNoCreatedEdgeOfAnotherType) {
    StringRowSink sink;
    runWrite("CREATE (a:Person {name: 'Ana'})-[:KNOWS_WELL]->(b:Person {name: 'Bo'}) WITH a MATCH (a)-[:INTERESTED_IN]->(m) RETURN m.name",
             sink);

    EXPECT_TRUE(sink.getRows().empty());
}

// The hop walks the created edge from the node it lands on, against its direction.
TEST_F(MatchCreatedEdgeTest, walksACreatedEdgeBackwards) {
    StringRowSink sink;
    runWrite("CREATE (a:Person {name: 'Ana'})-[:KNOWS_WELL]->(b:Person {name: 'Bo'}) WITH b MATCH (b)<--(m) RETURN m.name",
             sink);

    const std::vector<StringRowSink::Row> expected {{"Ana"}};
    EXPECT_EQ(sink.getRows(), expected);
}

// An undirected hop walks it either way round.
TEST_F(MatchCreatedEdgeTest, walksACreatedEdgeInEitherDirection) {
    StringRowSink sink;
    runWrite("CREATE (a:Person {name: 'Ana'})-[:KNOWS_WELL]->(b:Person {name: 'Bo'}) WITH b MATCH (b)--(m) RETURN m.name",
             sink);

    const std::vector<StringRowSink::Row> expected {{"Ana"}};
    EXPECT_EQ(sink.getRows(), expected);
}

// The edge the hop bound is the change's own, so its property is read out of the write
// buffer, as the property of the node it landed on is.
TEST_F(MatchCreatedEdgeTest, readsThePropertiesOfWhatItWalked) {
    StringRowSink sink;
    runWrite("CREATE (a:Person {name: 'Ana'})-[:KNOWS_WELL {duration: 7}]->(b:Person {name: 'Bo'}) WITH a MATCH (a)-[e]->(m) RETURN e.duration, m.name",
             sink);

    const std::vector<StringRowSink::Row> expected {{"7", "Bo"}};
    EXPECT_EQ(sink.getRows(), expected);
}

// Two hops over what the change wrote: the second starts from a node the first one only
// reached through the write buffer.
TEST_F(MatchCreatedEdgeTest, walksTwoCreatedEdgesInARow) {
    StringRowSink sink;
    runWrite("CREATE (a:Person {name: 'Ana'})-[:KNOWS_WELL]->(b:Person {name: 'Bo'})-[:KNOWS_WELL]->(c:Person {name: 'Cy'}) WITH a MATCH (a)-->(x)-->(y) RETURN x.name, y.name",
             sink);

    const std::vector<StringRowSink::Row> expected {{"Bo", "Cy"}};
    EXPECT_EQ(sink.getRows(), expected);
}

// The commit holds what the hop walked, so the same pattern reads it off the graph
// afterwards.
TEST_F(MatchCreatedEdgeTest, commitsTheEdgeItWalked) {
    runWrite("CREATE (a:Person {name: 'Ana'})-[:KNOWS_WELL]->(b:Person {name: 'Bo'}) WITH a MATCH (a)-->(m) RETURN m.name");

    StringRowSink sink;
    runQuery("MATCH (a:Person {name: 'Ana'})-->(m) RETURN m.name", sink);

    const std::vector<StringRowSink::Row> expected {{"Bo"}};
    EXPECT_EQ(sink.getRows(), expected);
}

// More pending edges than one chunk holds, so the walk fills several: what the hop reports
// is what the commit holds. The count between the two cuts drains every step of the CREATE
// before the hop runs, so the hop walks all of what was written and not the slice one step
// of it wrote.
TEST_F(MatchCreatedEdgeTest, walksMoreCreatedEdgesThanAChunkHolds) {
    StringRowSink walked;
    runWrite("MATCH (a:Person {name: 'Remy'}), (b), (c), (d), (e) CREATE (a)-[:CHUNKY]->(b) WITH count(a) AS made MATCH (p:Person {name: 'Remy'})-[:CHUNKY]->(m) RETURN count(m)",
             walked);

    ASSERT_EQ(walked.getRows().size(), 1u);
    EXPECT_GT(std::stoull(walked.getRows().front().front()), ChunkConfig::CHUNK_SIZE);

    StringRowSink committed;
    runQuery("MATCH (a:Person {name: 'Remy'})-[:CHUNKY]->(m) RETURN count(m)", committed);
    EXPECT_EQ(walked.getRows(), committed.getRows());
}

// The node the hop landed on is one the change wrote, so the SET writes into the buffer it
// lives in and the read below it reads back what was set.
TEST_F(MatchCreatedEdgeTest, setsAPropertyOnANodeItWalked) {
    StringRowSink sink;
    runWrite("MATCH (p:Person {name: 'Remy'}) CREATE (p)-[:MENTORS]->(b:Person {name: 'Bo'}) WITH p MATCH (p)-[:MENTORS]->(m) SET m.name = 'Bea' RETURN m.name",
             sink);

    const std::vector<StringRowSink::Row> expected {{"Bea"}};
    EXPECT_EQ(sink.getRows(), expected);

    StringRowSink committed;
    runQuery("MATCH (n:Person {name: 'Bea'}) RETURN n.name", committed);

    const std::vector<StringRowSink::Row> written {{"Bea"}};
    EXPECT_EQ(committed.getRows(), written);
}

// The edge the second CREATE hangs off the node the hop landed on, which the graph holds
// nowhere: the commit resolves it to the node the first CREATE wrote.
TEST_F(MatchCreatedEdgeTest, createsAnEdgeOffANodeItWalked) {
    runWrite("MATCH (p:Person {name: 'Remy'}) CREATE (p)-[:KNOWS_WELL]->(b:Person {name: 'Bo'}) WITH p MATCH (p)-[:KNOWS_WELL]->(m) CREATE (m)-[:INTERESTED_IN]->(:Interest {name: 'Kites'})");

    StringRowSink sink;
    runQuery("MATCH (n:Person {name: 'Bo'})-[:INTERESTED_IN]->(i) RETURN i.name", sink);

    const std::vector<StringRowSink::Row> expected {{"Kites"}};
    EXPECT_EQ(sink.getRows(), expected);
}

// The edge is the change's own, so the DELETE drops it from the write buffer and the commit
// carries neither it nor a tombstone for it.
TEST_F(MatchCreatedEdgeTest, deletesAnEdgeItWalked) {
    runWrite("MATCH (p:Person {name: 'Remy'}) CREATE (p)-[:INTERESTED_IN]->(b:Interest {name: 'Kites'}) WITH p MATCH (p)-[e:INTERESTED_IN]->(m) DELETE e");

    StringRowSink edges;
    runQuery("MATCH (p:Person {name: 'Remy'})-[:INTERESTED_IN]->(m) RETURN m.name", edges);
    EXPECT_TRUE(edges.getRows().empty());

    StringRowSink kites;
    runQuery("MATCH (n:Interest {name: 'Kites'}) RETURN n.name", kites);

    const std::vector<StringRowSink::Row> expected {{"Kites"}};
    EXPECT_EQ(kites.getRows(), expected);
}

// A node the hop landed on holds the edge it was reached by, so deleting it without DETACH
// is refused - the relationship the change wrote counts as much as one the graph holds.
TEST_F(MatchCreatedEdgeTest, refusesToDeleteANodeItWalkedToWithoutDetaching) {
    runWriteExpectingError("MATCH (p:Person {name: 'Remy'}) CREATE (p)-[:KNOWS_WELL]->(b:Person {name: 'Bo'}) WITH p MATCH (p)-[:KNOWS_WELL]->(m) DELETE m",
                           "Cannot delete a node with relationships");
}

// The hop reads the change's tip the way a scan does: an edge an earlier statement of the
// change staged is walked once that write commits.
TEST_F(MatchCreatedEdgeTest, walksNoEdgeAnEarlierStatementStaged) {
    StringRowSink sink;
    runWritesInOneChange("MATCH (p:Person {name: 'Remy'}) CREATE (p)-[:MENTORS]->(:Person {name: 'Bo'})",
                         "MATCH (p:Person {name: 'Remy'})-[:MENTORS]->(m) RETURN m.name",
                         sink);

    EXPECT_TRUE(sink.getRows().empty());
}

// A label on the pattern's far node tests the label set the CREATE spelled: the graph holds
// none for a node it has not committed.
TEST_F(MatchCreatedEdgeTest, keepsACreatedNodeItWalkedToThatCarriesTheLabel) {
    StringRowSink sink;
    runWrite("CREATE (a:Person {name: 'Ana'})-[:KNOWS_WELL]->(b:Person {name: 'Bo'}) WITH a MATCH (a)-->(m:Person) RETURN m.name",
             sink);

    const std::vector<StringRowSink::Row> expected {{"Bo"}};
    EXPECT_EQ(sink.getRows(), expected);
}

// The label is one the change introduced, so the label set the test matches is in the
// change's schema and nowhere else.
TEST_F(MatchCreatedEdgeTest, keepsACreatedNodeUnderALabelTheChangeIntroduced) {
    StringRowSink sink;
    runWrite("CREATE (a:Person {name: 'Ana'})-[:KNOWS_WELL]->(b:Brand {name: 'Ora'}) WITH a MATCH (a)-->(m:Brand) RETURN m.name",
             sink);

    const std::vector<StringRowSink::Row> expected {{"Ora"}};
    EXPECT_EQ(sink.getRows(), expected);
}

TEST_F(MatchCreatedEdgeTest, dropsACreatedNodeItWalkedToThatLacksTheLabel) {
    StringRowSink sink;
    runWrite("CREATE (a:Person {name: 'Ana'})-[:KNOWS_WELL]->(b:Person {name: 'Bo'}) WITH a MATCH (a)-->(m:Interest) RETURN m.name",
             sink);

    EXPECT_TRUE(sink.getRows().empty());
}

// The same test written as a predicate rather than as part of the pattern, on the node and
// on the edge the hop bound.
TEST_F(MatchCreatedEdgeTest, testsTheLabelAndTheTypeOfWhatItWalkedInAWhere) {
    StringRowSink node;
    runWrite("CREATE (a:Person {name: 'Ana'})-[:MENTORS]->(b:Brand {name: 'Ora'}) WITH a MATCH (a)-[e]->(m) WHERE m:Brand RETURN m.name",
             node);

    const std::vector<StringRowSink::Row> expected {{"Ora"}};
    EXPECT_EQ(node.getRows(), expected);

    StringRowSink edge;
    runWrite("CREATE (a:Person {name: 'Ivo'})-[:MENTORS]->(b:Brand {name: 'Uma'}) WITH a MATCH (a)-[e]->(m) WHERE e:MENTORS RETURN m.name",
             edge);

    const std::vector<StringRowSink::Row> typed {{"Uma"}};
    EXPECT_EQ(edge.getRows(), typed);
}

// The second cut publishes the edge but not the column of types the hop filled, so the type
// is read again - off the write buffer, which is the only place a created edge has one.
TEST_F(MatchCreatedEdgeTest, readsTheTypeOfACreatedEdgeTwoCutsDown) {
    StringRowSink named;
    runWrite("CREATE (a:Person {name: 'Ivo'})-[:MENTORS]->(b:Brand {name: 'Uma'}) WITH a MATCH (a)-[e]->(m) WITH e, m RETURN type(e), m.name",
             named);

    const std::vector<StringRowSink::Row> expected {{"MENTORS", "Uma"}};
    EXPECT_EQ(named.getRows(), expected);

    StringRowSink tested;
    runWrite("CREATE (a:Person {name: 'Ada'})-[:MENTORS]->(b:Brand {name: 'Eos'}) WITH a MATCH (a)-[e]->(m) WITH e, m WHERE e:MENTORS RETURN m.name",
             tested);

    const std::vector<StringRowSink::Row> typed {{"Eos"}};
    EXPECT_EQ(tested.getRows(), typed);
}

// A type disjunction over the write buffer: the hop keeps a pending edge carrying any of
// the named types, so both created edges come back off the same created node.
TEST_F(MatchCreatedEdgeTest, walksCreatedEdgesOfEitherTypeInADisjunction) {
    StringRowSink sink;
    runWrite("CREATE (a:Person {name: 'Ana'})-[:KNOWS_WELL]->(b:Person {name: 'Bo'}) "
             "CREATE (a)-[:MENTORS]->(c:Person {name: 'Cy'}) "
             "WITH a MATCH (a)-[:KNOWS_WELL|MENTORS]->(m) RETURN m.name",
             sink);

    std::vector<StringRowSink::Row> rows;
    sink.sortedRows(rows);

    const std::vector<StringRowSink::Row> expected {{"Bo"}, {"Cy"}};
    EXPECT_EQ(rows, expected);
}

// None of the named types is the one the CREATE wrote, so the pending edge is turned away.
TEST_F(MatchCreatedEdgeTest, walksNoCreatedEdgeWhenTheDisjunctionNamesOtherTypes) {
    StringRowSink sink;
    runWrite("CREATE (a:Person {name: 'Ana'})-[:KNOWS_WELL]->(b:Person {name: 'Bo'}) "
             "WITH a MATCH (a)-[:MENTORS|INTERESTED_IN]->(m) RETURN m.name",
             sink);

    EXPECT_TRUE(sink.getRows().empty());
}

// Off a committed node, so the disjunction runs over both the graph's edges and the
// change's: it keeps Remy's committed KNOWS_WELL edge and the pending MENTORS one, and
// drops his three committed INTERESTED_IN edges.
TEST_F(MatchCreatedEdgeTest, disjunctionMixesCommittedAndCreatedEdges) {
    StringRowSink sink;
    runWrite("MATCH (p:Person {name: 'Remy'}) CREATE (p)-[:MENTORS]->(b:Person {name: 'Bo'}) "
             "WITH p MATCH (p)-[:KNOWS_WELL|MENTORS]->(m) RETURN m.name",
             sink);

    std::vector<StringRowSink::Row> rows;
    sink.sortedRows(rows);

    const std::vector<StringRowSink::Row> expected {{"Adam"}, {"Bo"}};
    EXPECT_EQ(rows, expected);
}

// The whole-graph scan sibling, which reads the write buffer through NLPendingEdgeScan
// rather than the hop: the KNOWS_WELL edges the graph holds plus the pending MENTORS one.
TEST_F(MatchCreatedEdgeTest, scanWalksCreatedEdgesOfEitherTypeInADisjunction) {
    StringRowSink sink;
    runWrite("CREATE (a:Person {name: 'Ana'})-[:MENTORS]->(b:Person {name: 'Bo'}) "
             "WITH a MATCH (x)-[:KNOWS_WELL|MENTORS]->(y) RETURN y.name",
             sink);

    std::vector<StringRowSink::Row> rows;
    sink.sortedRows(rows);

    const std::vector<StringRowSink::Row> expected {{"Adam"}, {"Bo"}, {"Remy"}, {"Remy"}};
    EXPECT_EQ(rows, expected);
}

#include <gtest/gtest.h>

#include <string>
#include <vector>

#include "CallV3Test.h"
#include "StringRowSink.h"

using namespace db;
using namespace turing::test;

class MatchCreatedEdgeByLabelTest : public CallV3Test {
};

// MATCH (a)-->(m:Person) below the cut walks an edge the CREATE above it wrote: the labels
// of the node it lands on are the ones the write buffer holds, since the graph has neither
// the node nor the edge until the commit.
TEST_F(MatchCreatedEdgeByLabelTest, walksACreatedEdgeArrivingAtTheLabels) {
    StringRowSink sink;
    runWrite("CREATE (a:Person {name: 'Ana'})-[:KNOWS_WELL]->(b:Person {name: 'Bo'}) WITH a MATCH (a)-->(m:Person) RETURN m.name",
             sink);

    const std::vector<StringRowSink::Row> expected {{"Bo"}};
    EXPECT_EQ(sink.getRows(), expected);
}

// Bo is a Person, so the hop asking for an Interest walks nothing.
TEST_F(MatchCreatedEdgeByLabelTest, walksNoCreatedEdgeArrivingAtOtherLabels) {
    StringRowSink sink;
    runWrite("CREATE (a:Person {name: 'Ana'})-[:KNOWS_WELL]->(b:Person {name: 'Bo'}) WITH a MATCH (a)-->(m:Interest) RETURN m.name",
             sink);

    EXPECT_TRUE(sink.getRows().empty());
}

// The label is the change's own: the graph's schema does not have it, so the hop resolves
// the name against what the change wrote and matches the pending node with it.
TEST_F(MatchCreatedEdgeByLabelTest, walksACreatedEdgeArrivingAtALabelTheChangeIntroduced) {
    StringRowSink sink;
    runWrite("CREATE (a:Person {name: 'Ana'})-[:KNOWS_WELL]->(b:Wizard {name: 'Bo'}) WITH a MATCH (a)-->(m:Wizard) RETURN m.name",
             sink);

    const std::vector<StringRowSink::Row> expected {{"Bo"}};
    EXPECT_EQ(sink.getRows(), expected);
}

// The in-hop reaches the edge's source, so that is the end its labels are read off.
TEST_F(MatchCreatedEdgeByLabelTest, walksACreatedEdgeBackwardsFromTheLabels) {
    StringRowSink sink;
    runWrite("CREATE (a:Person {name: 'Ana'})-[:KNOWS_WELL]->(b:Person {name: 'Bo'}) WITH b MATCH (b)<--(m:Person) RETURN m.name",
             sink);

    const std::vector<StringRowSink::Row> expected {{"Ana"}};
    EXPECT_EQ(sink.getRows(), expected);
}

// The hop starts from a node the graph holds, so its rows mix the commit's edges arriving
// at an Interest with the pending one, which arrives at a Person and is dropped.
TEST_F(MatchCreatedEdgeByLabelTest, walksACreatedEdgeBesideTheCommittedOnes) {
    StringRowSink sink;
    runWrite("MATCH (p:Person {name: 'Remy'}) CREATE (p)-[:KNOWS_WELL]->(b:Person {name: 'Bo'}) WITH p MATCH (p)-->(m:Interest) RETURN m.name",
             sink);

    std::vector<StringRowSink::Row> rows;
    sink.sortedRows(rows);

    const std::vector<StringRowSink::Row> expected {{"Computers"}, {"Eighties"}, {"Ghosts"}};
    EXPECT_EQ(rows, expected);
}

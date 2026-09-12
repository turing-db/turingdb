#include <gtest/gtest.h>

#include <string>
#include <vector>

#include "CallV3Test.h"
#include "StringRowSink.h"

using namespace db;
using namespace turing::test;

class CreateWithTest : public CallV3Test {
};

// CREATE (n:Person {name: 'Zoe'}) WITH n RETURN n.name: the WITH ends the part the CREATE
// wrote in, and the part below it reads the node out of the change rather than out of the
// graph, which does not hold it until the commit.
TEST_F(CreateWithTest, readsAPropertyOfANodeCreatedAboveTheCut) {
    StringRowSink sink;
    runWrite("CREATE (n:Person {name: 'Zoe'}) WITH n RETURN n.name", sink);

    const std::vector<StringRowSink::Row> expected {{"Zoe"}};
    EXPECT_EQ(sink.getRows(), expected);

    StringRowSink committed;
    runQuery("MATCH (n:Person {name: 'Zoe'}) RETURN n.name", committed);
    EXPECT_EQ(committed.getRows(), expected);
}

// The WHERE of the WITH reads the node the CREATE wrote, one part below it.
TEST_F(CreateWithTest, filtersTheRowsACreateWroteOnTheCutsWhere) {
    StringRowSink kept;
    runWrite("CREATE (n:Person {name: 'Yan'}) WITH n WHERE n.name = 'Yan' RETURN n.name", kept);

    const std::vector<StringRowSink::Row> expected {{"Yan"}};
    EXPECT_EQ(kept.getRows(), expected);

    StringRowSink dropped;
    runWrite("CREATE (n:Person {name: 'Wu'}) WITH n WHERE n.name = 'Yan' RETURN n.name", dropped);
    EXPECT_TRUE(dropped.getRows().empty());
}

// One node per Person, ordered and cut by the WITH: the read below the cut names each node
// by a row of a column the sort and the limit have rewritten.
TEST_F(CreateWithTest, readsTheRowsThatSurviveTheCutsOrderAndLimit) {
    StringRowSink sink;
    runWrite("MATCH (p:Person) CREATE (n:Person {name: p.name}) WITH n ORDER BY n.name LIMIT 2 RETURN n.name",
             sink);

    const std::vector<StringRowSink::Row> expected {{"Adam"}, {"Cyrus"}};
    EXPECT_EQ(sink.getRows(), expected);
}

// labels(n) and n:Person read the labels the CREATE spelled: the graph holds no label set
// for a node it has not committed.
TEST_F(CreateWithTest, readsTheLabelsACreateWroteBelowTheCut) {
    StringRowSink labels;
    runWrite("CREATE (n:Person {name: 'Uma'}) WITH n RETURN labels(n)", labels);

    const std::vector<StringRowSink::Row> expected {{"Person"}};
    EXPECT_EQ(labels.getRows(), expected);

    StringRowSink matching;
    runWrite("CREATE (n:Person {name: 'Vic'}) WITH n WHERE n:Person RETURN n.name", matching);
    const std::vector<StringRowSink::Row> matched {{"Vic"}};
    EXPECT_EQ(matching.getRows(), matched);

    StringRowSink other;
    runWrite("CREATE (n:Person {name: 'Tim'}) WITH n WHERE n:Interest RETURN n.name", other);
    EXPECT_TRUE(other.getRows().empty());
}

// The WITH renames the node, and the part below it reads the property through the name that
// part knows it by.
TEST_F(CreateWithTest, readsACreatedNodeUnderTheNameTheCutPublishesIt) {
    StringRowSink sink;
    runWrite("CREATE (n:Person {name: 'Sia'}) WITH n AS m RETURN m.name, labels(m)", sink);

    const std::vector<StringRowSink::Row> expected {{"Sia", "Person"}};
    EXPECT_EQ(sink.getRows(), expected);
}

// SET below the cut updates the node the CREATE wrote, in the write buffer it still lives in.
TEST_F(CreateWithTest, setsAPropertyOnANodeCreatedAboveTheCut) {
    runWrite("CREATE (n:Person {name: 'Sam'}) WITH n SET n.age = 33");

    StringRowSink sink;
    runQuery("MATCH (n:Person {name: 'Sam'}) RETURN n.name, n.age", sink);

    const std::vector<StringRowSink::Row> expected {{"Sam", "33"}};
    EXPECT_EQ(sink.getRows(), expected);
}

// The edge hangs off a node the part above the cut wrote, so its source is a write-buffer
// entry rather than an ID the graph holds.
TEST_F(CreateWithTest, createsAnEdgeFromANodeCreatedAboveTheCut) {
    runWrite("CREATE (a:Person {name: 'Ana'}) WITH a CREATE (a)-[:KNOWS]->(b:Person {name: 'Bo'})");

    StringRowSink sink;
    runQuery("MATCH (a:Person {name: 'Ana'})-[:KNOWS]->(b) RETURN b.name", sink);

    const std::vector<StringRowSink::Row> expected {{"Bo"}};
    EXPECT_EQ(sink.getRows(), expected);
}

// The delete drops the node from the write buffer instead of tombstoning an ID the graph
// never held, so the commit carries neither the node nor a tombstone for it.
TEST_F(CreateWithTest, deletesANodeCreatedAboveTheCut) {
    runWrite("CREATE (d:Person {name: 'Dee'}) WITH d DELETE d");

    StringRowSink sink;
    runQuery("MATCH (n:Person {name: 'Dee'}) RETURN n.name", sink);
    EXPECT_TRUE(sink.getRows().empty());

    StringRowSink people;
    runQuery("MATCH (n:Person) RETURN count(n)", people);

    const std::vector<StringRowSink::Row> expected {{"8"}};
    EXPECT_EQ(people.getRows(), expected);
}

// Two cuts, and a CREATE in the part between them: each part reads what the one above it
// published.
TEST_F(CreateWithTest, carriesWhatEachPartWroteToTheNextOne) {
    StringRowSink sink;
    runWrite("CREATE (a:Person {name: 'Ivo'}) WITH a CREATE (b:Person {name: 'Jem'}) WITH a, b RETURN a.name, b.name",
             sink);

    const std::vector<StringRowSink::Row> expected {{"Ivo", "Jem"}};
    EXPECT_EQ(sink.getRows(), expected);
}

// The WITH aggregates over the rows the CREATE wrote, one per Person the MATCH found.
TEST_F(CreateWithTest, aggregatesTheRowsACreateWrote) {
    StringRowSink sink;
    runWrite("MATCH (p:Person) CREATE (n:Person {name: p.name}) WITH count(n) AS written RETURN written", sink);

    const std::vector<StringRowSink::Row> expected {{"8"}};
    EXPECT_EQ(sink.getRows(), expected);
}

// A property the CREATE did not write is null, read below the cut out of the write buffer
// exactly as it is read above it off the columns the CREATE wrote.
TEST_F(CreateWithTest, readsNullForAPropertyTheCreateDidNotWrite) {
    StringRowSink sink;
    runWrite("CREATE (n:Person {name: 'Kai'}) WITH n RETURN n.name, n.age", sink);

    const std::vector<StringRowSink::Row> expected {{"Kai", "null"}};
    EXPECT_EQ(sink.getRows(), expected);
}

// A MERGE's rows mix what it wrote with what it bound, and only the mask beside them tells
// the two apart. The cut has no item to carry that mask on, so the entity cannot cross it.
TEST_F(CreateWithTest, rejectsAWithThatPublishesAMergedEntity) {
    runWriteExpectingError("MERGE (n:Person {name: 'Nia'}) WITH n RETURN n.name",
                           "A WITH cannot publish 'n'");

    StringRowSink sink;
    runWrite("MERGE (n:Person {name: 'Nia'}) WITH n.name AS written RETURN written", sink);

    const std::vector<StringRowSink::Row> expected {{"Nia"}};
    EXPECT_EQ(sink.getRows(), expected);
}

// A query part reads then writes. `CREATE (n) MATCH (m)` is two parts with the cut between
// them left out, not one part with its clauses out of order.
TEST_F(CreateWithTest, rejectsAReadingClauseAfterAnUpdatingOne) {
    runWriteExpectingError("CREATE (n:Person {name: 'Qi'}) MATCH (m:Person) RETURN m.name",
                           "A reading clause cannot follow an updating clause");
}

// An optional pattern pads the rows it misses with an invalid ID, which names no entry of
// the write buffer the created node lives in.
TEST_F(CreateWithTest, rejectsAnOptionalMatchOverANodeCreatedAboveTheCut) {
    runWriteExpectingError("CREATE (n:Person {name: 'Ola'}) WITH n OPTIONAL MATCH (n)-[:KNOWS]->(m) RETURN m.name",
                           "An OPTIONAL MATCH cannot read what a CREATE in the same query wrote");
}

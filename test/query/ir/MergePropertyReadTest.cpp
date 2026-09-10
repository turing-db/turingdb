#include <gtest/gtest.h>

#include "WriteQueryTest.h"

using namespace db;
using namespace turing::test;

// Where a merge's rows read a property: off the graph for a row it bound, out of the
// change's write buffer for a row it wrote.
class MergePropertyReadTest : public WriteQueryTest {
};

// The pattern keys the tag on the value the match produced and two tags carry it, so the
// merge binds both: the property is read off the two rows it emitted, not off the one row
// that drove it
TEST_F(MergePropertyReadTest, readsTheKeyPropertyOfEveryRowAMergeBound) {
    expectWriteRowCount("CREATE (t:Tag {name: 'Remy'})", 0);
    expectWriteRowCount("CREATE (t:Tag {name: 'Remy'})", 0);

    expectWriteRows("MATCH (p:Person {name: 'Remy'}) MERGE (t:Tag {name: p.name}) RETURN t.name",
                    {{"Remy"}, {"Remy"}});
}

// The schema holds score as a double, so the integer the pattern asks for binds the node
// carrying 2.0 - and the projection reads that node's own value, not the 2 it asked for
TEST_F(MergePropertyReadTest, readsTheBoundValueRatherThanTheOneThePatternAsksFor) {
    expectWriteRowCount("CREATE (s:Score {score: 2.0})", 0);

    expectWriteRows("MERGE (s:Score {score: 2}) RETURN s.score", {{"2.000000"}});
}

// ON CREATE writes into the change, and the projection behind it reads the value there
TEST_F(MergePropertyReadTest, readsThePropertyOnCreateSetOnARowItWrote) {
    expectWriteRows("MERGE (n:Tag {name: 'x'}) ON CREATE SET n.age = 1 RETURN n.age", {{"1"}});
}

// ON CREATE leaves a bound row alone, so that row reads the value the graph holds
TEST_F(MergePropertyReadTest, readsTheGraphValueForARowOnCreateLeftAlone) {
    expectWriteRows("MERGE (p:Person {name: 'Remy'}) ON CREATE SET p.age = 1 RETURN p.age", {{"32"}});
}

// A plain SET touches every row, whichever way the merge bound it
TEST_F(MergePropertyReadTest, readsThePropertyAPlainSetWroteOnAWrittenRow) {
    expectWriteRows("MERGE (n:Tag {name: 'x'}) SET n.age = 7 RETURN n.age", {{"7"}});
}

// A property no write set is absent from the entity the merge wrote
TEST_F(MergePropertyReadTest, readsNullForAPropertyTheWriteDidNotSet) {
    expectWriteRows("MERGE (n:Tag {name: 'x'}) RETURN n.age", {{"null"}});
}

// The labels of a row the merge wrote are the ones its pattern named, read out of the
// change: the graph holds nothing at the provisional ID that row carries
TEST_F(MergePropertyReadTest, readsTheLabelsOfARowItWrote) {
    expectWriteRows("MERGE (n:Tag {name: 'x'}) RETURN labels(n)", {{"Tag"}});
}

// A row it bound reads the labels the graph holds for that node, which are the node's own
// and not the one label the pattern named
TEST_F(MergePropertyReadTest, readsTheGraphLabelsOfARowItBound) {
    expectWriteRows("MERGE (p:Person {name: 'Remy'}) RETURN labels(p)",
                    {{"Person, SoftwareEngineering, Founder"}});
}

// One merge, both kinds of row: Computers is an Interest the graph holds and Nope is one
// the merge writes, so each row answers from where its own entity lives
TEST_F(MergePropertyReadTest, readsTheLabelsOfTheBoundAndWrittenRowsOfOneMerge) {
    expectWriteRows("UNWIND ['Computers', 'Nope'] AS name MERGE (i:Interest {name: name}) RETURN labels(i)",
                    {{"Interest"}, {"SoftwareEngineering, Interest"}});
}

// The type of a hop the merge wrote is the one its pattern named
TEST_F(MergePropertyReadTest, readsTheTypeOfAHopItWrote) {
    expectWriteRows("MATCH (a:Person {name: 'Remy'}), (b:Person {name: 'Luc'}) "
                    "MERGE (a)-[e:KNOWS_WELL {duration: 5}]->(b) "
                    "RETURN type(e)",
                    {{"KNOWS_WELL"}});
}

// The hop sibling: an edge the merge wrote reads the value its own pattern carried
TEST_F(MergePropertyReadTest, readsThePropertyOfAHopItWrote) {
    expectWriteRows("MATCH (a:Person {name: 'Remy'}), (b:Person {name: 'Luc'}) "
                    "MERGE (a)-[e:KNOWS_WELL {duration: 5}]->(b) "
                    "RETURN e.duration",
                    {{"5"}});
}

int main(int argc, char** argv) {
    return turing::test::turingTestMain(argc, argv);
}

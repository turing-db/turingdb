#include <gtest/gtest.h>

#include <string>
#include <string_view>

#include "QueryInterpreterV3.h"
#include "QueryStatus.h"

#include "TuringDB.h"
#include "versioning/ChangeID.h"
#include "versioning/CommitHash.h"

#include "WriteQueryTest.h"

using namespace db;
using namespace turing::test;

// The MERGE patterns the engine turns away, and what it says about them.
class MergeRejectionTest : public WriteQueryTest {
protected:
    // Runs a writing query in its own change, leaving the change open: a rejected query
    // writes nothing, so nothing needs submitting
    void runQuery(std::string_view query, QueryStatus& status) {
        ChangeID changeID;
        openChange(changeID);

        NullSink sink;
        _interpreter->execute(status,
                              query,
                              _graphName,
                              CommitHash::head(),
                              changeID,
                              &sink);
    }
};

// Every node carries at least one label, so a pattern that would have to write a
// label-less one names something the graph cannot hold
TEST_F(MergeRejectionTest, rejectsANodePatternWithoutALabel) {
    QueryStatus status;
    runQuery("MERGE (n {name: 'x'})", status);

    EXPECT_FALSE(status.isOk());
    EXPECT_NE(status.getError().find("Node pattern must have at least one label"), std::string::npos)
        << status.getError();
}

// Every edge carries exactly one type, so a hop that would have to write an untyped one
// is turned away
TEST_F(MergeRejectionTest, rejectsAHopWithoutAnEdgeType) {
    QueryStatus status;
    runQuery("MATCH (a:Person), (b:Person) MERGE (a)-[e]->(b)", status);

    EXPECT_FALSE(status.isOk());
    EXPECT_NE(status.getError().find("Edge pattern must have at least one edge type"), std::string::npos)
        << status.getError();
}

// MERGE writes the pattern it does not find, and a variable-length hop names no one
// path to write: Cypher has no such write pattern, so it is turned away rather than
// silently written as a single hop
TEST_F(MergeRejectionTest, rejectsAVariableLengthHop) {
    QueryStatus status;
    runQuery("MATCH (a:Person), (b:Person) MERGE (a)-[:KNOWS_WELL]->{1,3}(b)", status);

    EXPECT_FALSE(status.isOk()) << status.getError();
    EXPECT_NE(status.getError().find("Variable length relationships cannot be used in a write pattern"),
              std::string::npos)
        << status.getError();
}

// A node an OPTIONAL MATCH did not match is null, and a merge cannot hang an edge off a
// node the graph does not hold: the six rows the pattern missed turn the query away where
// it is written, the way a CREATE of the same edge is
TEST_F(MergeRejectionTest, rejectsAnEdgeFromANodeAnOptionalMatchMissed) {
    QueryStatus status;
    runQuery("MATCH (p:Person) OPTIONAL MATCH (p)-[:KNOWS_WELL]->(f) "
             "MERGE (f)-[:PROBE]->(t:Tag {name: 'x'})",
             status);

    EXPECT_FALSE(status.isOk()) << status.getError();
    EXPECT_NE(status.getError().find("Cannot merge a pattern on a null node"), std::string::npos)
        << status.getError();
}

// The same null node standing on its own: a merge of a bound node alone has nothing to
// match and nothing it may write, so it is turned away rather than merged as a node with
// an invalid ID
TEST_F(MergeRejectionTest, rejectsAPatternOnANodeAnOptionalMatchMissed) {
    QueryStatus status;
    runQuery("MATCH (p:Person) OPTIONAL MATCH (p)-[:KNOWS_WELL]->(f) MERGE (f)", status);

    EXPECT_FALSE(status.isOk()) << status.getError();
    EXPECT_NE(status.getError().find("Cannot merge a pattern on a null node"), std::string::npos)
        << status.getError();
}

// A null matches nothing, so a merge keyed on one would write a node on every run. The
// analyzer turns away a key that is null by type.
TEST_F(MergeRejectionTest, rejectsANodeKeyThatIsNullByType) {
    QueryStatus status;
    runQuery("MERGE (t:Tag {name: 'k', flag: 1 < 'a'})", status);

    EXPECT_FALSE(status.isOk()) << status.getError();
    EXPECT_NE(status.getError().find("MERGE cannot match on property 'flag', which is null"), std::string::npos)
        << status.getError();
}

TEST_F(MergeRejectionTest, rejectsANodeKeyThatIsNullOnOneRow) {
    QueryStatus status;
    runQuery("UNWIND [null, 'a'] AS v MERGE (t:Tag {name: v}) RETURN t.name", status);

    EXPECT_FALSE(status.isOk()) << status.getError();
    EXPECT_NE(status.getError().find("MERGE cannot match on property 'name', which is null"), std::string::npos)
        << status.getError();
}

TEST_F(MergeRejectionTest, rejectsANodeKeyThatIsNullOnTheLastRow) {
    QueryStatus status;
    runQuery("UNWIND ['a', 'b', null] AS v MERGE (t:Tag {name: v})", status);

    EXPECT_FALSE(status.isOk()) << status.getError();
    EXPECT_NE(status.getError().find("MERGE cannot match on property 'name', which is null"), std::string::npos)
        << status.getError();
}

TEST_F(MergeRejectionTest, rejectsANodeKeyACaseMakesNullOnOneRow) {
    QueryStatus status;
    runQuery("MATCH (p:Person) "
             "MERGE (t:Tag {name: CASE WHEN p.name = 'Remy' THEN null ELSE p.name END})",
             status);

    EXPECT_FALSE(status.isOk()) << status.getError();
    EXPECT_NE(status.getError().find("MERGE cannot match on property 'name', which is null"), std::string::npos)
        << status.getError();
}

// Remy and Adam have an age, the other Person nodes do not
TEST_F(MergeRejectionTest, rejectsANodeKeyReadFromAMissingProperty) {
    QueryStatus status;
    runQuery("MATCH (p:Person) MERGE (t:Tag {name: 'x', age: p.age})", status);

    EXPECT_FALSE(status.isOk()) << status.getError();
    EXPECT_NE(status.getError().find("MERGE cannot match on property 'age', which is null"), std::string::npos)
        << status.getError();
}

TEST_F(MergeRejectionTest, rejectsAnEdgeKeyThatIsNullOnOneRow) {
    QueryStatus status;
    runQuery("MATCH (a:Person {name: 'Remy'}), (b:Person {name: 'Adam'}) "
             "UNWIND [1, null] AS w MERGE (a)-[:PROBE {weight: w}]->(b)",
             status);

    EXPECT_FALSE(status.isOk()) << status.getError();
    EXPECT_NE(status.getError().find("MERGE cannot match on property 'weight', which is null"), std::string::npos)
        << status.getError();
}

int main(int argc, char** argv) {
    return turing::test::turingTestMain(argc, argv);
}

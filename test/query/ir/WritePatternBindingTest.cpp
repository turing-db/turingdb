#include <gtest/gtest.h>

#include "QueryStatus.h"
#include "WriteQueryTest.h"

using namespace db;
using namespace turing::test;

// A CREATE or MERGE pattern names a bound variable bare, and its property maps read only
// variables an earlier clause bound: what the clause itself introduces has no value yet.
class WritePatternBindingTest : public WriteQueryTest {
protected:
    void expectWriteError(std::string_view query, std::string_view error) {
        ChangeID changeID;
        openChange(changeID);

        const QueryStatus status = runWrite(query, changeID);
        ASSERT_FALSE(status.isOk());
        EXPECT_NE(status.getError().find(error), std::string::npos) << status.getError();
    }
};

TEST_F(WritePatternBindingTest, createRejectsPropertiesOnABoundNode) {
    expectWriteError("MATCH (a:Person {name: 'Remy'}) CREATE (a {k: 1})", "already defined");
}

TEST_F(WritePatternBindingTest, createRejectsPropertiesOnANodeAnEarlierCreateMade) {
    expectWriteError("CREATE (a:X {k: 1}) CREATE (a {m: 2})", "already defined");
}

TEST_F(WritePatternBindingTest, mergeRejectsPropertiesOnABoundNode) {
    expectWriteError("MATCH (a:Person {name: 'Remy'}) MERGE (a {name: 'Remy'})-[:R]->(b:X)", "already defined");
}

TEST_F(WritePatternBindingTest, createRejectsAPropertyReadingItsOwnNode) {
    expectWriteError("CREATE (a:X {name: a.name})", "which the same CREATE introduces");
}

TEST_F(WritePatternBindingTest, createRejectsAPropertyReadingAnotherNodeItCreates) {
    expectWriteError("CREATE (a:X {name: 'x'}), (b:X {name: a.name})", "which the same CREATE introduces");
}

TEST_F(WritePatternBindingTest, createRejectsANodeReadingAnEdgeOfItsPattern) {
    expectWriteError("CREATE (a:X)-[r:R {w: 1}]->(b:X {w: r.w})", "which the same CREATE introduces");
}

TEST_F(WritePatternBindingTest, mergeRejectsAPropertyReadingANodeItIntroduces) {
    expectWriteError("MERGE (a:X {name: 'q'})-[:R]->(b:X {name: a.name})", "which the same MERGE introduces");
}

TEST_F(WritePatternBindingTest, createRejectsALabelTestOnItsOwnNode) {
    expectWriteError("CREATE (a:X {j: a:X})", "which the same CREATE introduces");
}

TEST_F(WritePatternBindingTest, createRejectsATypeTestOnItsOwnEdge) {
    expectWriteError("CREATE (a:X)-[r:R {j: r:R}]->(b:X)", "which the same CREATE introduces");
}

TEST_F(WritePatternBindingTest, createRejectsAnExistsReadingANodeItCreates) {
    expectWriteError("CREATE (a:X {k: 1}), (b:X {j: EXISTS { (a)-->() }})", "which the same CREATE introduces");
}

TEST_F(WritePatternBindingTest, mergeRejectsAPatternComprehensionReadingANodeItIntroduces) {
    expectWriteError("MERGE (a:X {k: 1})-[:R]->(b:X {j: [(a)-->(c) | c.k]})", "which the same MERGE introduces");
}

TEST_F(WritePatternBindingTest, mergeRejectsARepeatedRelationshipVariable) {
    expectWriteError("MERGE (a:X)-[e:R]->(b:X)-[e:R]->(c:X)", "appears twice in this pattern");
}

TEST_F(WritePatternBindingTest, createReadsAnExistsOverABoundNode) {
    expectWriteRows("MATCH (x:Person {name: 'Remy'}) CREATE (b:X {j: EXISTS { (x)-->() }}) RETURN b.j", {{"true"}});
}

TEST_F(WritePatternBindingTest, createReadsALabelTestOnABoundNode) {
    expectWriteRows("MATCH (x:Person {name: 'Remy'}) CREATE (b:X {j: x:Founder}) RETURN b.j", {{"true"}});
}

TEST_F(WritePatternBindingTest, createReadsANodeAnEarlierCreateMade) {
    expectWriteRows("CREATE (a:X {name: 'x'}) CREATE (b:X {name: a.name}) RETURN b.name", {{"x"}});
}

TEST_F(WritePatternBindingTest, createReadsABoundNode) {
    expectWriteRows("MATCH (a:Person {name: 'Remy'}) CREATE (b:X {name: a.name}) RETURN b.name", {{"Remy"}});
}

TEST_F(WritePatternBindingTest, mergeReadsABoundNode) {
    expectWriteRows("MATCH (a:Person {name: 'Remy'}) MERGE (a)-[:R]->(b:X {name: a.name}) RETURN b.name", {{"Remy"}});
}

int main(int argc, char** argv) {
    return turing::test::turingTestMain(argc, argv);
}

#include <gtest/gtest.h>

#include <string_view>

#include "QueryStatus.h"

#include "versioning/ChangeID.h"

#include "WriteQueryTest.h"

using namespace db;
using namespace turing::test;

// A MERGE behind an earlier statement of the same change that wrote what its pattern asks
// for, with no COMMIT between the two. The graph holds nothing the change staged, and the
// MERGE has to match it there rather than write a second copy.
//
// simpledb holds no Tag node and no MENTORS edge.
class MergeAfterEarlierStatementTest : public WriteQueryTest {
protected:
    void stageWrite(std::string_view query, const ChangeID& changeID) {
        const QueryStatus status = runWrite(query, changeID);
        ASSERT_TRUE(status.isOk()) << "query: " << query << "\nerror: " << status.getError();
    }
};

TEST_F(MergeAfterEarlierStatementTest, matchesTheNodeAnEarlierMergeWrote) {
    ChangeID changeID;
    openChange(changeID);

    stageWrite("MERGE (t:Tag {name: 'x'})", changeID);
    stageWrite("MERGE (t:Tag {name: 'x'})", changeID);
    submit(changeID);

    expectRows("MATCH (t:Tag {name: 'x'}) RETURN count(t)", {{"1"}});
}

TEST_F(MergeAfterEarlierStatementTest, matchesTheNodeAnEarlierCreateWrote) {
    ChangeID changeID;
    openChange(changeID);

    stageWrite("CREATE (t:Tag {name: 'x'})", changeID);
    stageWrite("MERGE (t:Tag {name: 'x'})", changeID);
    submit(changeID);

    expectRows("MATCH (t:Tag) RETURN count(t)", {{"1"}});
}

TEST_F(MergeAfterEarlierStatementTest, createsANodeForAnotherValue) {
    ChangeID changeID;
    openChange(changeID);

    stageWrite("MERGE (t:Tag {name: 'x'})", changeID);
    stageWrite("MERGE (t:Tag {name: 'y'})", changeID);
    submit(changeID);

    expectRows("MATCH (t:Tag) RETURN t.name", {{"x"}, {"y"}});
}

TEST_F(MergeAfterEarlierStatementTest, createsANodeInPlaceOfOneAnEarlierStatementDeleted) {
    ChangeID changeID;
    openChange(changeID);

    stageWrite("CREATE (t:Tag {name: 'x'}) WITH t DELETE t", changeID);
    stageWrite("MERGE (t:Tag {name: 'x'})", changeID);
    submit(changeID);

    expectRows("MATCH (t:Tag) RETURN count(t)", {{"1"}});
}

TEST_F(MergeAfterEarlierStatementTest, updatesTheNodeItMatched) {
    ChangeID changeID;
    openChange(changeID);

    stageWrite("MERGE (t:Tag {name: 'x'}) ON CREATE SET t.age = 1", changeID);
    stageWrite("MERGE (t:Tag {name: 'x'}) ON MATCH SET t.age = 2", changeID);
    submit(changeID);

    expectRows("MATCH (t:Tag) RETURN t.name, t.age", {{"x", "2"}});
}

TEST_F(MergeAfterEarlierStatementTest, matchesTheEdgeAnEarlierMergeWrote) {
    ChangeID changeID;
    openChange(changeID);

    const std::string_view merge = "MATCH (a:Person {name: 'Remy'}), (b:Person {name: 'Luc'}) MERGE (a)-[:MENTORS]->(b)";
    stageWrite(merge, changeID);
    stageWrite(merge, changeID);
    submit(changeID);

    expectRows("MATCH ()-[m:MENTORS]->() RETURN count(m)", {{"1"}});
}

TEST_F(MergeAfterEarlierStatementTest, matchesThePathAnEarlierMergeWrote) {
    ChangeID changeID;
    openChange(changeID);

    stageWrite("MERGE (a:Tag {name: 'a'})-[:TAGS]->(b:Tag {name: 'b'})", changeID);
    stageWrite("MERGE (y:Tag {name: 'b'})<-[:TAGS]-(x:Tag {name: 'a'})", changeID);
    submit(changeID);

    expectRows("MATCH (t:Tag) RETURN count(t)", {{"2"}});
    expectRows("MATCH ()-[e:TAGS]->() RETURN count(e)", {{"1"}});
}

int main(int argc, char** argv) {
    return turing::test::turingTestMain(argc, argv);
}

#include <gtest/gtest.h>

#include <algorithm>
#include <string>
#include <string_view>

#include "QueryInterpreterV3.h"
#include "QueryStatus.h"

#include "versioning/ChangeID.h"
#include "versioning/CommitHash.h"

#include "WriteQueryTest.h"

using namespace db;
using namespace turing::test;

// A read behind a DELETE of a node or an edge the graph already holds. The graph keeps the
// entity until the commit, and the read has to skip it as it skips one a commit deleted.
//
// simpledb holds 18 nodes, 8 of them Person nodes. Remy is node 0, Remy and Adam carry an
// age of 32, and the three KNOWS_WELL edges are Remy -> Adam, Adam -> Remy and
// Ghosts -> Remy.
class MatchAfterDeleteTest : public WriteQueryTest {
protected:
    void stageWrite(std::string_view query, const ChangeID& changeID) {
        const QueryStatus status = runWrite(query, changeID);
        ASSERT_TRUE(status.isOk()) << "query: " << query << "\nerror: " << status.getError();
    }

    void expectChangeRows(std::string_view query, const ChangeID& changeID, const Rows& expected) {
        RowSink sink;
        QueryStatus status;
        _interpreter->execute(status,
                              query,
                              _graphName,
                              CommitHash::head(),
                              changeID,
                              &_env->getMem(),
                              &sink);
        ASSERT_TRUE(status.isOk()) << "query: " << query << "\nerror: " << status.getError();

        Rows actual;
        sink.sortedRows(actual);

        Rows sortedExpected = expected;
        std::sort(sortedExpected.begin(), sortedExpected.end());

        std::string actualText;
        describeRows(actual, actualText);

        EXPECT_EQ(actual, sortedExpected) << "query: " << query << "\ngot:\n" << actualText;
    }
};

TEST_F(MatchAfterDeleteTest, doesNotScanTheDeletedNodeByLabel) {
    expectWriteRows("MATCH (p:Person {name: 'Remy'}) DETACH DELETE p "
                    "WITH count(*) AS deleted MATCH (q:Person) RETURN count(q)",
                    {{"7"}});
}

TEST_F(MatchAfterDeleteTest, doesNotScanTheDeletedNode) {
    expectWriteRows("MATCH (p:Person {name: 'Remy'}) DETACH DELETE p "
                    "WITH count(*) AS deleted MATCH (n) RETURN count(n)",
                    {{"17"}});
}

TEST_F(MatchAfterDeleteTest, doesNotScanTheDeletedNodeByPropertyValue) {
    expectWriteRows("MATCH (p:Person {name: 'Remy'}) DETACH DELETE p "
                    "WITH count(*) AS deleted MATCH (q:Person {age: 32}) RETURN q.name",
                    {{"Adam"}});
}

TEST_F(MatchAfterDeleteTest, doesNotScanTheDeletedNodeByID) {
    expectWriteRows("MATCH (p:Person {name: 'Remy'}) DETACH DELETE p "
                    "WITH count(*) AS deleted MATCH (n) WHERE id(n) = 0 RETURN n.name",
                    {});
}

TEST_F(MatchAfterDeleteTest, doesNotWalkTheEdgesOfTheDetachedNode) {
    expectWriteRows("MATCH (p:Person {name: 'Remy'}) DETACH DELETE p "
                    "WITH count(*) AS deleted MATCH (:Person {name: 'Adam'})-[]-(b) RETURN b.name",
                    {{"Bio"}, {"Cooking"}});
}

TEST_F(MatchAfterDeleteTest, doesNotScanTheEdgesOfTheDetachedNode) {
    expectWriteRows("MATCH (p:Person {name: 'Remy'}) DETACH DELETE p "
                    "WITH count(*) AS deleted MATCH (a:Person)-[f:KNOWS_WELL]->(b) RETURN f.name",
                    {});
}

TEST_F(MatchAfterDeleteTest, doesNotWalkTheDeletedEdge) {
    expectWriteRows("MATCH (:Person {name: 'Remy'})-[e:KNOWS_WELL]->() DELETE e "
                    "WITH count(*) AS deleted MATCH (a)-[f:KNOWS_WELL]->(b) RETURN f.name",
                    {{"Adam -> Remy"}, {"Ghosts -> Remy"}});
}

TEST_F(MatchAfterDeleteTest, doesNotScanTheDeletedEdge) {
    expectWriteRows("MATCH (:Person {name: 'Remy'})-[e:KNOWS_WELL]->() DELETE e "
                    "WITH count(*) AS deleted MATCH ()-[f:KNOWS_WELL]->() RETURN count(f)",
                    {{"2"}});
}

TEST_F(MatchAfterDeleteTest, mergesANewNodeInPlaceOfTheDeletedOne) {
    expectWriteRows("MATCH (p:Person {name: 'Remy'}) DETACH DELETE p "
                    "WITH count(*) AS deleted MERGE (q:Person {name: 'Remy'}) RETURN q.name",
                    {{"Remy"}});

    expectRows("MATCH (q:Person) RETURN count(q)", {{"8"}});
}

TEST_F(MatchAfterDeleteTest, deletesEveryNodeItsOwnScanFound) {
    expectWriteRows("MATCH (p:Person) DETACH DELETE p RETURN count(*)", {{"8"}});

    expectRows("MATCH (q:Person) RETURN count(q)", {{"0"}});
}

TEST_F(MatchAfterDeleteTest, doesNotScanTheNodeAnEarlierQueryOfTheChangeDeleted) {
    ChangeID changeID;
    openChange(changeID);

    stageWrite("MATCH (p:Person {name: 'Remy'}) DETACH DELETE p", changeID);

    expectChangeRows("MATCH (q:Person) RETURN count(q)", changeID, {{"7"}});
    expectChangeRows("MATCH (a)-[f:KNOWS_WELL]->(b) RETURN f.name", changeID, {});
}

int main(int argc, char** argv) {
    return turing::test::turingTestMain(argc, argv);
}

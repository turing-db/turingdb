#include <gtest/gtest.h>

#include <string_view>

#include "QueryInterpreterV3.h"
#include "QueryStatus.h"

#include "versioning/ChangeID.h"
#include "versioning/CommitHash.h"

#include "WriteQueryTest.h"

using namespace db;
using namespace turing::test;

// DELETE beside the writes of the same query: of an entity the change holds as a pending
// write rather than as a committed ID, and of one the query has given a relationship to.
class DeleteWrittenTest : public WriteQueryTest {
protected:
    void runQuery(std::string_view query, QueryStatus& status) {
        ChangeID changeID;
        openChange(changeID);

        NullSink sink;
        _interpreter->execute(status,
                              query,
                              _graphName,
                              CommitHash::head(),
                              changeID,
                              &_env->getMem(),
                              &sink);
    }
};

TEST_F(DeleteWrittenTest, deletesANodeTheSameQueryCreated) {
    expectWriteRowCount("CREATE (n:Tag {name: 'x'}) DELETE n", 0);

    expectRows("MATCH (t:Tag) RETURN t.name", {});
}

TEST_F(DeleteWrittenTest, deletesANodeTheSameQueryMerged) {
    expectWriteRowCount("MERGE (n:Tag {name: 'x'}) DELETE n", 0);

    expectRows("MATCH (t:Tag) RETURN t.name", {});
}

// A merge binds Remy rather than writing him, so the delete tombstones the committed
// node the way a MATCH would
TEST_F(DeleteWrittenTest, deletesTheCommittedNodeAMergeBound) {
    expectWriteRowCount("MERGE (p:Person {name: 'Remy'}) DETACH DELETE p", 0);

    expectRows("MATCH (p:Person) RETURN count(p)", {{"7"}});
}

TEST_F(DeleteWrittenTest, deletesAnEdgeTheSameQueryCreated) {
    expectWriteRowCount("CREATE (a:Tag {name: 'a'})-[e:LINKS]->(b:Tag {name: 'b'}) DELETE e", 0);

    expectRows("MATCH (t:Tag) RETURN count(t)", {{"2"}});
    expectRows("MATCH (a:Tag)-[:LINKS]->(b:Tag) RETURN a.name", {});
}

// The hop the same query wrote goes with the node it hangs off
TEST_F(DeleteWrittenTest, detachDeletesANodeTheSameQueryJoined) {
    expectWriteRowCount("CREATE (a:Tag {name: 'a'})-[e:LINKS]->(b:Tag {name: 'b'}) DETACH DELETE a", 0);

    expectRows("MATCH (t:Tag) RETURN t.name", {{"b"}});
    expectRows("MATCH (a:Tag)-[:LINKS]->(b:Tag) RETURN a.name", {});
}

// Without DETACH the node still carries that hop, so the delete is refused - the same
// answer a committed node with relationships gets
TEST_F(DeleteWrittenTest, refusesToDeleteAJoinedNodeWithoutDetach) {
    QueryStatus status;
    runQuery("CREATE (a:Tag {name: 'a'})-[e:LINKS]->(b:Tag {name: 'b'}) DELETE a", status);

    EXPECT_FALSE(status.isOk()) << status.getError();
    EXPECT_NE(status.getError().find("DETACH DELETE"), std::string::npos)
        << "status: " << status.getError();
}

// The node is committed but the relationship hanging off it is not, and a relationship
// is a relationship: the delete is refused the same way one on a committed edge is
TEST_F(DeleteWrittenTest, refusesToDeleteACommittedNodeTheQueryJoinedWithoutDetach) {
    expectWriteRowCount("CREATE (t:Tag {name: 'x'})", 0);

    QueryStatus status;
    runQuery("MATCH (t:Tag) CREATE (t)-[:LINKS]->(b:Tag {name: 'b'}) DELETE t", status);

    EXPECT_FALSE(status.isOk()) << status.getError();
    EXPECT_NE(status.getError().find("DETACH DELETE"), std::string::npos)
        << "status: " << status.getError();
}

// With DETACH the same query drops the relationship it wrote along with the node
TEST_F(DeleteWrittenTest, detachDeletesACommittedNodeTheQueryJoined) {
    expectWriteRowCount("CREATE (t:Tag {name: 'x'})", 0);

    expectWriteRowCount("MATCH (t:Tag) CREATE (t)-[:LINKS]->(b:Tag {name: 'b'}) DETACH DELETE t", 0);

    expectRows("MATCH (t:Tag) RETURN t.name", {{"b"}});
    expectRows("MATCH (a:Tag)-[:LINKS]->(b:Tag) RETURN a.name", {});
}

int main(int argc, char** argv) {
    return turing::test::turingTestMain(argc, argv);
}

#include <gtest/gtest.h>

#include <string>
#include <string_view>

#include "QueryInterpreterV3.h"
#include "QueryStatus.h"

#include "TuringDB.h"
#include "dataframe/Dataframe.h"
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
                              &_env->getMem(),
                              &sink);
    }

    // The v2 engine, which the analyzer turns a MERGE away from
    QueryStatus runOnPipelineEngine(std::string_view query) {
        ChangeID changeID;
        openChange(changeID);

        QueryCallbacks callbacks;
        callbacks.setOnOutputData([](const Dataframe*) {});

        const QueryState state(_graphName,
                               &_env->getMem(),
                               &_queryConfig,
                               &callbacks,
                               CommitHash::head(),
                               changeID);

        return _env->getDB().query(query, state);
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

// A CREATE's entities are provisional and the graph a merge reads holds none of them, so
// a merge that would have to bind one is turned away rather than binding the wrong node
TEST_F(MergeRejectionTest, rejectsAPatternBindingWhatACreateInTheSameQueryWrote) {
    QueryStatus status;
    runQuery("CREATE (a:Tag {name: 'x'}) MERGE (a)-[:LINKS]->(b:Tag {name: 'y'})", status);

    EXPECT_FALSE(status.isOk()) << status.getError();
    EXPECT_NE(status.getError().find("a CREATE in the same query writes it"), std::string::npos)
        << "status: " << status.getError();
}

// A merge's rows mix committed entities with entities this change wrote, and a tombstone
// names a committed ID, so the mixture is turned away rather than tombstoning whichever
// committed node a provisional ID collides with
TEST_F(MergeRejectionTest, rejectsADeleteOfWhatAMergeBound) {
    QueryStatus status;
    runQuery("MERGE (n:Tag {name: 'x'}) DELETE n", status);

    EXPECT_FALSE(status.isOk()) << status.getError();
    EXPECT_NE(status.getError().find("a MERGE in the same query binds it"), std::string::npos)
        << "status: " << status.getError();
}

// The pipeline engine has no MERGE, and says so rather than reporting a shape it failed
// to plan
TEST_F(MergeRejectionTest, rejectsMergeOnThePipelineEngine) {
    const QueryStatus status = runOnPipelineEngine("MERGE (n:Tag {name: 'x'})");

    EXPECT_FALSE(status.isOk());
    EXPECT_NE(status.getError().find("MERGE is only supported by the MLIR query engine"), std::string::npos)
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

// A CREATE's rows hold provisional IDs this change has not committed and a tombstone
// names a committed ID, so deleting them is turned away rather than tombstoning
// whichever committed node a provisional ID collides with
TEST_F(MergeRejectionTest, rejectsADeleteOfWhatACreateWrote) {
    QueryStatus status;
    runQuery("CREATE (n:Tag {name: 'x'}) DELETE n", status);

    EXPECT_FALSE(status.isOk()) << status.getError();
    EXPECT_NE(status.getError().find("a CREATE in the same query writes it"), std::string::npos)
        << "status: " << status.getError();
}

int main(int argc, char** argv) {
    return turing::test::turingTestMain(argc, argv);
}

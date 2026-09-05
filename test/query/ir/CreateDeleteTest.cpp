#include <gtest/gtest.h>

#include <stdint.h>

#include <memory>
#include <string>
#include <string_view>

#include "NLOutputSink.h"
#include "QueryInterpreterV3.h"
#include "QueryStatus.h"

#include "Graph.h"
#include "QueryConfig.h"
#include "SimpleGraph.h"
#include "SystemAccessor.h"
#include "SystemManager.h"
#include "TuringDB.h"
#include "dataframe/Dataframe.h"
#include "versioning/Change.h"
#include "versioning/ChangeID.h"
#include "versioning/CommitHash.h"

#include "IRTestRows.h"
#include "TuringTest.h"
#include "TuringTestEnv.h"

using namespace db;
using namespace turing::test;

namespace {

// The eighteen nodes of simpledb, which MATCH (n) matches once a create and its delete
// have netted out
constexpr uint64_t nodeCount = 18;

}

// A DELETE reading what a CREATE bound in the same query: the node the pattern wrote is
// the node the delete removes, rather than one a MATCH found.
class CreateDeleteTest : public TuringTest {
protected:
    void initialize() override {
        _env = TuringTestEnv::create(fs::Path {_outDir} / "turing");
        _interpreter = std::make_unique<QueryInterpreterV3>(&_env->getSystemManager());

        SystemAccessor system = _env->getSystemManager().accessUnique();
        Graph* graph = system.createGraph(_graphName);
        SimpleGraph::createSimpleGraph(graph);
    }

    void openChange(ChangeID& changeID) {
        SystemAccessor system = _env->getSystemManager().accessUnique();
        const auto res = system.newChange(_graphName);
        ASSERT_TRUE(res);

        changeID = res.value()->id();
    }

    void submit(const ChangeID& changeID) {
        QueryCallbacks callbacks;
        callbacks.setOnOutputData([](const Dataframe*) {});

        const QueryState submitState(_graphName,
                                     &_env->getMem(),
                                     &_queryConfig,
                                     &callbacks,
                                     CommitHash::head(),
                                     changeID);
        const QueryStatus status = _env->getDB().query("CHANGE SUBMIT", submitState);
        ASSERT_TRUE(status.isOk()) << "CHANGE SUBMIT failed";
    }

    // Runs a writing query in its own change, filling outStatus so a caller can assert on
    // a deliberate failure. The change is left unsubmitted.
    void runWrite(std::string_view query, QueryStatus& outStatus) {
        ChangeID changeID;
        openChange(changeID);

        NullSink sink;
        _interpreter->execute(outStatus,
                              query,
                              _graphName,
                              CommitHash::head(),
                              changeID,
                              &_env->getMem(),
                              &sink);
    }

    void applyWrite(std::string_view query) {
        ChangeID changeID;
        openChange(changeID);

        NullSink sink;
        QueryStatus status;
        _interpreter->execute(status,
                              query,
                              _graphName,
                              CommitHash::head(),
                              changeID,
                              &_env->getMem(),
                              &sink);
        ASSERT_TRUE(status.isOk()) << "query: " << query << "\nerror: " << status.getError();

        submit(changeID);
    }

    void expectRows(std::string_view query, const Rows& expected) {
        RowSink sink;
        QueryStatus status;
        _interpreter->execute(status,
                              query,
                              _graphName,
                              CommitHash::head(),
                              ChangeID::head(),
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

    void expectCounts(std::string_view query, const Counts& expected) {
        CountSink sink;
        QueryStatus status;
        _interpreter->execute(status,
                              query,
                              _graphName,
                              CommitHash::head(),
                              ChangeID::head(),
                              &_env->getMem(),
                              &sink);
        ASSERT_TRUE(status.isOk()) << "query: " << query << "\nerror: " << status.getError();

        Counts actual;
        sink.sortedCounts(actual);

        EXPECT_EQ(actual, expected) << "query: " << query;
    }

    const std::string _graphName = "simpledb";
    std::unique_ptr<TuringTestEnv> _env;
    std::unique_ptr<QueryInterpreterV3> _interpreter;
    QueryConfig _queryConfig;
};

TEST_F(CreateDeleteTest, deletesTheNodeItJustCreated) {
    QueryStatus status;
    runWrite("CREATE (n:Tag {name: 'ephemeral'}) DELETE n", status);

    EXPECT_TRUE(status.isOk()) << "error: " << status.getError();
}

TEST_F(CreateDeleteTest, createFollowedByItsDeleteAddsNoNode) {
    applyWrite("CREATE (n:Tag {name: 'ephemeral'}) DELETE n");

    expectCounts("MATCH (n) RETURN count(n)", {nodeCount});
}

TEST_F(CreateDeleteTest, deletesOnlyTheNodeTheDeleteNames) {
    applyWrite("CREATE (a:Tag {name: 'kept'}), (b:Tag {name: 'dropped'}) DELETE b");

    expectRows("MATCH (t:Tag) RETURN t.name", {{"kept"}});
}

TEST_F(CreateDeleteTest, deletesTheEdgeItJustCreatedAndKeepsItsEnds) {
    applyWrite("CREATE (a:Tag {name: 'a'})-[e:LINK]->(b:Tag {name: 'b'}) DELETE e");

    expectRows("MATCH (t:Tag) RETURN t.name", {{"a"}, {"b"}});
    expectRows("MATCH (a:Tag)-[:LINK]->(b:Tag) RETURN a.name, b.name", {});
}

TEST_F(CreateDeleteTest, detachDeletesTheNodeItJustCreatedWithItsEdge) {
    applyWrite("CREATE (a:Tag {name: 'a'})-[:LINK]->(b:Tag {name: 'b'}) DETACH DELETE a");

    expectRows("MATCH (t:Tag) RETURN t.name", {{"b"}});
}

// Deleting the edge first leaves the node isolated, so the plain delete of it that follows
// has nothing to reject
TEST_F(CreateDeleteTest, deletesACreatedEdgeAndItsCreatedEndTogether) {
    applyWrite("CREATE (a:Tag {name: 'a'})-[e:LINK]->(b:Tag {name: 'b'}) DELETE e, a");

    expectRows("MATCH (t:Tag) RETURN t.name", {{"b"}});
}

TEST_F(CreateDeleteTest, rejectsAPlainDeleteOfACreatedNodeItsQueryGaveAnEdge) {
    QueryStatus status;
    runWrite("CREATE (a:Tag {name: 'a'})-[:LINK]->(b:Tag {name: 'b'}) DELETE a", status);

    EXPECT_FALSE(status.isOk()) << "a plain DELETE of a connected node should fail";
}

// A created node's ID is its offset in the change's write buffer, which collides with the
// ID of a committed node the delete must leave in place: simpledb's node 0, Remy, and
// every edge incident to him.
TEST_F(CreateDeleteTest, detachDeleteOfACreatedNodeLeavesTheCommittedNodesAlone) {
    applyWrite("CREATE (a:Tag {name: 'a'})-[:LINK]->(b:Tag {name: 'b'}) DETACH DELETE a");

    expectRows("MATCH (p:Person {name: 'Remy'}) RETURN p.name", {{"Remy"}});
    expectRows("MATCH (p:Person)-[e:KNOWS_WELL]->(q:Person) RETURN e.name",
               {{"Adam -> Remy"}, {"Remy -> Adam"}});
    expectRows("MATCH (i:Interest)-[e:KNOWS_WELL]->(p:Person) RETURN e.name", {{"Ghosts -> Remy"}});
}

// The same collision on the edge side, against simpledb's committed edge 0, Remy -> Adam.
TEST_F(CreateDeleteTest, deleteOfACreatedEdgeLeavesTheCommittedEdgesAlone) {
    applyWrite("CREATE (a:Tag {name: 'a'})-[e:LINK]->(b:Tag {name: 'b'}) DELETE e");

    expectRows("MATCH (p:Person)-[e:KNOWS_WELL]->(q:Person) RETURN e.name",
               {{"Adam -> Remy"}, {"Remy -> Adam"}});
}

int main(int argc, char** argv) {
    return turing::test::turingTestMain(argc, argv);
}

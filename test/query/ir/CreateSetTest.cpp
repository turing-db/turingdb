#include <gtest/gtest.h>

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

// A SET writing to what a CREATE bound in the same query: the property lands on the entity
// the pattern wrote, rather than on one a MATCH found.
class CreateSetTest : public TuringTest {
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

    const std::string _graphName = "simpledb";
    std::unique_ptr<TuringTestEnv> _env;
    std::unique_ptr<QueryInterpreterV3> _interpreter;
    QueryConfig _queryConfig;
};

TEST_F(CreateSetTest, setsAPropertyOnTheNodeItJustCreated) {
    applyWrite("CREATE (n:Tag {name: 'x'}) SET n.age = 1");

    expectRows("MATCH (t:Tag) RETURN t.name, t.age", {{"x", "1"}});
}

TEST_F(CreateSetTest, overwritesAPropertyTheCreateWrote) {
    applyWrite("CREATE (n:Tag {name: 'x'}) SET n.name = 'y'");

    expectRows("MATCH (t:Tag) RETURN t.name", {{"y"}});
}

TEST_F(CreateSetTest, setsTwoPropertiesOnTheNodeItJustCreated) {
    applyWrite("CREATE (n:Tag {name: 'x'}) SET n.name = 'y', n.age = 2");

    expectRows("MATCH (t:Tag) RETURN t.name, t.age", {{"y", "2"}});
}

// A created node's ID is its offset in the change's write buffer, which collides with the
// ID of a committed node the SET must leave alone: simpledb's node 0, Remy.
TEST_F(CreateSetTest, setOnACreatedNodeLeavesTheCommittedNodesAlone) {
    applyWrite("CREATE (n:Tag {name: 'x'}) SET n.name = 'y'");

    expectRows("MATCH (p:Person {name: 'Remy'}) RETURN p.name, p.age", {{"Remy", "32"}});
}

TEST_F(CreateSetTest, setsAPropertyOnTheEdgeItJustCreated) {
    applyWrite("CREATE (a:Tag {name: 'a'})-[e:LINK]->(b:Tag {name: 'b'}) SET e.name = 'a -> b'");

    expectRows("MATCH (a:Tag)-[e:LINK]->(b:Tag) RETURN e.name", {{"a -> b"}});
}

// The same collision on the edge side, against simpledb's committed edge 0, Remy -> Adam.
TEST_F(CreateSetTest, setOnACreatedEdgeLeavesTheCommittedEdgesAlone) {
    applyWrite("CREATE (a:Tag {name: 'a'})-[e:LINK]->(b:Tag {name: 'b'}) SET e.name = 'a -> b'");

    expectRows("MATCH (p:Person)-[e:KNOWS_WELL]->(q:Person) RETURN e.name",
               {{"Adam -> Remy"}, {"Remy -> Adam"}});
}

// The SET names a matched node, not a created one, so the CREATE beside it is no reason to
// turn the query away
TEST_F(CreateSetTest, setsAPropertyOnAMatchedNodeBesideACreate) {
    applyWrite("MATCH (p:Person {name: 'Remy'}) CREATE (t:Tag {name: 'x'}) SET p.age = 99");

    expectRows("MATCH (p:Person {name: 'Remy'}) RETURN p.age", {{"99"}});
    expectRows("MATCH (t:Tag) RETURN t.name", {{"x"}});
}

int main(int argc, char** argv) {
    return turing::test::turingTestMain(argc, argv);
}

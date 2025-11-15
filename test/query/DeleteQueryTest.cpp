#include <gtest/gtest.h>

#include "QueryTester.h"
#include "TuringTest.h"
#include "TuringTestEnv.h"

#include "SystemManager.h"
#include "QueryInterpreter.h"
#include "SimpleGraph.h"
#include "versioning/Change.h"

using namespace db;
using namespace turing::test;
using namespace testing;

class DeleteQueryTest : public TuringTest {
public:
    void initialize() override {
        _env = TuringTestEnv::create(fs::Path {_outDir} / "turing");

        Graph* graph = _env->getSystemManager().createGraph("simple");
        SimpleGraph::createSimpleGraph(graph);

        _interp = std::make_unique<QueryInterpreter>(&_env->getSystemManager(),
                                                     &_env->getJobSystem());
    }

protected:
    std::unique_ptr<TuringTestEnv> _env;
    std::unique_ptr<QueryInterpreter> _interp {nullptr};

    inline static const auto newChange = [](QueryTester& tester) {
        auto res = tester.query("CHANGE NEW")
                       .expectVector<const Change*>({}, false)
                       .execute()
                       .outputColumnVector<const Change*>(0);
        const ChangeID id = res.value()->back()->id();
        tester.setChangeID(id);
        return id;
    };

    inline static const auto submitChange = [](QueryTester& tester) {
        tester.query("change submit")
            .execute();
        tester.setChangeID(ChangeID::head());
    };
};

TEST_F(DeleteQueryTest, tryDeleteErrors) {
    QueryTester tester {_env->getMem(), *_interp, "default"};
    
    // Not in change
    tester.query("delete nodes 0")
        .expectError()
        .expectErrorMessage("DeleteStep: Cannot perform deletion outside of a write transaction")
        .execute();
    tester.query("delete edges 0")
        .expectError()
        .expectErrorMessage("DeleteStep: Cannot perform deletion outside of a write transaction")
        .execute();

    newChange(tester);

    // Node does not exist nor in write buffer
    tester.query("delete nodes 0")
        .expectError()
        .expectErrorMessage("Graph does not contain node with ID: 0.")
        .execute();
    tester.query("delete edges 0")
        .expectError()
        .expectErrorMessage("Graph does not contain edge with ID: 0.")
        .execute();

    tester.query("create (n:EphemeralNode)")
        .execute();

    // Node does not exist, but it is in pending commit
    tester.query("delete nodes 0")
        .expectError()
        .expectErrorMessage("Graph does not contain node with ID: 0.")
        .execute();

    tester.query("create (s:EphemeralNode)-[e:EphemeralEdge]-(t:EphemeralNode)")
        .execute();

    tester.query("delete edges 0")
        .expectError()
        .expectErrorMessage("Graph does not contain edge with ID: 0.")
        .execute();

    tester.query("delete nodes 1,2")
        .expectError()
        .expectErrorMessage("Graph does not contain node with ID: 1.")
        .execute();
}

TEST_F(DeleteQueryTest, deletedNodeConflict) {
    QueryTester tester {_env->getMem(), *_interp};
    
    ChangeID firstID = newChange(tester);

    // Delete Remy
    tester.query("delete nodes 0")
        .execute();

    newChange(tester);

    // Also delete Remy
    tester.query("delete nodes 0")
        .execute();

    // Second change deletes Remy
    submitChange(tester);

    tester.setChangeID(firstID);

    // First change also tries to delete Remy
    tester.query("change submit")
        .expectError()
        .expectErrorMessage(
            "This change attempted to delete Node 0 (which is now "
            "Node 0 on main) which has been modified on main.")
        .execute();
}

TEST_F(DeleteQueryTest, deleteEdgeConflict) {
    QueryTester tester {_env->getMem(), *_interp};

    ChangeID firstID = newChange(tester);

    // Delete edge 10
    tester.query("delete edges 10")
        .execute();

    newChange(tester);

    // Also delete edge 10
    tester.query("delete edges 10")
        .execute();

    // Second change deletes edge 10
    submitChange(tester);

    tester.setChangeID(firstID);

    // First change also tries to delete edge 10
    tester.query("change submit")
        .expectError()
        .expectErrorMessage(
            "This change attempted to delete Edge 10 (which is now "
            "Edge 10 on main) which has been modified on main.")
        .execute();
}

TEST_F(DeleteQueryTest, deleteOutEdgeSideEffect) {
    QueryTester tester {_env->getMem(), *_interp};

    ChangeID fstChange = newChange(tester);
    ChangeID sndChange = newChange(tester);

    tester.setChangeID(fstChange);
    tester.query("create (n @ 12)-[e:WORKS_WITH]-(m:Person{name=\"Cyrus\"})")
        .execute();
    submitChange(tester);

    tester.setChangeID(sndChange);
    tester.query("delete nodes 12")
        .execute();

    const std::string expectedError =
        "Submit rejected: Commits on main have created an edge (ID: 13) incident to Node "
        "12, which this Change attempts to delete.";

    tester.query("change submit")
        .expectError()
        .expectErrorMessage(expectedError)
        .execute();
}

TEST_F(DeleteQueryTest, deleteInEdgeSideEffect) {
    QueryTester tester {_env->getMem(), *_interp};

    ChangeID fstChange = newChange(tester);
    ChangeID sndChange = newChange(tester);

    tester.setChangeID(fstChange);
    tester.query("create (m:Person{name=\"Cyrus\"})-[e:WORKS_WITH]-(n @ 12)")
        .execute();
    submitChange(tester);

    tester.setChangeID(sndChange);
    tester.query("delete nodes 12")
        .execute();

    const std::string expectedError =
        "Submit rejected: Commits on main have created an edge (ID: 13) incident to Node "
        "12, which this Change attempts to delete.";

    tester.query("change submit")
        .expectError()
        .expectErrorMessage(expectedError)
        .execute();
}

/*
Testing the following flow:

1. Change1 is created and creates two nodes, and an edge between them
2. Submits
3. Change2 is created deletes the source node
4. Submits
5. Change3 is created deletes the target node
6. Submits

This should be accepted
*/
TEST_F(DeleteQueryTest, noConflictOnDeletedEdge) {
    QueryTester tester {_env->getMem(), *_interp, "default"};

    newChange(tester);
    //                    node 0    edge 0       node 1
    tester.query("create (n:SOURCE)-[e:NEWEDGE]-(m:TARGET)")
        .execute();
    submitChange(tester);

    newChange(tester);
    // This should also delete the edge
    tester.query("delete nodes 0")
        .execute();
    submitChange(tester);

    newChange(tester);
    tester.query("delete nodes 1")
        .execute();
    submitChange(tester);
}

/*
Testing the following flow:

1. Change1 is created and creates two nodes, and an edge between them
2. Submits
3. Change 2 is created
4. Change 3 is created
5. Change 2 deletes the source node
6. Change 3 deletes the target node
7. Change 2 submits -> accepted
8 Change 3 submits -> rejected (write conflict on the edge)
*/
TEST_F(DeleteQueryTest, conflictOnDeletedEdge) {
    QueryTester tester {_env->getMem(), *_interp, "default"};

    newChange(tester);
    //                    node 0    edge 0       node 1
    tester.query("create (n:SOURCE)-[e:NEWEDGE]-(m:TARGET)")
        .execute();
    submitChange(tester);

    ChangeID change2 = newChange(tester);
    ChangeID change3 = newChange(tester);

    tester.setChangeID(change2);
    // This should also delete the edge
    tester.query("delete nodes 0")
        .execute();

    tester.setChangeID(change3);
    tester.query("delete nodes 1")
        .execute();
    submitChange(tester);

    const std::string expectedError =
        "This change attempted to delete Edge 0 (which is now Edge "
        "0 on main) which has been modified on main.";

    tester.setChangeID(change2);
    tester.query("change submit")
        .expectError()
        .expectErrorMessage(expectedError)
        .execute();
}

TEST_F(DeleteQueryTest, deleteTombstonedNode) {
    QueryTester tester {_env->getMem(), *_interp};

    newChange(tester);
    tester.query("delete nodes 10")
        .execute();
    submitChange(tester);

    newChange(tester);
    tester.query("delete nodes 10")
        .expectError() // already deleted: reject
        .execute();
    tester.query("delete nodes 9")
        .execute(); // 9 still exists :accept
    submitChange(tester);
}

TEST_F(DeleteQueryTest, deleteTombstonedEdge) {
    QueryTester tester {_env->getMem(), *_interp};

    newChange(tester);
    tester.query("delete edges 7")
        .execute();
    submitChange(tester);

    newChange(tester);
    tester.query("delete edges 7")
        .expectError() // already deleted: reject
        .execute();
    tester.query("delete edges 8")
        .execute(); // 9 still exists: accept
    submitChange(tester);
}

TEST_F(DeleteQueryTest, idempotentInCommit) {
    QueryTester tester {_env->getMem(), *_interp};

    newChange(tester);
    tester.query("delete edges 7,7,7,7,7")
        .execute();

    tester.query("delete nodes 3,3,3,3,3")
        .execute();

    for (size_t i = 0; i < 5; i++) {
        tester.query("delete edges 5")
            .execute();
        tester.query("delete nodes 4")
            .execute();
    }

    submitChange(tester);

    newChange(tester);

    tester.query("delete edges 7")
        .expectError()
        .expectErrorMessage("Graph does not contain edge with ID: 7.")
        .execute();

    tester.query("delete edges 7,7,7,7")
        .expectError()
        .expectErrorMessage("Graph does not contain edge with ID: 7.")
        .execute();

    tester.query("delete edges 5")
        .expectError()
        .expectErrorMessage("Graph does not contain edge with ID: 5.")
        .execute();

    tester.query("delete edges 5,5,5,5")
        .expectErrorMessage("Graph does not contain edge with ID: 5.")
        .expectError()
        .execute();

    tester.query("delete nodes 3")
        .expectError()
        .expectErrorMessage("Graph does not contain node with ID: 3.")
        .execute();

    tester.query("delete nodes 3,3,3,3")
        .expectError()
        .expectErrorMessage("Graph does not contain node with ID: 3.")
        .execute();

    tester.query("delete nodes 4")
        .expectError()
        .expectErrorMessage("Graph does not contain node with ID: 4.")
        .execute();

    tester.query("delete nodes 4,4,4,4")
        .expectErrorMessage("Graph does not contain node with ID: 4.")
        .expectError()
        .execute();
}

TEST_F(DeleteQueryTest, deleteNodesScanNodes) {
    QueryTester tester {_env->getMem(), *_interp};

    { // Delete the first node
        auto VERIFY = [&]() {
            tester.query("match (n) return n")
                .expectVector<NodeID>({1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12})
                .execute();
        };

        newChange(tester);
        tester.query("delete nodes 0")
            .execute();
        tester.query("commit")
            .execute();
        VERIFY();
        submitChange(tester);
        VERIFY();
    }

    { // Delete some nodes from the middle of the graph
        auto VERIFY = [&]() {
            tester.query("match (n) return n")
                .expectVector<NodeID>({1, 2, 3, 5, 6, 7, 8, 9, 11, 12})
                .execute();
        };

        newChange(tester);
        tester.query("delete nodes 4, 10")
            .execute();
        tester.query("commit")
            .execute();
        VERIFY();
        submitChange(tester);
        VERIFY();
    }

    { // Delete the last node
        auto VERIFY = [&]() {
            tester.query("match (n) return n")
                .expectVector<NodeID>({1, 2, 3, 5, 6, 7, 8, 9, 11})
                .execute();
        };

        newChange(tester);
        tester.query("delete nodes 12")
            .execute();
        tester.query("commit")
            .execute();
        VERIFY();
        submitChange(tester);
        VERIFY();
    }
}

TEST_F(DeleteQueryTest, deleteEdgesScanEdges) {
    QueryTester tester {_env->getMem(), *_interp};

    { // Delete the first edge
        auto VERIFY = [&]() {
            tester.query("match (n)-[e]-(m) return e")
                .expectVector<EdgeID>({1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12})
                .execute();
        };

        newChange(tester);
        tester.query("delete edges 0")
            .execute();
        tester.query("commit")
            .execute();
        VERIFY();
        submitChange(tester);
        VERIFY();
    }

    { // Delete some edges from the middle of the graph
        auto VERIFY = [&]() {
            tester.query("match (n)-[e]-(m) return e")
                .expectVector<EdgeID>({1, 2, 4, 5, 6, 7, 8, 9, 10, 12})
                .execute();
        };

        newChange(tester);
        tester.query("delete edges 3, 11")
            .execute();
        tester.query("commit")
            .execute();
        VERIFY();
        submitChange(tester);
        VERIFY();
    }

    { // Delete the last edge
        auto VERIFY = [&]() {
            tester.query("match (n)-[e]-(m) return e")
                .expectVector<EdgeID>({1, 2, 4, 5, 6, 7, 8, 9, 10})
                .execute();
        };

        newChange(tester);
        tester.query("delete edges 12")
            .execute();
        tester.query("commit")
            .execute();
        VERIFY();
        submitChange(tester);
        VERIFY();
    }
}

TEST_F(DeleteQueryTest, deleteNodesScanEdges) {
    QueryTester tester {_env->getMem(), *_interp};

    { // Delete Remy, ensure his edges are also deleted
        auto VERIFY = [&]() {
            tester.query("match (n)-[e]-(m) return e")
                .expectVector<EdgeID>({5, 6, 8, 9, 10, 11, 12})
                .execute();
        };
        newChange(tester);
        tester.query("delete nodes 0")
            .execute();
        tester.query("commit")
            .execute();
        VERIFY();
        submitChange(tester);
        VERIFY();
    }

    { // Delete Maxime, ensure his edges are also deleted
        auto VERIFY = [&]() {
            tester.query("match (n)-[e]-(m) return e")
                .expectVector<EdgeID>({5, 6, 10, 11, 12})
                .execute();
        };
        newChange(tester);
        tester.query("delete nodes 8")
            .execute();
        tester.query("commit")
            .execute();
        VERIFY();
        submitChange(tester);
        VERIFY();
    }

    { // Delete Cooking, ensure Martina -> Cooking and Adam -> Cooking are deleted
        auto VERIFY = [&]() {
            tester.query("match (n)-[e]-(m) return e")
                .expectVector<EdgeID>({5, 10, 11})
                .execute();
        };
        newChange(tester);
        tester.query("delete nodes 5")
            .execute();
        tester.query("commit")
            .execute();
        VERIFY();
        submitChange(tester);
        VERIFY();
    }
}

// Same as @ref deleteNodesScanEdges, but does not materialise edgeID column
TEST_F(DeleteQueryTest, delNodesScanEdgesNoEdgeIDs) {
    QueryTester tester {_env->getMem(), *_interp};

    { // Delete Remy, ensure his edges are also deleted
        auto VERIFY = [&]() {
            tester.query("match (n)-[e]-(m) return n,m")/*,e")*/
                .expectVector<NodeID>({1, 1, 8, 8, 9, 9, 11})
                .expectVector<NodeID>({4, 5, 4, 7, 10, 2, 5})
                // .expectVector<EdgeID>({5, 6, 8, 9, 10, 11, 12})
                .execute();
        };
        newChange(tester);
        tester.query("delete nodes 0")
            .execute();
        tester.query("commit")
            .execute();
        VERIFY();
        submitChange(tester);
        VERIFY();
    }

    { // Delete Maxime, ensure his edges are also deleted
        auto VERIFY = [&]() {
            tester.query("match (n)-[e]-(m) return n,m")/*,e")*/
                .expectVector<NodeID>({1, 1, 9, 9, 11})
                .expectVector<NodeID>({4, 5, 10, 2, 5})
                // .expectVector<EdgeID>({5, 6, 10, 11, 12})
                .execute();
        };
        newChange(tester);
        tester.query("delete nodes 8")
            .execute();
        tester.query("commit")
            .execute();
        VERIFY();
        submitChange(tester);
        VERIFY();
    }

    { // Delete Cooking, ensure Martina -> Cooking and Adam -> Cooking are deleted
        auto VERIFY = [&]() {
            tester.query("match (n)-[e]-(m) return n,m")/*,e")*/
                .expectVector<NodeID>({1, 9, 9})
                .expectVector<NodeID>({4, 10, 2})
                // .expectVector<EdgeID>({5, 10, 11})
                .execute();
        };
        newChange(tester);
        tester.query("delete nodes 5")
            .execute();
        tester.query("commit")
            .execute();
        VERIFY();
        submitChange(tester);
        VERIFY();
    }
}

TEST_F(DeleteQueryTest, delEdgesScanEdgesNoEdgeIDs) {
    QueryTester tester {_env->getMem(), *_interp};

    { // Below passes with the comments uncommented, fails with as edge column is not
      // materialised to filter
        auto VERIFY = [&]() {
            tester.query("match (n)-[e]-(m) return n,m"/*, e*/)
                .expectVector<NodeID>({0, 0, 0, 0, 1, 1, 1, 6, 8, 8, 11})
                .expectVector<NodeID>({1, 6, 2, 3, 0, 4, 5, 0, 4, 7, 5})
                // .expectVector<EdgeID>({0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 12})
                .execute();
        };
        newChange(tester);
        tester.query("delete edges 10, 11") // Delete Luc's edges
            .execute();
        tester.query("commit")
            .execute();
        VERIFY();
        submitChange(tester);
        VERIFY();
    }
}

TEST_F(DeleteQueryTest, delNodeGetOutEdges) {
    QueryTester tester {_env->getMem(), *_interp};

    { // Remy, then use NodeID injection to force GetOutEdges
        auto VERIFY = [&]() {
            tester.query("match (n{name=\"Remy\"})-[e]-(m) return e")
                .expectVector<EdgeID>({})
                .execute();
        };

        newChange(tester);
        tester.query("delete nodes 0")
            .execute();
        tester.query("commit")
            .execute();
        VERIFY();
        submitChange(tester);
        VERIFY();
    }
}

TEST_F(DeleteQueryTest, delEdgesGetOutEdges) {
    QueryTester tester {_env->getMem(), *_interp};

    { // Delete some of Adam's edges, then use NodeID injection to force GetOutEdges
        auto VERIFY = [&]() {
            tester.query("match (n @ 1)-[e]-(m) return e,m")
                .expectVector<EdgeID>({5})
                .expectVector<NodeID>({4})
                .execute();
        };

        newChange(tester);
        tester.query("delete edges 4,6")
            .execute();
        tester.query("commit")
            .execute();
        VERIFY();
        submitChange(tester);
        VERIFY();
    }
}

TEST_F(DeleteQueryTest, delNodeGetOutEdgesNoEdges) {
    QueryTester tester {_env->getMem(), *_interp};

    { // Remy, then use NodeID injection to force GetOutEdges
        auto VERIFY = [&]() { // Only return n so e is not allocced
            tester.query("match (n{name=\"Remy\"})-[e]-(m) return n")
                .expectVector<NodeID>({})
                .execute();
        };

        newChange(tester);
        tester.query("delete nodes 0")
            .execute();
        tester.query("commit")
            .execute();
        VERIFY();
        submitChange(tester);
        VERIFY();
    }
}

TEST_F(DeleteQueryTest, delEdgeGetOutEdgesNoEdges) {
    QueryTester tester {_env->getMem(), *_interp};

    { // Remy, use NodeID injection to force GetOutEdges
        auto VERIFY = [&]() { // Only return n so e is not allocced
            tester.query("match (n @ 0)-[e]-(m) return n")
                .expectVector<NodeID>({0}) // There is still 1 Remy edge left
                .execute();
        };

        newChange(tester);
        tester.query("delete edges 0,1,2")
            .execute();
        tester.query("commit")
            .execute();
        VERIFY();
        submitChange(tester);
        VERIFY();
    }
}

TEST_F(DeleteQueryTest, delNodesScanLabels) {
    QueryTester tester {_env->getMem(), *_interp};

    { // Delete the first node
        auto VERIFY = [&]() {
            tester.query("match (n:Person) return n")
                .expectVector<NodeID>({1, 8, 9, 11, 12})
                .execute();
        };

        newChange(tester);
        tester.query("delete nodes 0")
            .execute();
        tester.query("commit")
            .execute();
        VERIFY();
        submitChange(tester);
        VERIFY();
    }

    { // Delete some nodes from the middle of the graph
        auto VERIFY = [&]() {
            tester.query("match (n:Person) return n")
                .expectVector<NodeID>({1, 8, 12})
                .execute();
        };

        newChange(tester);
        tester.query("delete nodes 9, 11")
            .execute();
        tester.query("commit")
            .execute();
        VERIFY();
        submitChange(tester);
        VERIFY();
    }

    { // Delete the last node
        auto VERIFY = [&]() {
            tester.query("match (n:Person) return n")
                .expectVector<NodeID>({1, 8})
                .execute();
        };

        newChange(tester);
        tester.query("delete nodes 12")
            .execute();
        tester.query("commit")
            .execute();
        VERIFY();
        submitChange(tester);
        VERIFY();
    }
}

TEST_F(DeleteQueryTest, delNodesStringApprox) {
    QueryTester tester {_env->getMem(), *_interp};

    { // Delete unrelated node
        auto VERIFY = [&]() {
            tester.query("match (n{name~=\"Ma\"}) return n")
                .expectVector<NodeID>({8, 11})
                .execute();
        };

        newChange(tester);
        tester.query("delete nodes 0")
            .execute();
        tester.query("commit")
            .execute();
        VERIFY();
        submitChange(tester);
        VERIFY();
    }

    { // Delete Martina
        auto VERIFY = [&]() {
            tester.query("match (n{name~=\"Ma\"}) return n")
                .expectVector<NodeID>({8})
                .execute();
        };

        newChange(tester);
        tester.query("delete nodes 11")
            .execute();
        tester.query("commit")
            .execute();
        VERIFY();
        submitChange(tester);
        VERIFY();
    }

    { // Delete Maxime
        auto VERIFY = [&]() {
            tester.query("match (n{name~=\"Ma\"}) return n")
                .expectVector<NodeID>({})
                .execute();
        };

        newChange(tester);
        tester.query("delete nodes 8")
            .execute();
        tester.query("commit")
            .execute();
        VERIFY();
        submitChange(tester);
        VERIFY();
    }
}

// Test LookupStringIndexStep
TEST_F(DeleteQueryTest, delEdgesStringApprox) {
    QueryTester tester {_env->getMem(), *_interp};

    {
        auto VERIFY = [&](){
            tester.query("match (n)-[e{name~=\"Ma\"}]-(m) return e")
                .expectVector<EdgeID>({8,12})
                .execute();
        };

        newChange(tester);
        tester.query("delete edges 9")
            .execute();
        tester.query("commit")
            .execute();
        VERIFY();
        submitChange(tester);
        VERIFY();
    }

    {
        auto VERIFY = [&](){
            tester.query("match (n)-[e{name~=\"Ma\"}]-(m) return e")
                .expectVector<EdgeID>({8})
                .execute();
        };

        newChange(tester);
        tester.query("delete edges 12")
            .execute();
        tester.query("commit")
            .execute();
        VERIFY();
        submitChange(tester);
        VERIFY();
    }

    {
        auto VERIFY = [&](){
            tester.query("match (n)-[e{name~=\"Ma\"}]-(m) return e")
                .expectVector<EdgeID>({})
                .execute();
        };

        newChange(tester);
        tester.query("delete edges 8")
            .execute();
        tester.query("commit")
            .execute();
        VERIFY();
        submitChange(tester);
        VERIFY();
    }
}

TEST_F(DeleteQueryTest, deleteNodesScanStringProperties) {
    QueryTester tester {_env->getMem(), *_interp};

    { // Delete unrelated node
        auto VERIFY = [&](){
            tester.query("match (n{name=\"Remy\"}) return n")
                .expectVector<NodeID>({0})
                .execute();
        };

        newChange(tester);
        tester.query("delete nodes 1")
            .execute();
        tester.query("commit")
            .execute();
        VERIFY();
        submitChange(tester);
        VERIFY();
    }

    { // Delete only node which matches
        auto VERIFY = [&](){
            tester.query("match (n{name=\"Remy\"}) return n")
                .expectVector<NodeID>({})
                .execute();
        };

        newChange(tester);
        tester.query("delete nodes 0")
            .execute();
        tester.query("commit")
            .execute();
        VERIFY();
        submitChange(tester);
        VERIFY();
    }
}

TEST_F(DeleteQueryTest, deleteNodesScanBoolProperties) {
    QueryTester tester {_env->getMem(), *_interp};

    {
        auto VERIFY = [&]() {
            tester.query("match (n{hasPhD=true}) return n, n.hasPhD")
                .expectVector<NodeID>({0, 11})
                .expectOptVector<types::Bool::Primitive>({true, true})
                .execute();
        };

        newChange(tester);
        tester.query("delete nodes 1, 9")
            .execute();
        tester.query("commit")
            .execute();
        VERIFY();
        submitChange(tester);
        VERIFY();
    }

    {
        auto VERIFY = [&]() {
                tester.query("match (n{hasPhD=true}) return n, n.hasPhD")
                    .expectVector<NodeID>({0})
                    .expectOptVector<types::Bool::Primitive>({true})
                    .execute();
            };

        newChange(tester);
        tester.query("delete nodes 11").execute();
        tester.query("commit").execute();
        VERIFY();
        submitChange(tester);
        VERIFY();
    }

    {
        auto VERIFY = [&]() {
            tester.query("match (n{hasPhD=true}) return n, n.hasPhD")
                .expectVector<NodeID>({})
                .expectOptVector<types::Bool::Primitive>({})
                .execute();
        };

        newChange(tester);
        tester.query("delete nodes 0").execute();
        tester.query("commit").execute();
        VERIFY();
        submitChange(tester);
        VERIFY();
    }
}

TEST_F(DeleteQueryTest, deleteCommitThenRebase) {
    QueryTester tester {_env->getMem(), *_interp, "default"};

    newChange(tester);
    tester.query("create (n:NODE)-[e:EDGE]-(m:NODE)")
        .execute();
    submitChange(tester);

    ChangeID ch1 = newChange(tester);
    ChangeID ch2 = newChange(tester);

    tester.setChangeID(ch1);

    tester.query("create (n:NODE)-[e:EDGE]-(m:NODE)") // IDs 2, 1, 3 locally
        .execute();
    tester.query("commit")
        .execute();
    tester.query("delete nodes 2") // Delete a node we just created
        .execute();
    tester.query("commit")
        .execute();

    tester.setChangeID(ch2);

    tester.query("create (n:NODE)-[e:EDGE]-(m:NODE)")  // IDs 2, 1, 3 locally
        .execute();
    submitChange(tester); // IDs are now 2, 1, 3 on main

    tester.query("match (n) return n")
        .expectVector<NodeID>({0,1,2,3})
        .execute();
    tester.query("match (n)-[e]-(m) return e")
        .expectVector<EdgeID>({0,1})
        .execute();

    tester.setChangeID(ch1); // This change created 2 nodes and one edge, and deleted the node
    submitChange(tester);    // Now rebase will +2 to nodeIDs and +1 to EdgeIDs

    tester.query("match (n) return n")
        .expectVector<NodeID>({0,1,2,3,/*4,*/ 5}) // Change1 created 4 (locally as 2) but deleted it
        .execute();
    tester.query("match (n)-[e]-(m) return e") // Change1 created 2 (locally as 1) but deleted it
        .expectVector<EdgeID>({0,1, /*2*/})
        .execute();
}

TEST_F(DeleteQueryTest, multipleCommitsThenRebase) {
    QueryTester tester {_env->getMem(), *_interp, "default"};

    newChange(tester);
    tester.query("create (n:NODE)-[e:EDGE]-(m:NODE)")
        .execute();
    submitChange(tester);

    ChangeID ch1 = newChange(tester);
    ChangeID ch2 = newChange(tester);

    tester.setChangeID(ch1);

    tester.query("create (n:NODE)-[e:EDGE]-(m:NODE)") // IDs 2, 1, 3 locally
        .execute();
    tester.query("commit")
        .execute();
    tester.query("delete nodes 2") // Delete a node we just created
        .execute();
    tester.query("commit")
        .execute();

    tester.query("create (n:NEWNODE)") // Node 4 localy
        .execute();
    tester.query("commit")
        .execute();
    tester.query("delete nodes 1")
        .execute();

    tester.setChangeID(ch2);

    tester.query("create (n:NODE)-[e:EDGE]-(m:NODE)") // IDs 2, 1, 3 locally
        .execute();
    submitChange(tester); // IDs are now 2, 1, 3 on main

    tester.query("match (n) return n")
        .expectVector<NodeID>({0, 1, 2, 3})
        .execute();
    tester.query("match (n)-[e]-(m) return e")
        .expectVector<EdgeID>({0, 1})
        .execute();

    tester.setChangeID(ch1); // This change created 2 nodes and one edge, and deleted the node
    submitChange(tester); // Now rebase will +2 to nodeIDs and +1 to EdgeIDs

    tester.query("match (n) return n")
        .expectVector<NodeID>(
            {0, 2, 3, /*4,*/ 5, 6})
        .execute();
    tester.query("match (n)-[e]-(m) return e") // Change1 created 2 (locally as 1) but deleted it
        .expectVector<EdgeID>({1, /*2*/})
        .execute();
}

TEST_F(DeleteQueryTest, deleteAllNodes) {
    QueryTester tester {_env->getMem(), *_interp};

    newChange(tester);
    tester.query("delete nodes 0,1,2,3,4,5,6,7,8,9,10,11,12")
        .execute();
    submitChange(tester);

    tester.query("match (n) return n")
        .expectVector<NodeID>({})
        .execute();
    tester.query("match (n)-[e]-(m) return n,e,m")
        .expectVector<NodeID>({})
        .expectVector<EdgeID>({})
        .expectVector<NodeID>({})
        .execute();
}

TEST_F(DeleteQueryTest, deleteAllEdges) {
    QueryTester tester {_env->getMem(), *_interp};

    newChange(tester);
    tester.query("delete edges 0,1,2,3,4,5,6,7,8,9,10,11,12")
        .execute();
    submitChange(tester);

    tester.query("match (n) return n")
        .expectVector<NodeID>({0,1,2,3,4,5,6,7,8,9,10,11,12})
        .execute();
    tester.query("match (n)-[e]-(m) return n,e,m")
        .expectVector<NodeID>({})
        .expectVector<EdgeID>({})
        .expectVector<NodeID>({})
        .execute();
}

// Test to ensure we cannot create edges between nodes which have been deleted
TEST_F(DeleteQueryTest, injectDeletedNode) {
    QueryTester tester {_env->getMem(), *_interp};

    {
        newChange(tester);
        tester.query("delete nodes 8") // Delete Maxime
            .execute();
        submitChange(tester);
    }

    {
        newChange(tester);
        tester.query("create (n @ 8)-[e:NEWEDGE]-(m:NEWNODE)")
            .expectError()
            .expectErrorMessage("No such node with ID: '8'")
            .execute();
    }

}

TEST_F(DeleteQueryTest, resolvedOutEdgesDeleteConflict) {
    QueryTester tester {_env->getMem(), *_interp};

    ChangeID change0 = newChange(tester);
    ChangeID change1 = newChange(tester);

    { // Try and delete Luc
        tester.setChangeID(change0);
        tester.query("delete nodes 9")
            .execute();
    }

    { // But create an edge between Luc and Cyrus
        tester.setChangeID(change1);
        // This edge has ID 13
        tester.query(R"(create (n @ 9)-[e:WorksWith{name="Luc -> Cyrus"}]-(m:Person{name="Cyrus"}))")
            .execute();
        submitChange(tester);
    }

    { // Change 2 actually just deletes the new edge from Luc
        newChange(tester); // Change 2
        tester.query("delete edges 13")
            .execute();
        submitChange(tester);
    }

    { // So Change 0 can delete Luc as the graph is the same as when it branched
        tester.setChangeID(change0);
        submitChange(tester);
    }
}

TEST_F(DeleteQueryTest, resolvedInEdgesDeleteConflict) {
    QueryTester tester {_env->getMem(), *_interp};

    ChangeID change0 = newChange(tester);
    ChangeID change1 = newChange(tester);

    { // Try and delete Luc
        tester.setChangeID(change0);
        tester.query("delete nodes 9")
            .execute();
    }

    { // But create an edge between Cyrus and Luc
        tester.setChangeID(change1);
        // This edge has ID 13
        tester.query(R"(create (m:Person{name="Cyrus"})-[e:WorksWith{name="Cyrus -> Luc"}]-(n @ 9))")
            .execute();
        submitChange(tester);
    }

    { // Change 2 actually just deletes the new edge from Cyrus
        newChange(tester); // Change 2
        tester.query("delete edges 13")
            .execute();
        submitChange(tester);
    }

    { // So Change 0 can delete Luc as the graph is the same as when it branched
        tester.setChangeID(change0);
        submitChange(tester);
    }
}

TEST_F(DeleteQueryTest, resolvedEdgesDeleteConflict) {
    QueryTester tester {_env->getMem(), *_interp};

    ChangeID change0 = newChange(tester);
    ChangeID change1 = newChange(tester);

    { // Try and delete Luc
        tester.setChangeID(change0);
        tester.query("delete nodes 9")
            .execute();
    }

    { // But create an edge between Cyrus and Luc and Luc and Doruk
        tester.setChangeID(change1);
        // This edge has ID 13
        tester.query(R"(create (m:Person{name="Cyrus"})-[e:WorksWith{name="Cyrus -> Luc"}]-(n @ 9))")
            .execute();
        tester.query(R"(create (n @ 9)-[e:WorksWith{name="Luc -> Doruk"}]-(m:Person{name="Doruk"}))")
            .execute();
        submitChange(tester);
    }

    { // Change 2 actually just deletes the new edge from Cyrus and to Doruk
        newChange(tester); // Change 2
        tester.query("delete edges 13, 14")
            .execute();
        submitChange(tester);
    }

    { // So Change 0 can delete Luc as the graph is the same as when it branched
        tester.setChangeID(change0);
        submitChange(tester);
    }
}

TEST_F(DeleteQueryTest, validAfterDeletedNodeSSIReject) {
    QueryTester tester {_env->getMem(), *_interp};

    ChangeID firstID = newChange(tester);

    // Delete Remy
    tester.query("delete nodes 0")
        .execute();

    newChange(tester);

    // Also delete Remy
    tester.query("delete nodes 0")
        .execute();

    // Second change deletes Remy
    submitChange(tester);

    tester.setChangeID(firstID);

    // First change also tries to delete Remy
    tester.query("change submit")
        .expectError()
        .expectErrorMessage("This change attempted to delete Node 0 (which is now "
                            "Node 0 on main) which has been modified on main.")
        .execute();

    // We were just rejected on main, but we should still be in a valid state
    // Remy is not deleted as we have not committed:
    tester.query("match (n) return n")
        .expectVector<NodeID>({0,1,2,3,4,5,6,7,8,9,10,11,12})
        .execute();

    tester.query("commit")
        .execute();

    // Now Remy should be deleted
    tester.query("match (n) return n")
        .expectVector<NodeID>({1,2,3,4,5,6,7,8,9,10,11,12})
        .execute();
}

TEST_F(DeleteQueryTest, validAfterDeletedEdgeSSIReject) {
    QueryTester tester {_env->getMem(), *_interp};

    ChangeID firstID = newChange(tester);

    // Delete edge 10
    tester.query("delete edges 10")
        .execute();

    newChange(tester);

    // Also delete edge 10
    tester.query("delete edges 10")
        .execute();

    // Second change deletes edge 10
    submitChange(tester);

    tester.setChangeID(firstID);

    // First change also tries to delete edge 10
    tester.query("change submit")
        .expectError()
        .expectErrorMessage(
            "This change attempted to delete Edge 10 (which is now "
            "Edge 10 on main) which has been modified on main.")
        .execute();

    // We were just rejected on main, but we should still be in a valid state
    // Edge 10 is not deleted as we have not committed:
    tester.query("match (n)-[e]-(m) return e")
        .expectVector<EdgeID>({0,1,2,3,4,5,6,7,8,9,10,11,12})
        .execute();

    tester.query("commit")
        .execute();

    // Now Edge 10 should be deleted
    tester.query("match (n)-[e]-(m) return e")
        .expectVector<EdgeID>({0,1,2,3,4,5,6,7,8,9,11,12})
        .execute();
}

TEST_F(DeleteQueryTest, validAfterDeleteEdgeSideEffectSSIReject) {
    QueryTester tester {_env->getMem(), *_interp};

    ChangeID fstChange = newChange(tester);
    ChangeID sndChange = newChange(tester);

    { // First Change
        tester.setChangeID(fstChange);
        tester.query("create (n @ 12)-[e:WORKS_WITH]-(m:Person{name=\"Cyrus\"})")
            .execute();
        submitChange(tester);
    }

    { // Second Change
        tester.setChangeID(sndChange);
        tester.query("delete nodes 12").execute();

        const std::string expectedError =
            "Submit rejected: Commits on main have created an edge (ID: 13) incident to "
            "Node 12, which this Change attempts to delete.";

        tester.query("change submit")
            .expectError()
            .expectErrorMessage(expectedError)
            .execute();

        // Nothing has been deleted: we haven't committed/submitted
        tester.query("match (n) return n")
            .expectVector<NodeID>({0,1,2,3,4,5,6,7,8,9,10,11,12})
            .execute();
        tester.query("match (n)-[e]-(m) return e")
            .expectVector<EdgeID>({0,1,2,3,4,5,6,7,8,9,10,11,12})
            .execute();

        tester.query("commit")
            .execute();

        // Now we have submitted, we should see our local changes
        tester.query("match (n) return n")
            .expectVector<NodeID>({0,1,2,3,4,5,6,7,8,9,10,11})
            .execute();
        // Deleting Suhas should not delete any edges
        tester.query("match (n)-[e]-(m) return e")
            .expectVector<EdgeID>({0,1,2,3,4,5,6,7,8,9,10,11,12})
            .execute();
    }
}

TEST_F(DeleteQueryTest, validAfterDelEdgeConflictSSIReject) {
    QueryTester tester {_env->getMem(), *_interp, "default"};

    { // Change 1
        newChange(tester);
        //                    node 0    edge 0       node 1
        tester.query("create (n:SOURCE)-[e:NEWEDGE]-(m:TARGET)").execute();
        submitChange(tester);
    }

    ChangeID change2 = newChange(tester);
    ChangeID change3 = newChange(tester);

    { // Change 2
        tester.setChangeID(change2);
        // This should also delete the edge
        tester.query("delete nodes 0").execute();
    }

    { // Change 3
        tester.setChangeID(change3);
        tester.query("delete nodes 1").execute();
        submitChange(tester);
    }

    { // Change 2
        const std::string expectedError = "This change attempted "
                                          "to delete Edge 0 (which is now Edge "
                                          "0 on main) which has been modified on main.";

        tester.setChangeID(change2);
        tester.query("change submit")
            .expectError()
            .expectErrorMessage(expectedError)
            .execute();

        // We tried to delete node 0, but were rejected, we should see both nodes and the
        // edge
        tester.query("match (n)-[e]-(m) return n,e,m")
            .expectVector<NodeID>({0})
            .expectVector<EdgeID>({0})
            .expectVector<NodeID>({1})
            .execute();

        // Now apply the pending changes
        tester.query("commit")
            .execute();

        // Node 0 deleted => Edge 0 deleted => no edges
        tester.query("match (n)-[e]-(m) return n,e,m")
            .expectVector<NodeID>({})
            .expectVector<EdgeID>({})
            .expectVector<NodeID>({})
            .execute();

        // Only node 1 remains (even thought it was deleted on main)
        tester.query("match (n) return n")
            .expectVector<NodeID>({1})
            .execute();
    }
}

TEST_F(DeleteQueryTest, rebasedTombstones) {
    QueryTester tester {_env->getMem(), *_interp};

    ChangeID firstChange = newChange(tester);
    ChangeID secondChange = newChange(tester);

    // Create changes under the second
    tester.setChangeID(firstChange);
    tester.query(R"(CREATE (n:Person{name="Cyrus"}))") // Node 13
        .execute();
    tester.query(R"(CREATE (n:Person{name="Doruk"}))") // Node 14
        .execute();
    submitChange(tester);


    // Second change creates new nodes too, but deletes them
    tester.setChangeID(secondChange);
    tester.query(R"(CREATE (n:Interest{name="Manchester"}))") // Node 13
        .execute();
    tester.query(R"(CREATE (n:Interest{name="Music"}))")      // Node 14
        .execute();
    tester.query(R"(CREATE (n:Interest{name="Cats"}))")       // Node 15
        .execute();
    tester.query("commit")
        .execute();
    tester.query("delete nodes 13")
        .execute();
    submitChange(tester);

    // Node 13 shouldn't be deleted: 13->15 rebased on main: 15 deleted

    tester.query("match (n) return n")
        .expectVector<NodeID>({0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 16, 17})
        .execute();
}

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
            "Unexpected exception: This change attempted to delete Node 0 (which is now "
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
            "Unexpected exception: This change attempted to delete Edge 10 (which is now "
            "Edge 10 on main) which has been modified on main.")
        .execute();
}

TEST_F(DeleteQueryTest, deleteEdgeSideEffect) {
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
    tester.query("change submit")
        .expectError()
        .expectErrorMessage("ERRR")
        .execute();
}

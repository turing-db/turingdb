#include <cstdint>
#include <gtest/gtest.h>

#include "Path.h"
#include "QueryInterpreter.h"
#include "QueryTester.h"
#include "SystemManager.h"
#include "TuringDB.h"
#include "TuringTest.h"
#include "TuringTestEnv.h"
#include "SimpleGraph.h"
#include "metadata/PropertyType.h"
#include "versioning/Change.h"

using namespace db;
using namespace turing::test;

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

    // new change and set the id
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

TEST_F(DeleteQueryTest, deleteRemy) {
    QueryTester tester {_env->getMem(), *_interp};

    tester.query("match (n) return n, n.name")
        .expectVector<NodeID>({0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12})
        .expectOptVector<types::String::Primitive>(
            {"Remy", "Adam", "Computers", "Eighties", "Bio", "Cooking", "Ghosts",
             "Paddle", "Maxime", "Luc", "Animals", "Martina", "Suhas"});

    tester.query("match (n)-[e]-(m) return e, e.name")
        .expectVector<EdgeID>({0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12})
        .expectOptVector<types::String::Primitive>({
            "Remy -> Adam",
            "Remy -> Ghosts",
            "Remy -> Computers",
            "Remy -> Eighties",
            "Adam -> Remy",
            "Adam -> Bio",
            "Adam -> Cooking",
            "Ghosts -> Remy",
            "Maxime -> Bio",
            "Maxime -> Paddle",
            "Luc -> Animals",
            "Luc -> Computers",
            "Martina -> Cooking",
        })
        .execute();

    newChange(tester);

    tester.query("delete nodes 0")
        .execute();

    tester.query("change submit")
          .execute();

    tester.setChangeID(ChangeID::head());

    tester.query("match (n)-[e]-(m) return n, n.name")
        // nodes in 1st DP << 1, node 6 missing
        .expectVector<NodeID>({0, 1, 2, 3, 4, 5, 7, 8, 9, 10, 11, 12})
        // Remy gone
        .expectOptVector<types::String::Primitive>(
            {"Adam", "Computers", "Eighties", "Bio", "Cooking", "Ghosts", "Paddle",
             "Maxime", "Luc", "Animals", "Martina", "Suhas"});
    // edges incident to remy are gone
    tester.query("match (n)-[e]-(m) return e, e.name")
        .expectVector<EdgeID>({0, 1, 8, 9, 10, 11, 12})
        .expectOptVector<types::String::Primitive>({
            "Adam -> Bio",
            "Adam -> Cooking",
            "Maxime -> Bio",
            "Maxime -> Paddle",
            "Luc -> Animals",
            "Luc -> Computers",
            "Martina -> Cooking",
        })
        .execute();
}

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

TEST_F(DeleteQueryTest, commitThenDelete) {
    QueryTester tester {_env->getMem(), *_interp, "default"};

    newChange(tester);

    tester.query("create (n:NewNode{deleted=false})")
        .execute();

    tester.query("change submit")
        .execute();

    tester.setChangeID(ChangeID::head());

    tester.query("match (n) return n, n.deleted")
        .expectVector<NodeID>({0})
        .expectOptVector<types::Bool::Primitive>({false})
        .execute();

    newChange(tester);

    tester.query("delete nodes 0")
        .execute();

    tester.query("change submit")
        .execute();

    tester.setChangeID(ChangeID::head());

    tester.query("match (n) return n, n.deleted")
        .expectVector<NodeID>({})
        .expectOptVector<types::Bool::Primitive>({})
        .execute();
}

// NOTE @Cyrus @Remy agree to *not* delete label sets upon final owning node being deleted
TEST_F(DeleteQueryTest, deleteLabelSet) {
    QueryTester tester {_env->getMem(), *_interp, "default"};
    newChange(tester);
    tester.query("create (n:NewNode{deleted=false})")
        .execute();
    tester.query("change submit")
        .execute();
    tester.setChangeID(ChangeID::head());
    tester.query("call labelsets ()")
        .expectVector<LabelSetID>({0})
        .expectVector<types::String::Primitive>({"NewNode"})
        .execute();

    newChange(tester);
    tester.query("delete nodes 0")
        .execute();
    tester.query("change submit")
        .execute();
    tester.setChangeID(ChangeID::head());
    // Labelset still exist
    tester.query("call labelsets ()")
        .expectVector<LabelSetID>({0})
        .expectVector<types::String::Primitive>({"NewNode"})
        .execute();
}

TEST_F(DeleteQueryTest, starPattern) {
    QueryTester tester {_env->getMem(), *_interp, "default"};
    const std::string CREATE_STAR_QUERY =
        "CREATE (a:Node {name: 'A'}), (b:Node {name: 'B'}), (c:Node {name: 'C'}), "
        "(d:Node {name: 'D'}), (e:Node {name: 'E'}), (center:Node {name: 'Center'}), "
        "(a)-[:CONNECTED_TO{edge=true}]-(center), (b)-[:CONNECTED_TO{edge=true}]-(center), "
        "(c)-[:CONNECTED_TO{edge=true}]-(center), (d)-[:CONNECTED_TO{edge=true}]-(center), "
        "(e)-[:CONNECTED_TO{edge=true}]-(center)";
    const NodeID CENTRE_ID = 5;

    newChange(tester);
    tester.query(CREATE_STAR_QUERY)
        .execute();
    tester.query("change submit")
        .execute();
    tester.setChangeID(ChangeID::head());

    auto matchNode = [](const char name) {
        return fmt::format("MATCH (n:Node {{name: '{}'}})-[e]-(m) RETURN e.edge", name);
    };

    constexpr std::array<char, 5> names = {'A', 'B', 'C', 'D', 'E'};
    // Expect each node to have one edge
    for (char name : names) {
        tester.query(matchNode(name))
            .expectOptVector<types::Bool::Primitive>({true})
            .execute();
    }

    newChange(tester);
    // Delete the centre node
    tester.query("delete nodes " + std::to_string(CENTRE_ID.getValue()))
        .execute();

    tester.query("change submit")
        .execute();
    tester.setChangeID(ChangeID::head());

    // Expect each node to have no edges
    for (char name : names) {
        tester.query(matchNode(name))
            .expectOptVector<types::Bool::Primitive>({})
            .execute();
    }
}

TEST_F(DeleteQueryTest, inAndOutEdges) {
    QueryTester tester {_env->getMem(), *_interp, "default"};

    newChange(tester);

    // s - p - t
    // p has in and out edges
    tester.query("create (s:SOURCE{capacity=100}), (t:SINK{capacity=100}), (p:NODE{capacity=10}), (s)-[e:PIPE{rusty=true}]-(p), (p)-[e:PIPE{rusty=true}]-(t)")
        .execute();
    tester.query("change submit")
        .execute();

    constexpr uint64_t SOURCEID = 0;
    constexpr uint64_t NODEID = 2;
    constexpr uint64_t SINKID = 1;
    constexpr uint64_t FIRSTEDGE = 0;
    constexpr uint64_t SECONDEDGE = 1;

    tester.setChangeID(ChangeID::head());

    tester.query("match (n)-[e]-(m) return n,e,m, n.capacity, e.rusty, m.capacity")
        .expectVector<NodeID>({SOURCEID, NODEID})
        .expectVector<EdgeID>({FIRSTEDGE, SECONDEDGE})
        .expectVector<NodeID>({NODEID, SINKID})
        .expectOptVector<types::Int64::Primitive>({100, 10})
        .expectOptVector<types::Bool::Primitive>({true, true})
        .expectOptVector<types::Int64::Primitive>({10, 100})
        .execute();

    // Delete the middle node (p)
    newChange(tester);
    tester.query("delete nodes " + std::to_string(NODEID))
        .execute();
    submitChange(tester);

    // Should be no edges
    tester.query("match (n)-[e]-(m) return e")
        .expectVector<EdgeID>({})
        .execute();

    // Should just have source and sink
    tester.query("match (n) return n, n.capacity")
        .expectVector<NodeID>({SOURCEID, SINKID})
        .expectOptVector<types::Int64::Primitive>({100,100})
        .execute();
}

TEST_F(DeleteQueryTest, deleteThenCreate) {
    QueryTester tester{_env->getMem(), *_interp};

    newChange(tester);
    tester.query("delete nodes 0")
        .execute();
    tester.query("change submit")
          .execute();
    tester.setChangeID(ChangeID::head());
    // See @ref deleteRemy for validation of the above behaviour

    newChange(tester);
    tester.query("create (n:Person{name=\"Cyrus\", age=22, isFrench=false, hasPhd=true})")
        .execute();
    tester.query("commit")
        .execute();

    tester.query("match (n @ 13) return n, n.name")
        .expectVector<NodeID>({13})
        .expectOptVector<types::String::Primitive>({"Cyrus"})
        .execute();
    auto animalIDOpt = tester.query("match (n{name=\"Animals\"}) return n")
        .expectVector<NodeID>({333}, false)
        .execute()
        .outputColumnVector<NodeID>(0);
    ASSERT_TRUE(animalIDOpt.has_value());
    NodeID animalID = animalIDOpt.value()->at(0);

    auto newEdgeQuery = fmt::format("create (n @ 13)-[e:LOVES{{name=\"Cyrus -> Animals\"}}]-(m @ {})", animalID.getValue());
    tester.query(newEdgeQuery)
        .execute();
    tester.query("commit")
        .execute();

    tester.query("match (n)-[e]-(m) return n, n.name")
        // nodes in 1st DP << 1, node 6 missing, 13 added
        .expectVector<NodeID>({0, 1, 2, 3, 4, 5, 7, 8, 9, 10, 11, 12, 13})
        // Remy gone, cyrus here
        .expectOptVector<types::String::Primitive>(
            {"Adam", "Computers", "Eighties", "Bio", "Cooking", "Ghosts", "Paddle",
             "Maxime", "Luc", "Animals", "Martina", "Suhas", "Cyrus"});

    tester.query("match (n)-[e]-(m) return e, e.name")
        // Remy edges gone, cyrus edge here
        .expectVector<EdgeID>({0, 1, 8, 9, 10, 11, 12, 13})
        .expectOptVector<types::String::Primitive>({
            "Adam -> Bio",
            "Adam -> Cooking",
            "Maxime -> Bio",
            "Maxime -> Paddle",
            "Luc -> Animals",
            "Luc -> Computers",
            "Martina -> Cooking",
            "Cyrus -> Animals",
        })
        .execute();
}

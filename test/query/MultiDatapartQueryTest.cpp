#include <gtest/gtest.h>

#include "LocalMemory.h"
#include "SystemManager.h"
#include "QueryInterpreter.h"
#include "TuringDB.h"
#include "SimpleGraph.h"
#include "QueryTester.h"
#include "writers/GraphWriter.h"
#include "TuringTest.h"

#define EDGEID(edgeName) \
    edgeName._edgeID.getValue()

#define NODEID(nodeName) \
    nodeName.getValue()

using namespace db;

// The tests in QueryTest.cpp cover a lot of the multidatapart cases.
// In this class we try and touch edge cases that might not be commonly seen
class MultiDatapartQueryTest : public turing::test::TuringTest {
public:
protected:
    TuringDB _db;
    LocalMemory _mem;
    std::unique_ptr<QueryInterpreter> _interp {nullptr};
};

TEST_F(MultiDatapartQueryTest, MultiDatapartTest) {
    SystemManager& sysMan = _db.getSystemManager();
    Graph* graph = sysMan.createGraph("simple");
    GraphWriter writer {graph};
    _interp = std::make_unique<QueryInterpreter>(&_db.getSystemManager(),
                                                 &_db.getJobSystem());
    QueryTester tester {_mem, *_interp};

    const auto node1 = writer.addNode({"label1", "label2", "label3"});
    writer.addNodeProperty<types::String>(node1, "name", "node1");
    writer.addNodeProperty<types::String>(node1, "dob", "00/01");
    writer.addNodeProperty<types::Bool>(node1, "popular", false);
    writer.addNodeProperty<types::Bool>(node1, "knows_djistraka", true);

    const auto node2 = writer.addNode({"label1", "label2", "label3"});
    writer.addNodeProperty<types::String>(node2, "name", "node2");
    writer.addNodeProperty<types::String>(node2, "dob", "00/02");
    writer.addNodeProperty<types::Bool>(node2, "popular", true);
    writer.addNodeProperty<types::Bool>(node2, "leaf", true);
    writer.addNodeProperty<types::Bool>(node2, "knows_djistraka", false);

    writer.commit();

    const auto edge1 = writer.addEdge("CONNECTION", node1, node2);
    writer.addEdgeProperty<types::String>(edge1, "name", "edge1");
    writer.addEdgeProperty<types::String>(edge1, "proficiency", "expert");
    writer.addEdgeProperty<types::Int64>(edge1, "weight", 200);

    writer.commit();

    const auto node3 = writer.addNode({"label3", "label4"});
    writer.addNodeProperty<types::String>(node3, "name", "node3");
    writer.addNodeProperty<types::String>(node3, "dob", "00/03");
    writer.addNodeProperty<types::Bool>(node3, "popular", true);
    writer.addNodeProperty<types::Bool>(node3, "knows_djistraka", false);

    const auto edge2 = writer.addEdge("CONNECTION", node1, node3);
    writer.addEdgeProperty<types::String>(edge2, "name", "edge2");
    writer.addEdgeProperty<types::String>(edge2, "proficiency", "moderate");
    writer.addEdgeProperty<types::Int64>(edge2, "weight", 20);

    writer.commit();

    const auto node4 = writer.addNode({"label6"});
    writer.addNodeProperty<types::String>(node4, "name", "node4");
    writer.addNodeProperty<types::String>(node4, "dob", "00/04");
    writer.addNodeProperty<types::Bool>(node4, "leaf", true);
    writer.addNodeProperty<types::Bool>(node4, "knows_djistraka", false);

    writer.commit();

    const auto edge3 = writer.addEdge("CONNECTION", node3, node4);
    writer.addEdgeProperty<types::String>(edge3, "name", "edge3");
    writer.addEdgeProperty<types::String>(edge3, "proficiency", "beginner");
    writer.addEdgeProperty<types::Bool>(edge3, "twig", false);
    writer.addEdgeProperty<types::Int64>(edge3, "weight", 3);

    writer.submit();

    // Tests labelset index when a datapart in the iteration has no labelsets and needs to be skipped
    tester.query("MATCH (n:label1)--(m:label3) RETURN n,m")
        .expectVector<NodeID>({NODEID(node1), NODEID(node1)})
        .expectVector<NodeID>({NODEID(node2), NODEID(node3)})
        .execute();

    // Tests node property indexing when a datapart in the iteration doesn't have a node property
    // container and needs to be skipped
    tester.query("MATCH (n{\"knows_djistraka\":true})--(m{\"knows_djistraka\":false}) RETURN n,m")
        .expectVector<NodeID>({NODEID(node1), NODEID(node1)})
        .expectVector<NodeID>({NODEID(node2), NODEID(node3)})
        .execute();

    // Tests edge property indexing when a datapart in the iteration doesn't have a edge property
    // container and needs to be skipped
    tester.query("MATCH (n)-[e{proficiency:\"beginner\"}]-(m) RETURN e")
        .expectVector<EdgeID>({EDGEID(edge3)})
        .execute();

    // Test node property indexing when we need to skip datapart that has a node property container
    // but not the property type we are looking for.
    tester.query("MATCH (n{leaf:true}) RETURN n")
        .expectVector<NodeID>({NODEID(node2), NODEID(node4)})
        .execute();

    // Test edge property indexing when we need to skip datapart that has a edge property container
    // but not the property type we are looking for.
    tester.query("MATCH (n)-[e{twig:false}]-(m) RETURN e")
        .expectVector<EdgeID>({EDGEID(edge3)})
        .execute();
}

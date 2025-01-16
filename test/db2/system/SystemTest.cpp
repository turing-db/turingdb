#include "gtest/gtest.h"

#include "SystemManager.h"
#include "Graph.h"

using namespace db;

class SystemManagerTest : public ::testing::Test {
};

TEST_F(SystemManagerTest, createGraph) {
    SystemManager sysMan;

    // The default graph exists
    Graph* defaultGraph = sysMan.getDefaultGraph();
    ASSERT_TRUE(defaultGraph);
    ASSERT_EQ(defaultGraph->getName(), "default");

    // We can find back the default db by its name
    ASSERT_EQ(sysMan.getDefaultGraph(), sysMan.getGraph("default"));

    // Create graph
    Graph* myGraph = sysMan.createGraph("mygraph");
    ASSERT_TRUE(myGraph);
    ASSERT_NE(defaultGraph, myGraph);
    ASSERT_EQ(myGraph->getName(), "mygraph");

    // Try to create a graph with the same name, should return nullptr
    Graph* myGraph2 = sysMan.createGraph("mygraph");
    ASSERT_TRUE(!myGraph2);

    // Create a graph with a different name
    Graph* diffGraph = sysMan.createGraph("diffgraph");
    ASSERT_TRUE(diffGraph);
    ASSERT_EQ(diffGraph->getName(), "diffgraph");
    ASSERT_NE(diffGraph, myGraph);
    ASSERT_NE(diffGraph, defaultGraph);

    // Test that we get back mygraph when we search by name
    ASSERT_EQ(sysMan.getGraph("mygraph"), myGraph);
    ASSERT_EQ(sysMan.getGraph("diffgraph"), diffGraph);
}

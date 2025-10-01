#include "gtest/gtest.h"

#include "SystemManager.h"
#include "Graph.h"
#include "TuringTest.h"
#include "TuringTestEnv.h"

using namespace db;
using namespace turing::test;

class SystemManagerTest : public TuringTest {
};

TEST_F(SystemManagerTest, createGraph) {
    auto env = TuringTestEnv::create(fs::Path {_outDir} / "turing");
    auto& sysMan = env->getSystemManager();

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

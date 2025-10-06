#include <gtest/gtest.h>

#include "TuringDB.h"
#include "SystemManager.h"
#include "QueryInterpreter.h"
#include "TuringTest.h"
#include "dump/GraphDumper.h"
#include "dump/GraphLoader.h"
#include "Graph.h"
#include "SimpleGraph.h"
#include "Path.h"
#include "TuringTestEnv.h"
#include "QueryTester.h"

using namespace db;
using namespace turing::test;

class QueryIndexExistenceTest : public TuringTest {
public:
    void initialize() override {
        _env = TuringTestEnv::createSyncedOnDisk(fs::Path {_outDir} / "turing");

        _builtGraph = _env->getSystemManager().createGraph("simpledb");
        SimpleGraph::createSimpleGraph(_builtGraph);
        _interp = std::make_unique<QueryInterpreter>(&_env->getSystemManager(),
                                                     &_env->getJobSystem());
    }

protected:
    Graph* _builtGraph {nullptr};
    std::unique_ptr<TuringTestEnv> _env;
    std::unique_ptr<QueryInterpreter> _interp {nullptr};
};

// Dump graph, load it: index should be present
TEST_F(QueryIndexExistenceTest, noStringIndex) {
    {
        ASSERT_TRUE(_env->getSystemManager().dumpGraph("simpledb"));
    }

    // Create a new environment pointing to the same directory
    auto env2 = TuringTestEnv::create(fs::Path {_outDir} / "turing");
    auto newInterp = std::make_unique<QueryInterpreter>(&env2->getSystemManager(),
                                                        &env2->getJobSystem());

    ASSERT_TRUE(env2->getSystemManager().loadGraph("simpledb"));

    QueryTester tester {env2->getMem(), *newInterp};
    tester.setGraphName("simpledb");

    tester.query("MATCH (n{name=\"Remy\"}) return n")
        .expectVector<NodeID>({0})
        .execute();

    tester.query("MATCH (n{name~=\"Remy\"}) return n")
        .expectVector<NodeID>({0})
        .execute();
}

int main(int argc, char** argv) {
    return turing::test::turingTestMain(argc, argv, [] {
        testing::GTEST_FLAG(repeat) = 3;
    });
}

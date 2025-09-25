#include <gtest/gtest.h>

#include "TuringDB.h"
#include "SystemManager.h"
#include "QueryInterpreter.h"
#include "LocalMemory.h"
#include "TuringTest.h"
#include "GraphDumper.h"
#include "GraphLoader.h"
#include "Graph.h"
#include "SimpleGraph.h"
#include "Path.h"
#include "TuringConfig.h"
#include "QueryTester.h"

using namespace db;

class QueryIndexExistenceTest : public turing::test::TuringTest {
public:
    void initialize() override {
        const fs::Path turingDir = fs::Path(_outDir) / "turing";
        _config.setTuringDirectory(turingDir);
        _db = std::make_unique<TuringDB>(_config);

        SystemManager& sysMan = _db->getSystemManager();

        _builtGraph = sysMan.createGraph("simple");
        SimpleGraph::createSimpleGraph(_builtGraph);
        _interp = std::make_unique<QueryInterpreter>(&_db->getSystemManager(),
                                                     &_db->getJobSystem());

        auto graphDir = sysMan.getConfig().getGraphsDir();
    }

protected:
    TuringConfig _config;
    std::unique_ptr<TuringDB> _db;
    LocalMemory _mem;
    Graph* _builtGraph {nullptr};
    std::unique_ptr<QueryInterpreter> _interp {nullptr};
};

// Dump graph, load it: index should be present
TEST_F(QueryIndexExistenceTest, noStringIndex) {
    {
        auto& sysMan = _db->getSystemManager();
        ASSERT_TRUE(sysMan.dumpGraph("simple"));
    }

    TuringDB newDB(_config);
    LocalMemory newMem;
    auto newInterp = std::make_unique<QueryInterpreter>(&newDB.getSystemManager(),
                                                        &newDB.getJobSystem());

    auto& sysMan = newDB.getSystemManager();
    ASSERT_TRUE(sysMan.loadGraph("simple"));

    QueryTester tester {newMem, *newInterp};
    tester.setGraphName("simple");

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

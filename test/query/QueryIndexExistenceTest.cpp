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
#include "QueryTester.h"

using namespace db;

class QueryIndexExistenceTest : public turing::test::TuringTest {
public:
    void initialize() override {
        SystemManager& sysMan = _db.getSystemManager();
        _builtGraph = sysMan.createGraph("simple");
        SimpleGraph::createSimpleGraph(_builtGraph);
        _interp = std::make_unique<QueryInterpreter>(&_db.getSystemManager(),
                                                     &_db.getJobSystem());

        auto graphDir = sysMan.getGraphsDir();
        std::string x = std::string(graphDir.filename());
        _workingPath = fs::Path("newSimple");

        if (FileUtils::exists(_workingPath.filename())) {
            FileUtils::removeDirectory(_workingPath.filename());
        }
    }

    void terminate() override {
        if (FileUtils::exists(_workingPath.filename())) {
            FileUtils::removeDirectory(_workingPath.filename());
        }
    }
protected:
    TuringDB _db;
    LocalMemory _mem;
    fs::Path _workingPath;
    Graph* _builtGraph;
    std::unique_ptr<QueryInterpreter> _interp {nullptr};
};

// Dump graph, load it: index should not be present
// TODO: This test should fail when index dumping is implemented.
TEST_F(QueryIndexExistenceTest, noStringApproxIndex) {
    GraphDumper dumper;
    auto res = dumper.dump(*_builtGraph, _workingPath);
    if (!res) {
        throw TuringException("Failed to dump graph:\n" + res.error().fmtMessage());
    }

    const std::string& newName = "newSimple";
    TuringDB newDB;
    LocalMemory newMem;
    std::unique_ptr<QueryInterpreter> newInterp =
        std::make_unique<QueryInterpreter>(&newDB.getSystemManager(), &newDB.getJobSystem());

    auto graphDir = newDB.getSystemManager().getGraphsDir();

    bool loadRes =
        newDB.getSystemManager().loadGraph(_workingPath, newName, newDB.getJobSystem());
    ASSERT_TRUE(loadRes);


    QueryTester tester {newMem, *newInterp};
    tester.setGraphName(newName);

    tester.query("MATCH (n{name=\"Remy\"}) return n")
        .expectVector<NodeID>({0})
        .execute();

    tester.query("MATCH (n{name~=\"Remy\"}) return n")
        .expectError()
        .expectErrorMessage("Approximate string index was not initialised. The current graph was likely loaded incorrectly.")
        .execute();
}

int main(int argc, char** argv) {
    return turing::test::turingTestMain(argc, argv, [] {
        testing::GTEST_FLAG(repeat) = 3;
    });
}

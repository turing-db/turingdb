#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "GraphDumper.h"
#include "Path.h"
#include "SystemManager.h"
#include "TuringConfig.h"
#include "TuringException.h"
#include "TuringTest.h"
#include "TuringTestEnv.h"

#include "SimpleGraph.h"
#include "GraphLoader.h"
#include "TuringDB.h"
#include "LocalMemory.h"
#include "Graph.h"
#include "comparators/GraphComparator.h"
#include "views/GraphView.h"

using namespace db;
using namespace turing::test;

class SimpleGraphSerialisationTest : public TuringTest {
public:
    void initialize() override {
        _env = TuringTestEnv::createSyncedOnDisk(fs::Path {_outDir} / "turing");

        _builtGraph = _env->getSystemManager().createGraph("simple");
        SimpleGraph::createSimpleGraph(_builtGraph);
        _workingPath = fs::Path {_outDir + "/testfile"};

        loadDumpLoadSimpleDb();
    }

protected:
    std::unique_ptr<TuringTestEnv> _env;
    Graph* _builtGraph {nullptr};
    std::unique_ptr<Graph> _loadedGraph;
    LocalMemory _mem;
    fs::Path _workingPath;

private:
    void loadDumpLoadSimpleDb() {
        GraphDumper dumper;

        auto res = dumper.dump(*_builtGraph, _workingPath);
        if (!res) {
            throw TuringException("Failed to dump graph:\n" + res.error().fmtMessage());
        }

        _loadedGraph = Graph::create();
        const auto loadRes = GraphLoader::load(_loadedGraph.get(), _workingPath);
        if (!loadRes) {
            throw TuringException("Failed to dump graph:\n" + res.error().fmtMessage());
        }
    }
};

TEST_F(SimpleGraphSerialisationTest, indexInitialisation) {
    ASSERT_TRUE(GraphComparator::same(*_builtGraph, *_loadedGraph));
}

int main(int argc, char** argv) {
    return turingTestMain(argc, argv, [] { testing::GTEST_FLAG(repeat) = 3; });
}

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "GraphDumper.h"
#include "Path.h"
#include "SystemManager.h"
#include "TuringConfig.h"
#include "TuringException.h"
#include "TuringTest.h"

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
        _config.setSyncedOnDisk(false);
        _db = std::make_unique<TuringDB>(_config);

        SystemManager& sysMan = _db->getSystemManager();
        _builtGraph = sysMan.createGraph("simple");
        SimpleGraph::createSimpleGraph(_builtGraph);
        _workingPath = fs::Path {_outDir + "/testfile"};

        // XXX: Need to remove the directory created in TuringTest.h SetUp() has
        // GraphDumper requires the directory does not exist
        if (FileUtils::exists(_workingPath.filename())) {
            FileUtils::removeDirectory(_workingPath.filename());
        }
        loadDumpLoadSimpleDb();
    }

protected:
    TuringConfig _config;
    std::unique_ptr<TuringDB> _db;
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

        _loadedGraph = Graph::createEmptyGraph();
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

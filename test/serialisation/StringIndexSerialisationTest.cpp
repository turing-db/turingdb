#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "GraphDumper.h"
#include "Path.h"
#include "TuringException.h"
#include "TuringTest.h"

#include "TuringDB.h"
#include "LocalMemory.h"
#include "Graph.h"
#include "SystemManager.h"
#include "indexers/StringPropertyIndexer.h"
#include "reader/GraphReader.h"
#include "versioning/Transaction.h"
#include "views/GraphView.h"
#include "GraphLoader.h"
#include "TuringConfig.h"
#include "SimpleGraph.h"

using namespace db;
using namespace turing::test;


class StringIndexSerialisationTest : public TuringTest {
public:
    void initialize()  override {
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

TEST_F(StringIndexSerialisationTest, indexInitialisation) {
    auto tx = _builtGraph->openTransaction();
    auto reader = tx.readGraph();
    auto builtDps = reader.dataparts();

    for (const auto& dp : builtDps) {
        EXPECT_TRUE(dp->getEdgeStrPropIndexer().isInitialised());
        EXPECT_TRUE(dp->getNodeStrPropIndexer().isInitialised());
    }

    auto txl = _loadedGraph->openTransaction();
    auto readerl = txl.readGraph();
    auto loadedDps = readerl.dataparts();

    for (const auto& dp : loadedDps) {
        EXPECT_TRUE(dp->getEdgeStrPropIndexer().isInitialised());
        EXPECT_TRUE(dp->getNodeStrPropIndexer().isInitialised());
    }
}

int main(int argc, char** argv) {
    return turingTestMain(argc, argv, [] {
        testing::GTEST_FLAG(repeat) = 3;
    });
}

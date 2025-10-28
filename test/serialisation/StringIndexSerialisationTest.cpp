#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "DataPart.h"
#include "dump/GraphDumper.h"
#include "Path.h"
#include "TuringException.h"
#include "TuringTest.h"

#include "SystemManager.h"
#include "dump/GraphLoader.h"
#include "TuringConfig.h"
#include "TuringTestEnv.h"
#include "SimpleGraph.h"
#include "indexers/StringPropertyIndexer.h"
#include "reader/GraphReader.h"
#include "versioning/ChangeID.h"
#include "versioning/Transaction.h"

using namespace db;
using namespace turing::test;

class StringIndexSerialisationTest : public TuringTest {
public:
    void initialize()  override {
        _env.getConfig().setSyncedOnDisk(false);
        _workingPath = fs::Path {_outDir + "/testfile"};
        _config.setTuringDirectory(_workingPath);

        SystemManager& sysMan = _env.getSystemManager();
        Graph* builtGraph = sysMan.createGraph(_builtGraphName);
        SimpleGraph::createSimpleGraph(builtGraph);

        loadDumpLoadSimpleDb();
    }

protected:
    TuringConfig _config;
    TuringTestEnv _env;
    std::string _builtGraphName {"simple"};
    std::string _loadedGraphName {"newsimple"};
    fs::Path _workingPath;

private:
    void loadDumpLoadSimpleDb() {
        SystemManager& sysMan = _env.getSystemManager();

        auto res = GraphDumper::dump(*sysMan.getGraph(_builtGraphName), _workingPath);
        if (!res) {
            throw TuringException("Failed to dump graph:\n" + res.error().fmtMessage());
        }

        Graph* loadedGraph = sysMan.createGraph(_loadedGraphName);
        const auto loadRes = GraphLoader::load(loadedGraph, _workingPath);
        if (!loadRes) {
            throw TuringException("Failed to dump graph:\n" + res.error().fmtMessage());
        }
    }
};

TEST_F(StringIndexSerialisationTest, indexInitialisation) {
    SystemManager& sysMan = _env.getSystemManager();
    auto txRes = sysMan.openTransaction(_builtGraphName, CommitHash::head(), ChangeID::head());
    ASSERT_TRUE(txRes);
    auto& tx = txRes.value();
    auto reader = tx.readGraph();
    auto builtDps = reader.dataparts();

    for (const auto& dp : builtDps) {
        EXPECT_TRUE(dp->getEdgeStrPropIndexer().isInitialised());
        EXPECT_TRUE(dp->getNodeStrPropIndexer().isInitialised());
    }

    auto txLRes = sysMan.openTransaction(_loadedGraphName, CommitHash::head(), ChangeID::head());
    ASSERT_TRUE(txLRes);
    auto& txLoad = txLRes.value();

    auto readerLoad = txLoad.readGraph();
    auto loadedDps = readerLoad.dataparts();

    for (const auto& dp : loadedDps) {
        EXPECT_TRUE(dp->getEdgeStrPropIndexer().isInitialised());
        EXPECT_TRUE(dp->getNodeStrPropIndexer().isInitialised());
    }
}

int main(int argc, char** argv) {
    return turingTestMain(argc, argv, [] {
        testing::GTEST_FLAG(repeat) = 1;
    });
}

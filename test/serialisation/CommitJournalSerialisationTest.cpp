#include <gtest/gtest.h>

#include "Graph.h"
#include "SystemManager.h"
#include "TuringConfig.h"
#include "TuringException.h"
#include "TuringTest.h"
#include "TuringDB.h"
#include "SimpleGraph.h"
#include "dump/GraphDumper.h"
#include "dump/GraphLoader.h"
#include "reader/GraphReader.h"
#include "versioning/Transaction.h"

using namespace db;
using namespace turing::test;

class CommitJournalSerialisationTest : public TuringTest {
public:
    void initialize()  override {
        _config.setSyncedOnDisk(false);
        _workingPath = fs::Path {_outDir + "/testfile"};
        _config.setTuringDirectory(_workingPath);
        _db = std::make_unique<TuringDB>(&_config);
        _db->run();

        SystemManager& sysMan = _db->getSystemManager();
        _builtGraph = sysMan.createGraph("simple");
        SimpleGraph::createSimpleGraph(_builtGraph);

        // XXX: Need to remove the directory created in TuringTest.h SetUp() has
        // GraphDumper requires the directory does not exist
        if (FileUtils::exists(_workingPath.filename())) {
            FileUtils::removeDirectory(_workingPath.filename());
        }
    }

protected:
    TuringConfig _config;
    std::unique_ptr<TuringDB> _db;
    Graph* _builtGraph {nullptr};
    std::unique_ptr<Graph> _loadedGraph;
    fs::Path _workingPath;

    void dumpLoadSimpleDB() {
        auto res = GraphDumper::dump(*_builtGraph, _workingPath);
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

TEST_F(CommitJournalSerialisationTest, emptyOnCreation) {
    const auto tx = _builtGraph->openTransaction();    
    const auto reader = tx.readGraph();
    const auto commitViews = reader.commits();

    // Write sets should be empty when creating simple db
    for (const auto& commitView : commitViews) {
        const CommitJournal& journal = commitView.history().journal();
        EXPECT_TRUE(journal.nodeWriteSet().empty());
        EXPECT_TRUE(journal.edgeWriteSet().empty());
    }
}

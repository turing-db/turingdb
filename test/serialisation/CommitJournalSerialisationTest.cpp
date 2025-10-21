#include <gtest/gtest.h>

#include "Graph.h"
#include "SystemManager.h"
#include "TuringConfig.h"
#include "TuringException.h"
#include "TuringTest.h"
#include "TuringDB.h"
#include "SimpleGraph.h"
#include "TuringTestEnv.h"
#include "dump/GraphDumper.h"
#include "dump/GraphLoader.h"
#include "reader/GraphReader.h"
#include "versioning/Change.h"
#include "versioning/ChangeID.h"
#include "versioning/CommitHash.h"
#include "versioning/Transaction.h"

using namespace db;
using namespace turing::test;

class CommitJournalSerialisationTest : public TuringTest {
public:
    void initialize()  override {
        _config.setSyncedOnDisk(false);
        _workingPath = fs::Path {_outDir + "/testfile"};
        _env = TuringTestEnv::create(_workingPath);
        _config.setTuringDirectory(_workingPath);

        SystemManager& sysMan = _env->getDB().getSystemManager();
        _builtGraph = sysMan.createGraph("simple");
        SimpleGraph::createSimpleGraph(_builtGraph);
    }

protected:
    TuringConfig _config;
    Graph* _builtGraph {nullptr};
    std::unique_ptr<Graph> _loadedGraph;
    fs::Path _workingPath;
    std::unique_ptr<TuringTestEnv> _env;

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

    ChangeID newChange() {
        auto changeRes = _env->getDB().getSystemManager().newChange("simple");
        bioassert(changeRes);
        Change* change = changeRes.value();
        ChangeID changeID = change->id();
        return changeID;
    }
};

TEST_F(CommitJournalSerialisationTest, emptyOnCreation) {
    { // On creation
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

    dumpLoadSimpleDB();

    { // After dumping and loading
        const auto tx = _loadedGraph->openTransaction();
        const auto reader = tx.readGraph();
        const auto commitViews = reader.commits();

        // Write sets should be empty when creating simple db
        for (const auto& commitView : commitViews) {
            const CommitJournal& journal = commitView.history().journal();
            EXPECT_TRUE(journal.nodeWriteSet().empty());
            EXPECT_TRUE(journal.edgeWriteSet().empty());
        }
    }
}

TEST_F(CommitJournalSerialisationTest, createNodeThenLoad) {
    size_t numCommits = 0;
    {
        const auto tx = _builtGraph->openTransaction();
        const auto reader = tx.readGraph();
        const auto commitViews = reader.commits();
        numCommits = commitViews.size();
    }

    // Make a change to the graph
    {
        ChangeID changeID = newChange();
        auto res1 = _env->getDB().query("create (n:NEWNODE)", "simple", &_env->getMem(),
                               CommitHash::head(), changeID);
        ASSERT_TRUE(res1);

        auto res2 = _env->getDB().query("change submit", "simple", &_env->getMem(),
                               CommitHash::head(), changeID);
        ASSERT_TRUE(res2);
    }

    { // Check the commits
        const auto txRes = _env->getDB().getSystemManager().openTransaction(
            "simple", CommitHash::head(), ChangeID::head());
        ASSERT_TRUE(txRes.has_value());

        const auto& tx = txRes.value();
        const auto reader = tx.readGraph();
        const auto commitViews = reader.commits();

        // We should have an extra commit
        ASSERT_TRUE(commitViews.size() == numCommits + 1);
        const CommitView& commitOfInterest = commitViews.back();
        // This commit should contain an entry in the write set
        ASSERT_FALSE(commitOfInterest.history().journal().empty());
        const CommitJournal& journalOfInterest = commitOfInterest.history().journal();

        ASSERT_TRUE(journalOfInterest.nodeWriteSet().size() == 1);
        ASSERT_TRUE(journalOfInterest.nodeWriteSet().contains(13));
    }

    dumpLoadSimpleDB();

    { // Check the commits of the dumped and loaded graph
        const auto tx = _loadedGraph->openTransaction();
        const auto reader = tx.readGraph();
        const auto commitViews = reader.commits();

        // We should have an extra commit
        ASSERT_TRUE(commitViews.size() == numCommits + 1);
        const CommitView& commitOfInterest = commitViews.back();
        // This commit should contain an entry in the write set
        ASSERT_FALSE(commitOfInterest.history().journal().empty());
        const CommitJournal& journalOfInterest = commitOfInterest.history().journal();

        ASSERT_TRUE(journalOfInterest.nodeWriteSet().size() == 1);
        ASSERT_TRUE(journalOfInterest.nodeWriteSet().contains(13));
    }
}

TEST_F(CommitJournalSerialisationTest, createNodesAndEdgesThenLoad) {
    size_t numCommits = 0;
    {
        const auto tx = _builtGraph->openTransaction();
        const auto reader = tx.readGraph();
        const auto commitViews = reader.commits();
        numCommits = commitViews.size();
    }

    // Make a change to the graph
    {
        ChangeID changeID = newChange();
        auto res1 = _env->getDB().query("create (n:NEWNODE)", "simple", &_env->getMem(),
                               CommitHash::head(), changeID);

        ASSERT_TRUE(res1);
        auto res2 = _env->getDB().query("create (n:NEWNODE)-[e:NEWEDGE]-(m:NEWNODE)", "simple",
                               &_env->getMem(), CommitHash::head(), changeID);
        ASSERT_TRUE(res2);

        auto res3 = _env->getDB().query("change submit", "simple", &_env->getMem(),
                               CommitHash::head(), changeID);
        ASSERT_TRUE(res3);
    }

    auto VERIFY = [](const CommitView& commit) {
        ASSERT_FALSE(commit.history().journal().empty());
        const CommitJournal& journalOfInterest = commit.history().journal();

        ASSERT_TRUE(journalOfInterest.nodeWriteSet().size() == 3);
        ASSERT_TRUE(journalOfInterest.nodeWriteSet().contains(13));
        ASSERT_TRUE(journalOfInterest.nodeWriteSet().contains(14));
        ASSERT_TRUE(journalOfInterest.nodeWriteSet().contains(15));

        ASSERT_TRUE(journalOfInterest.edgeWriteSet().size() == 1);
        ASSERT_TRUE(journalOfInterest.edgeWriteSet().contains(13));
    };

    { // Check the commits
        const auto txRes = _env->getDB().getSystemManager().openTransaction(
            "simple", CommitHash::head(), ChangeID::head());
        ASSERT_TRUE(txRes.has_value());

        const auto& tx = txRes.value();
        const auto reader = tx.readGraph();
        const auto commitViews = reader.commits();

        // We should have an extra commit
        ASSERT_TRUE(commitViews.size() == numCommits + 1);
        const CommitView& commitOfInterest = commitViews.back();
        VERIFY(commitOfInterest);
    }

    dumpLoadSimpleDB();

    { // Check the commits of the dumped and loaded graph
        const auto tx = _loadedGraph->openTransaction();
        const auto reader = tx.readGraph();
        const auto commitViews = reader.commits();

        // We should have an extra commit
        ASSERT_TRUE(commitViews.size() == numCommits + 1);
        const CommitView& commitOfInterest = commitViews.back();
        VERIFY(commitOfInterest);
    }
}

int main(int argc, char** argv) {
    return turingTestMain(argc, argv, [] { testing::GTEST_FLAG(repeat) = 1; });
}

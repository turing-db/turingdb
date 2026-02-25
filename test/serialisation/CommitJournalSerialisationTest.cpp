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
#include "versioning/VersionController.h"

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
        _builtGraph = sysMan.createGraph("simpledb");
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
        auto changeRes = _env->getDB().getSystemManager().newChange(_builtGraph->getName());
        bioassert(changeRes, "invalid change");
        Change* change = changeRes.value();
        ChangeID changeID = change->id();
        return changeID;
    }
};

TEST_F(CommitJournalSerialisationTest, emptyOnCreation) {
    { // On creation
        const auto tx = _builtGraph->openTransaction();
        const CommitHash hash = tx.viewGraph().headCommitHash();
        const Commit* commit = _builtGraph->getVersionController().getCommitSafe(hash);
        EXPECT_NE(commit, nullptr);

        // Write sets should be empty when creating simple db
        while (commit) {
            const CommitJournal& journal = commit->history().journal();
            EXPECT_TRUE(journal.nodeWriteSet().empty());
            EXPECT_TRUE(journal.edgeWriteSet().empty());

            commit = commit->getPreviousCommit();
        }
    }

    dumpLoadSimpleDB();

    { // After dumping and loading
        const auto tx = _loadedGraph->openTransaction();
        const CommitHash hash = tx.viewGraph().headCommitHash();
        const Commit* commit = _builtGraph->getVersionController().getCommitSafe(hash);
        EXPECT_NE(commit, nullptr);

        // Write sets should be empty when creating simple db
        while (commit) {
            const CommitJournal& journal = commit->history().journal();
            EXPECT_TRUE(journal.nodeWriteSet().empty());
            EXPECT_TRUE(journal.edgeWriteSet().empty());
            commit = commit->getPreviousCommit();
        }
    }
}

TEST_F(CommitJournalSerialisationTest, createNodeThenLoad) {
    const auto& graphName = _builtGraph->getName();

    size_t numCommits = 0;
    size_t numNodes = 0;
    {
        const auto tx = _builtGraph->openTransaction();
        numNodes = tx.readGraph().getNodeCount();
        numCommits = _builtGraph->getVersionController().getNumCommits();
    }

    // Make a change to the graph
    {
        ChangeID changeID = newChange();
        auto res1 = _env->getDB().query("create (n:NEWNODE)", graphName, &_env->getMem(),
                                        CommitHash::head(), changeID);
        ASSERT_TRUE(res1);

        auto res2 = _env->getDB().query("change submit", graphName, &_env->getMem(),
                                        CommitHash::head(), changeID);
        ASSERT_TRUE(res2);
    }

    { // Check the commits
        const auto& controller = _builtGraph->getVersionController();
        const auto tx = _builtGraph->openTransaction();

        // We should have an extra commit
        ASSERT_TRUE(controller.getNumCommits() == numCommits + 1);
        const CommitData* commitOfInterest = tx.commitData().get();

        // This commit should contain an entry in the write set
        ASSERT_FALSE(commitOfInterest->history().journal().empty());
        const CommitJournal& journalOfInterest = commitOfInterest->history().journal();

        ASSERT_TRUE(journalOfInterest.nodeWriteSet().size() == 1);
        ASSERT_TRUE(journalOfInterest.nodeWriteSet().contains(numNodes));
    }

    dumpLoadSimpleDB();

    { // Check the commits of the dumped and loaded graph
        const auto& controller = _loadedGraph->getVersionController();
        const auto tx = _loadedGraph->openTransaction();

        // We should have an extra commit
        ASSERT_TRUE(controller.getNumCommits() == numCommits + 1);
        const CommitData* commitOfInterest = tx.commitData().get();
        // This commit should contain an entry in the write set
        ASSERT_FALSE(commitOfInterest->history().journal().empty());
        const CommitJournal& journalOfInterest = commitOfInterest->history().journal();

        ASSERT_TRUE(journalOfInterest.nodeWriteSet().size() == 1);
        ASSERT_TRUE(journalOfInterest.nodeWriteSet().contains(numNodes));
    }
}

TEST_F(CommitJournalSerialisationTest, createNodesAndEdgesThenLoad) {
    const auto& graphName = _builtGraph->getName();

    size_t numCommits = 0;
    size_t numNodes = 0;
    {
        const auto tx = _builtGraph->openTransaction();
        const auto& controller = _builtGraph->getVersionController();
        numCommits = controller.getNumCommits();
        numNodes = tx.readGraph().getNodeCount();
    }

    // Make a change to the graph
    {
        ChangeID changeID = newChange();
        auto res1 = _env->getDB().query("create (n:NEWNODE)", graphName, &_env->getMem(),
                                        CommitHash::head(), changeID);

        ASSERT_TRUE(res1);
        auto res2 = _env->getDB().query("create (n:NEWNODE)-[e:NEWEDGE]->(m:NEWNODE)", graphName,
                                        &_env->getMem(), CommitHash::head(), changeID);
        ASSERT_TRUE(res2);

        auto res3 = _env->getDB().query("change submit", graphName, &_env->getMem(),
                                        CommitHash::head(), changeID);
        ASSERT_TRUE(res3);
    }

    const auto VERIFY = [numNodes](const CommitData* commit) {
        ASSERT_FALSE(commit->history().journal().empty());
        const CommitJournal& journalOfInterest = commit->history().journal();

        ASSERT_TRUE(journalOfInterest.nodeWriteSet().size() == 3);
        ASSERT_TRUE(journalOfInterest.nodeWriteSet().contains(numNodes));
        ASSERT_TRUE(journalOfInterest.nodeWriteSet().contains(numNodes+1));
        ASSERT_TRUE(journalOfInterest.nodeWriteSet().contains(numNodes+2));

        ASSERT_TRUE(journalOfInterest.edgeWriteSet().size() == 1);
        ASSERT_TRUE(journalOfInterest.edgeWriteSet().contains(numNodes));
    };

    { // Check the commits
        const auto& controller = _builtGraph->getVersionController();

        const auto tx = _builtGraph->openTransaction();

        // We should have an extra commit
        ASSERT_TRUE(controller.getNumCommits() == numCommits + 1);
        const CommitData* commitOfInterest = tx.commitData().get();
        VERIFY(commitOfInterest);
    }

    dumpLoadSimpleDB();

    { // Check the commits of the dumped and loaded graph
        const auto& controller = _builtGraph->getVersionController();
        const auto tx = _loadedGraph->openTransaction();

        // We should have an extra commit
        ASSERT_TRUE(controller.getNumCommits() == numCommits + 1);
        const CommitData* commitOfInterest = tx.commitData().get();
        VERIFY(commitOfInterest);
    }
}

int main(int argc, char** argv) {
    return turingTestMain(argc, argv, [] { testing::GTEST_FLAG(repeat) = 1; });
}

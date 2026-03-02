#include "JobSystem.h"
#include "TuringTest.h"

#include "Graph.h"
#include "SimpleGraph.h"
#include "versioning/Change.h"
#include "versioning/CommitHash.h"
#include "versioning/Transaction.h"
#include "versioning/VersionController.h"
#include "dump/DumpResult.h"
#include "dump/GraphDumper.h"
#include "dump/GraphLoader.h"


using namespace db;
using namespace turing::test;

// After GraphLoader::load, all commits except the HEAD are registered as lightweight
// skeletons (no CommitData). Only the HEAD commit has its data fully loaded from disk.
// This fixture builds a graph, dumps it to disk, then loads it back so that
// commitA (last commit of simple graph) is a skeleton and commitB (HEAD) is fully loaded.
class LoadCommitDataTest : public TuringTest {
public:
    void initialize() override {
        _jobSystem = JobSystem::create();
        _originalPath = fs::Path {_outDir} / "testdb";

        // Build a graph with 2 commits in an in-memory environment
        {
            _dumpedGraph = Graph::create("testdb", _originalPath);
            SimpleGraph::createSimpleGraph(_dumpedGraph.get());

            // SimpleGraph::createSimpleGraph(graph);
            _initialHeadHash = _dumpedGraph->getHeadHash(); // last commit of simple graph (will become skeleton)
            _numInitialCommits = _dumpedGraph->getVersionController().getNumCommits();

            auto changePtr = _dumpedGraph->newChange();
            bioassert(changePtr->access().submit(*_jobSystem), "change submit failed");

            _newHeadHash = _dumpedGraph->getHeadHash(); // HEAD commit (will remain loaded)

            auto dumpRes = GraphDumper::dump(*_dumpedGraph, _dumpedGraph->getPath());
            bioassert(dumpRes, "dump failed");
        }


        // Load the graph back. The path must match _graphPath so that
        // Graph::loadCommit can locate commit directories on disk.
        _loadedGraph = Graph::create("loadedTestDB", _originalPath);
        auto loadRes = GraphLoader::load(_loadedGraph.get(), _dumpedGraph->getPath());
        bioassert(loadRes, "load failed");
    }

protected:
    std::unique_ptr<db::JobSystem> _jobSystem;
    CommitHash _initialHeadHash; // initial commit — skeleton after load
    CommitHash _newHeadHash;     // HEAD commit   — fully loaded after load
    size_t _numInitialCommits;
    fs::Path _originalPath;
    fs::Path _graphPath;
    std::unique_ptr<Graph> _dumpedGraph;
    std::unique_ptr<Graph> _loadedGraph;
};

TEST_F(LoadCommitDataTest, nonHeadCommitIsSkeletonAfterLoad) {
    const Commit* commit =
        _loadedGraph->getVersionController().getCommitSafe(_initialHeadHash);
    ASSERT_NE(commit, nullptr);
    EXPECT_FALSE(commit->hasData());
}

TEST_F(LoadCommitDataTest, headCommitHasDataAfterLoad) {
    const Commit* commit =
        _loadedGraph->getVersionController().getCommitSafe(_newHeadHash);
    ASSERT_NE(commit, nullptr);
    EXPECT_TRUE(commit->hasData());
}

TEST_F(LoadCommitDataTest, everyCommitIsSkeletonOtherThanHead) {
    const Commit* commit =
        _loadedGraph->getVersionController().getCommitSafe(_initialHeadHash);

    while (commit) {
        ASSERT_NE(commit, nullptr);
        EXPECT_FALSE(commit->hasData());
        commit = commit->getPreviousCommit();
    }
}

TEST_F(LoadCommitDataTest, oneMoreCommitExistsAfterLoad) {
    EXPECT_EQ(_loadedGraph->getVersionController().getNumCommits(), _numInitialCommits + 1);
}

TEST_F(LoadCommitDataTest, loadCommitHydratesSkeleton) {
    const auto res = _loadedGraph->loadCommit(_initialHeadHash);
    ASSERT_TRUE(res) << res.error().fmtMessage();

    const Commit* commit =
        _loadedGraph->getVersionController().getCommitSafe(_initialHeadHash);
    ASSERT_NE(commit, nullptr);
    EXPECT_TRUE(commit->hasData());
}

TEST_F(LoadCommitDataTest, loadAlreadyLoadedCommitIsNoOp) {
    // Loading the HEAD commit (already loaded) should succeed silently
    const auto res = _loadedGraph->loadCommit(_newHeadHash);
    EXPECT_TRUE(res) << res.error().fmtMessage();

    const Commit* commit =
        _loadedGraph->getVersionController().getCommitSafe(_newHeadHash);
    ASSERT_NE(commit, nullptr);
    EXPECT_TRUE(commit->hasData());
}

TEST_F(LoadCommitDataTest, loadNonExistentHashFails) {
    const CommitHash bogusHash {0xdeadbeefcafeULL};
    const auto res = _loadedGraph->loadCommit(bogusHash);
    EXPECT_FALSE(res);
    EXPECT_EQ(res.error().getType(), DumpErrorType::COMMIT_HASH_NOT_FOUND);
}

TEST_F(LoadCommitDataTest, openTransactionAfterLoadCommit) {
    const auto loadRes = _loadedGraph->loadCommit(_initialHeadHash);
    ASSERT_TRUE(loadRes) << loadRes.error().fmtMessage();

    const FrozenCommitTx tx = _loadedGraph->openTransaction(_initialHeadHash);
    EXPECT_TRUE(tx.isValid());
}

int main(int argc, char** argv) {
    return turingTestMain(argc, argv, [] { testing::GTEST_FLAG(repeat) = 1; });
}

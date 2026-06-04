#include "TuringTest.h"

#include <string_view>

#include "TuringDB.h"
#include "QueryConfig.h"
#include "Graph.h"
#include "SimpleGraph.h"
#include "SystemManager.h"
#include "QueryStatus.h"
#include "versioning/Change.h"
#include "versioning/ChangeID.h"
#include "versioning/CommitHash.h"
#include "dataframe/Dataframe.h"
#include "dump/GraphDumper.h"

#include "TuringTestEnv.h"

using namespace turing::test;

// Fixture that builds a graph, dumps it to disk, creates a synced environment,
// and loads the graph via the LOAD GRAPH query. After loading, commitA (last
// commit of simple graph) is a skeleton and commitB (HEAD) is fully loaded.
class LoadCommitTest : public TuringTest {
public:
    void initialize() override {
        const auto turingDir = fs::Path {_outDir} / "turing";

        // Step 1: Create the synced environment first so that TuringDB::init()
        // creates the graphs/ directory before we dump into it.
        _env = TuringTestEnv::createSyncedOnDisk(turingDir);
        _db = &_env->getDB();

        // Step 2: Build graph with 2 commits in a separate in-memory environment.
        {
            auto buildEnv = TuringTestEnv::create(fs::Path {_outDir} / "build");
            Graph* graph = buildEnv->getSystemManager().createGraph("testdb");
            // Pass false so the graph keeps the "testdb" name — LOAD GRAPH testdb
            // relies on it. Most callers use the default (rename to "simpledb").
            SimpleGraph::createSimpleGraph(graph, false);
            _initialHeadHash = graph->getHeadHash(); // last commit of simple graph (skeleton after load)

            auto changeRes = buildEnv->getSystemManager().newChange("testdb");
            bioassert(changeRes, "create change failed");
            const ChangeID chid = changeRes.value()->id();

            QueryConfig buildQueryConfig;
            QueryCallbacks submitCallbacks;
            const QueryState submitState("testdb", &buildEnv->getMem(), &buildQueryConfig, &submitCallbacks, CommitHash::head(), chid);
            const auto submitRes = buildEnv->getDB().query("CHANGE SUBMIT", submitState);
            bioassert(submitRes, "change submit failed");
            _newHeadHash = graph->getHeadHash(); // HEAD commit (loaded after LOAD GRAPH)

            // Step 3: Dump to the graphs directory where LOAD GRAPH expects it.
            const auto graphDir = turingDir / "graphs" / "testdb";
            auto dumpRes = GraphDumper::dump(graph, graphDir);
            bioassert(dumpRes, "dump failed");
        }

        // Step 4: Load the graph via query so the SystemManager registers it.
        QueryCallbacks loadCallbacks;
        const QueryState loadState("default", &_env->getMem(), &_queryConfig, &loadCallbacks);
        auto loadRes = _db->query("LOAD GRAPH testdb", loadState);
        bioassert(loadRes, "LOAD GRAPH failed");
    }

protected:
    CommitHash _initialHeadHash; // initial commit — skeleton after LOAD GRAPH
    CommitHash _newHeadHash;     // HEAD commit   — fully loaded after LOAD GRAPH
    std::unique_ptr<TuringTestEnv> _env;
    TuringDB* _db {nullptr};
    QueryConfig _queryConfig;

    QueryStatus query(std::string_view q,
                      CommitHash hash = CommitHash::head()) {
        QueryCallbacks callbacks;
        const QueryState state("testdb", &_env->getMem(), &_queryConfig, &callbacks, hash);
        return _db->query(q, state);
    }

    // Build the LOAD COMMIT query string for a given hash (hex-encoded).
    std::string loadCommitQuery(CommitHash hash) {
        return fmt::format("LOAD COMMIT '{:x}'", hash.get());
    }
};

TEST_F(LoadCommitTest, queryingUnloadedCommitGivesHelpfulError) {
    auto result = query("MATCH (n) RETURN n", _initialHeadHash);

    EXPECT_FALSE(result);
    EXPECT_EQ(result.getStatus(), QueryStatus::Status::COMMIT_NOT_LOADED);
    EXPECT_NE(result.getError().find("LOAD COMMIT"), std::string::npos);
}

TEST_F(LoadCommitTest, queryingLoadedHeadSucceeds) {
    auto result = query("MATCH (n) RETURN n", _newHeadHash);
    EXPECT_TRUE(result);
}

TEST_F(LoadCommitTest, loadCommitQuerySucceeds) {
    auto result = query(loadCommitQuery(_initialHeadHash));
    EXPECT_TRUE(result);
}

TEST_F(LoadCommitTest, canQueryAfterLoadCommit) {
    ASSERT_TRUE(query(loadCommitQuery(_initialHeadHash)));

    auto result = query("MATCH (n) RETURN n", _initialHeadHash);
    EXPECT_TRUE(result);
}

TEST_F(LoadCommitTest, loadAlreadyLoadedCommitIsNoOp) {
    // HEAD commit is already loaded — loading it again should silently succeed.
    auto result = query(loadCommitQuery(_newHeadHash));
    EXPECT_TRUE(result);
}

TEST_F(LoadCommitTest, loadInvalidHashStringFails) {
    auto result = query("LOAD COMMIT 'notahash'");

    EXPECT_FALSE(result);
    EXPECT_EQ(result.getStatus(), QueryStatus::Status::EXEC_ERROR);
}

TEST_F(LoadCommitTest, loadNonExistentHashFails) {
    // Valid hex but no such commit exists in the graph.
    auto result = query("LOAD COMMIT 'deadbeef00000000'");

    EXPECT_FALSE(result);
    EXPECT_EQ(result.getStatus(), QueryStatus::Status::EXEC_ERROR);
}

int main(int argc, char** argv) {
    return turing::test::turingTestMain(argc, argv, [] {
        testing::GTEST_FLAG(repeat) = 1;
    });
}

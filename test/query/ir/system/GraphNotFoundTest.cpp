#include <gtest/gtest.h>

#include <memory>
#include <string>
#include <string_view>

#include "QueryInterpreterV3.h"
#include "QueryStatus.h"

#include "Graph.h"
#include "SimpleGraph.h"
#include "SystemAccessor.h"
#include "SystemManager.h"
#include "versioning/ChangeID.h"
#include "versioning/CommitHash.h"

#include "StringRowSink.h"
#include "TuringTest.h"
#include "TuringTestEnv.h"

using namespace db;
using namespace turing::test;

class GraphNotFoundTest : public TuringTest {
public:
    void initialize() override {
        _turingDir = fs::Path {_outDir} / "turing";
        startDatabase();

        SystemAccessor system = _env->getSystemManager().accessUnique();
        Graph* graph = system.createGraph(_graphName);
        SimpleGraph::createSimpleGraph(graph);
    }

protected:
    void startDatabase() {
        _interpreter.reset();
        _env.reset();

        _env = TuringTestEnv::createSyncedOnDisk(_turingDir);
        _interpreter = std::make_unique<QueryInterpreterV3>(&_env->getSystemManager(), &_env->getMem(), &_env->getCompilerContext());
    }

    void runQuery(QueryStatus& status,
                  std::string_view query,
                  std::string_view graphName,
                  CommitHash commit,
                  ChangeID change) {
        StringRowSink sink;
        _interpreter->execute(status, query, graphName, commit, change, &sink);
    }

    void runQuery(QueryStatus& status, std::string_view query, std::string_view graphName) {
        runQuery(status, query, graphName, CommitHash::head(), ChangeID::head());
    }

    const std::string _graphName = "simpledb";
    fs::Path _turingDir;
    std::unique_ptr<TuringTestEnv> _env;
    std::unique_ptr<QueryInterpreterV3> _interpreter;
};

TEST_F(GraphNotFoundTest, aGraphThatDoesNotExistIsNamed) {
    QueryStatus status;
    runQuery(status, "MATCH (n) RETURN count(n)", "not_a_graph");

    EXPECT_EQ(status.getStatus(), QueryStatus::Status::GRAPH_NOT_FOUND);
    EXPECT_EQ(status.getError(), "Graph 'not_a_graph' does not exist");
}

TEST_F(GraphNotFoundTest, aGraphOnDiskButNotLoadedSaysToLoadIt) {
    startDatabase();

    QueryStatus status;
    runQuery(status, "MATCH (n) RETURN count(n)", _graphName);

    EXPECT_EQ(status.getStatus(), QueryStatus::Status::GRAPH_NOT_FOUND);
    EXPECT_EQ(status.getError(), "Graph 'simpledb' is on disk but not loaded - use LOAD GRAPH simpledb to load it");

    QueryStatus loadStatus;
    runQuery(loadStatus, "LOAD GRAPH simpledb", "default");
    ASSERT_TRUE(loadStatus.isOk()) << loadStatus.getError();

    QueryStatus loadedStatus;
    runQuery(loadedStatus, "MATCH (n) RETURN count(n)", _graphName);
    ASSERT_TRUE(loadedStatus.isOk()) << loadedStatus.getError();
}

TEST_F(GraphNotFoundTest, aPathOutsideTheGraphsDirectoryIsNotAGraphOnDisk) {
    QueryStatus status;
    runQuery(status, "MATCH (n) RETURN count(n)", "..");

    EXPECT_EQ(status.getStatus(), QueryStatus::Status::GRAPH_NOT_FOUND);
    EXPECT_EQ(status.getError(), "Graph '..' does not exist");
}

TEST_F(GraphNotFoundTest, aChangeThatDoesNotExistHasAMessage) {
    QueryStatus status;
    runQuery(status, "MATCH (n) RETURN count(n)", _graphName, CommitHash::head(), ChangeID {12345});

    EXPECT_EQ(status.getStatus(), QueryStatus::Status::CHANGE_NOT_FOUND);
    EXPECT_EQ(status.getError(), "Change '12345' does not exist");
}

TEST_F(GraphNotFoundTest, aCommitThatDoesNotExistHasAMessage) {
    QueryStatus status;
    runQuery(status, "MATCH (n) RETURN count(n)", _graphName, CommitHash {0x12345}, ChangeID::head());

    EXPECT_EQ(status.getStatus(), QueryStatus::Status::COMMIT_NOT_FOUND);
    EXPECT_EQ(status.getError(), "Commit '12345' does not exist");
}

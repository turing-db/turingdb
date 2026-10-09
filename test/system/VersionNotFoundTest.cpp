#include <gtest/gtest.h>

#include <memory>
#include <string>
#include <string_view>

#include <spdlog/fmt/fmt.h>

#include "Graph.h"
#include "SimpleGraph.h"
#include "SystemAccessor.h"
#include "SystemManager.h"
#include "versioning/Change.h"
#include "versioning/ChangeAccessor.h"
#include "versioning/ChangeID.h"
#include "versioning/ChangeResult.h"
#include "versioning/CommitHash.h"
#include "versioning/Transaction.h"

#include "TuringTest.h"
#include "TuringTestEnv.h"

using namespace db;
using namespace turing::test;

namespace {

constexpr std::string_view GRAPH_NAME = "simpledb";

}

class VersionNotFoundTest : public TuringTest {
public:
    void initialize() override {
        _turingDir = fs::Path {_outDir} / "turing";
        _env = TuringTestEnv::createSyncedOnDisk(_turingDir);

        SystemAccessor system = _env->getSystemManager().accessUnique();
        Graph* graph = system.createGraph(GRAPH_NAME);
        SimpleGraph::createSimpleGraph(graph);
    }

protected:
    void expectOpenError(CommitHash commit,
                         ChangeID change,
                         ChangeErrorType expectedType,
                         std::string_view expectedMessage) {
        SystemAccessor system = _env->getSystemManager().accessUnique();
        const ChangeResult<Transaction> result = system.openTransaction(GRAPH_NAME, commit, change);

        ASSERT_FALSE(result);
        EXPECT_EQ(result.error().getType(), expectedType);
        EXPECT_EQ(result.error().fmtMessage(), expectedMessage);
    }

    fs::Path _turingDir;
    std::unique_ptr<TuringTestEnv> _env;
};

TEST_F(VersionNotFoundTest, aChangeIsNamedInDecimal) {
    expectOpenError(CommitHash::head(), ChangeID {16}, ChangeErrorType::CHANGE_NOT_FOUND, "Change '16' does not exist");
}

TEST_F(VersionNotFoundTest, aCommitIsNamedInHexadecimal) {
    expectOpenError(CommitHash {0xabc}, ChangeID::head(), ChangeErrorType::COMMIT_NOT_FOUND, "Commit 'abc' does not exist");
}

TEST_F(VersionNotFoundTest, aCommitReadInAChangeIsNamed) {
    ChangeID changeID;
    {
        SystemAccessor system = _env->getSystemManager().accessUnique();
        const ChangeResult<Change*> change = system.newChange(GRAPH_NAME);
        ASSERT_TRUE(change);
        changeID = change.value()->id();
    }

    expectOpenError(CommitHash {0xabc}, changeID, ChangeErrorType::COMMIT_NOT_FOUND, "Commit 'abc' does not exist");
}

TEST_F(VersionNotFoundTest, aCommitNotLoadedIsNamedInTheLoadCommitItSuggests) {
    CommitHash skeletonHash;
    {
        SystemAccessor system = _env->getSystemManager().accessUnique();
        const Graph* graph = system.getGraph(GRAPH_NAME);
        skeletonHash = graph->getHeadHash();

        const ChangeResult<Change*> change = system.newChange(GRAPH_NAME);
        ASSERT_TRUE(change);

        ChangeAccessor accessor = change.value()->access();
        ASSERT_TRUE(system.submitChange(accessor));
        ASSERT_TRUE(system.dumpGraph(GRAPH_NAME));
    }

    _env.reset();
    _env = TuringTestEnv::createSyncedOnDisk(_turingDir);
    {
        SystemAccessor system = _env->getSystemManager().accessUnique();
        ASSERT_TRUE(system.loadGraph(GRAPH_NAME));
    }

    const std::string hash = fmt::format("{}", skeletonHash);
    expectOpenError(skeletonHash,
                    ChangeID::head(),
                    ChangeErrorType::COMMIT_NOT_LOADED,
                    fmt::format("Commit '{0}' is not loaded to memory - use LOAD COMMIT '{0}' to load it", hash));
}

TEST_F(VersionNotFoundTest, getChangeNamesTheChange) {
    SystemAccessor system = _env->getSystemManager().accessUnique();
    const Graph* graph = system.getGraph(GRAPH_NAME);
    const ChangeResult<Change*> result = system.getChange(graph, ChangeID {999});

    ASSERT_FALSE(result);
    EXPECT_EQ(result.error().getType(), ChangeErrorType::CHANGE_NOT_FOUND);
    EXPECT_EQ(result.error().fmtMessage(), "Change '999' does not exist");
}

TEST_F(VersionNotFoundTest, deleteChangeNamesTheChange) {
    SystemAccessor system = _env->getSystemManager().accessUnique();
    const ChangeResult<Change*> change = system.newChange(GRAPH_NAME);
    ASSERT_TRUE(change);

    ChangeAccessor accessor = change.value()->access();
    const ChangeResult<void> result = system.deleteChange(accessor, ChangeID {999});

    ASSERT_FALSE(result);
    EXPECT_EQ(result.error().getType(), ChangeErrorType::CHANGE_NOT_FOUND);
    EXPECT_EQ(result.error().fmtMessage(), "Change '999' does not exist");
}

TEST_F(VersionNotFoundTest, submitChangeNamesTheChange) {
    SystemAccessor system = _env->getSystemManager().accessUnique();
    Graph* graph = system.getGraph(GRAPH_NAME);
    const std::unique_ptr<Change> unregistered = graph->newChange();

    ChangeAccessor accessor = unregistered->access();
    const ChangeResult<void> result = system.submitChange(accessor);

    ASSERT_FALSE(result);
    EXPECT_EQ(result.error().getType(), ChangeErrorType::CHANGE_NOT_FOUND);
    EXPECT_EQ(result.error().fmtMessage(), fmt::format("Change '{}' does not exist", unregistered->id()));
}

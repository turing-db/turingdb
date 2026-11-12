#include <gtest/gtest.h>

#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <memory>
#include <string>
#include <string_view>
#include <thread>
#include <vector>

#include "SystemAccessor.h"
#include "SystemManager.h"
#include "dump/LoadGraphResult.h"

#include "TuringTest.h"
#include "TuringTestEnv.h"

using namespace db;
using namespace turing::test;

class LoadGraphShowsLoadingTest : public TuringTest {
protected:
    void initialize() override {
        _turingDir = fs::Path {_outDir} / "turing";
        _env = TuringTestEnv::createSyncedOnDisk(_turingDir);
    }

    void terminate() override {
        releaseBlockedLoad();
    }

    // GraphLoader opens the graph's "info" file first. As a FIFO, that open blocks
    // until a writer opens it, which holds LOAD GRAPH between reserve and publish.
    void startBlockedLoad(std::string_view name) {
        const fs::Path graphDir = _env->getConfig().getGraphsDir() / name;
        ASSERT_EQ(::mkdir(graphDir.c_str(), 0755), 0);

        _fifoPath = graphDir / "info";
        ASSERT_EQ(::mkfifo(_fifoPath.c_str(), 0644), 0);

        SystemManager& systemManager = _env->getSystemManager();
        const std::string graphName {name};
        _loader = std::thread([this, &systemManager, graphName] {
            SystemAccessor system = systemManager.accessShared();
            const LoadGraphResult<Graph*> result = system.loadGraph(graphName);
            _loadSucceeded = result.has_value();
            _loadFinished = true;
        });
    }

    bool waitUntilLoading(std::string_view name) {
        const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);

        while (std::chrono::steady_clock::now() < deadline && !_loadFinished) {
            const SystemAccessor system = _env->getSystemManager().accessShared();
            if (system.isGraphLoading(name)) {
                return true;
            }

            std::this_thread::sleep_for(std::chrono::milliseconds(1));
        }

        return false;
    }

    void releaseBlockedLoad() {
        if (!_loader.joinable()) {
            return;
        }

        while (!_loadFinished) {
            const int fd = ::open(_fifoPath.c_str(), O_WRONLY | O_NONBLOCK);
            if (fd >= 0) {
                ::close(fd);
                break;
            }

            std::this_thread::sleep_for(std::chrono::milliseconds(1));
        }

        _loader.join();
    }

    fs::Path _turingDir;
    fs::Path _fifoPath;
    std::unique_ptr<TuringTestEnv> _env;
    std::thread _loader;
    std::atomic<bool> _loadFinished {false};
    std::atomic<bool> _loadSucceeded {false};
};

TEST_F(LoadGraphShowsLoadingTest, aGraphIsLoadingWhileLoadGraphRuns) {
    startBlockedLoad("pending");

    const bool sawLoading = waitUntilLoading("pending");
    EXPECT_TRUE(sawLoading);

    if (sawLoading) {
        const SystemAccessor system = _env->getSystemManager().accessShared();
        EXPECT_EQ(system.getGraph("pending"), nullptr);

        std::vector<std::string> names;
        system.listAvailableGraphs(names);
        EXPECT_NE(std::ranges::find(names, "pending"), names.end());
    }

    releaseBlockedLoad();

    EXPECT_FALSE(_loadSucceeded);

    const SystemAccessor system = _env->getSystemManager().accessShared();
    EXPECT_FALSE(system.isGraphLoading("pending"));
    EXPECT_EQ(system.getGraph("pending"), nullptr);
}

TEST_F(LoadGraphShowsLoadingTest, aSecondLoadGraphDuringTheLoadKeepsTheFirstLoading) {
    startBlockedLoad("pending");

    const bool sawLoading = waitUntilLoading("pending");
    EXPECT_TRUE(sawLoading);

    if (sawLoading) {
        SystemAccessor system = _env->getSystemManager().accessShared();
        const LoadGraphResult<Graph*> second = system.loadGraph("pending");

        EXPECT_FALSE(second);
        if (!second) {
            EXPECT_EQ(second.error().getType(), LoadGraphErrorType::GRAPH_EXISTS);
        }
        EXPECT_TRUE(system.isGraphLoading("pending"));
    }

    releaseBlockedLoad();

    const SystemAccessor system = _env->getSystemManager().accessShared();
    EXPECT_FALSE(system.isGraphLoading("pending"));
}

TEST_F(LoadGraphShowsLoadingTest, aLoadedGraphIsNotLoading) {
    {
        SystemAccessor system = _env->getSystemManager().accessUnique();
        ASSERT_NE(system.createGraph("saved"), nullptr);
    }

    _env.reset();
    _env = TuringTestEnv::createSyncedOnDisk(_turingDir);

    SystemAccessor system = _env->getSystemManager().accessUnique();
    const LoadGraphResult<Graph*> result = system.loadGraph("saved");
    ASSERT_TRUE(result);

    EXPECT_EQ(system.getGraph("saved"), result.value());
    EXPECT_FALSE(system.isGraphLoading("saved"));
}

TEST_F(LoadGraphShowsLoadingTest, aMissingGraphIsNotLoadingAfterLoadGraphFails) {
    SystemAccessor system = _env->getSystemManager().accessUnique();
    const LoadGraphResult<Graph*> result = system.loadGraph("missing");

    ASSERT_FALSE(result);
    EXPECT_EQ(result.error().getType(), LoadGraphErrorType::FILE_ERROR);
    EXPECT_FALSE(system.isGraphLoading("missing"));

    std::vector<std::string> names;
    system.listAvailableGraphs(names);
    EXPECT_EQ(std::ranges::find(names, "missing"), names.end());

    const LoadGraphResult<Graph*> again = system.loadGraph("missing");
    ASSERT_FALSE(again);
    EXPECT_EQ(again.error().getType(), LoadGraphErrorType::FILE_ERROR);
}

int main(int argc, char** argv) {
    return turing::test::turingTestMain(argc, argv);
}

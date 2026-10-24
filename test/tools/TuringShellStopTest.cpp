#include <gtest/gtest.h>

#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>

#include <chrono>
#include <future>
#include <memory>
#include <string>
#include <string_view>
#include <thread>

#include "LineNoiseHandle.h"
#include "SystemAccessor.h"
#include "SystemManager.h"
#include "TuringShell.h"
#include "TuringTest.h"
#include "TuringTestEnv.h"

using namespace db;
using namespace turing::test;

class TuringShellStopTest : public TuringTest {
protected:
    void initialize() override {
        _env = TuringTestEnv::create(fs::Path {_outDir} / "turing");

        const char* home = getenv("HOME");
        _savedHome = home ? home : "";
        setenv("HOME", _outDir.c_str(), 1);

        _savedStdin = dup(STDIN_FILENO);
        ASSERT_EQ(pipe(_stdinPipe), 0);
        dup2(_stdinPipe[0], STDIN_FILENO);
        clearerr(stdin);
    }

    void terminate() override {
        dup2(_savedStdin, STDIN_FILENO);
        close(_savedStdin);
        close(_stdinPipe[0]);
        closeStdinWriter();
        clearerr(stdin);

        setenv("HOME", _savedHome.c_str(), 1);
    }

    void writeStdin(std::string_view text) {
        ASSERT_EQ(write(_stdinPipe[1], text.data(), text.size()), static_cast<ssize_t>(text.size()));
    }

    void closeStdinWriter() {
        if (_stdinPipe[1] != -1) {
            close(_stdinPipe[1]);
            _stdinPipe[1] = -1;
        }
    }

    bool waitForGraph(std::string_view name) {
        const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);

        while (std::chrono::steady_clock::now() < deadline) {
            {
                SystemAccessor system = _env->getSystemManager().accessShared();
                if (system.getGraph(name) != nullptr) {
                    return true;
                }
            }

            std::this_thread::sleep_for(std::chrono::milliseconds(10));
        }

        return false;
    }

    void expectLoopEnds(std::future<void>& loop) {
        const bool ended = loop.wait_for(std::chrono::seconds(5)) == std::future_status::ready;
        EXPECT_TRUE(ended) << "the shell loop did not end after stop()";

        if (!ended) {
            closeStdinWriter();
        }
    }

    std::unique_ptr<TuringTestEnv> _env;
    LineNoiseHandle _lineNoiseHandle;
    std::string _savedHome;
    int _savedStdin {-1};
    int _stdinPipe[2] {-1, -1};
};

TEST_F(TuringShellStopTest, aStopBeforeTheLoopEndsItAtOnce) {
    TuringShell shell(_env->getDB(), &_env->getMem(), &_lineNoiseHandle);
    shell.stop();

    std::future<void> loop = std::async(std::launch::async, [&shell] { shell.startLoop(); });
    expectLoopEnds(loop);
}

TEST_F(TuringShellStopTest, linesWrittenTogetherAllRunBeforeAStopEndsTheLoop) {
    TuringShell shell(_env->getDB(), &_env->getMem(), &_lineNoiseHandle);
    std::future<void> loop = std::async(std::launch::async, [&shell] { shell.startLoop(); });

    writeStdin("CREATE GRAPH first\nCREATE GRAPH second\n");
    EXPECT_TRUE(waitForGraph("first"));
    EXPECT_TRUE(waitForGraph("second"));

    shell.stop();
    expectLoopEnds(loop);
}

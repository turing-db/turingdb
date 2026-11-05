#include <gtest/gtest.h>

#include <chrono>
#include <future>
#include <string_view>
#include <thread>

#include "ShellTestFixture.h"
#include "SystemAccessor.h"
#include "SystemManager.h"

using namespace db;
using namespace turing::test;

class TuringShellStopTest : public ShellTestFixture {
protected:
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
};

TEST_F(TuringShellStopTest, aStopBeforeTheLoopEndsItAtOnce) {
    TuringShell shell(_env->getDB(), &_env->getMem(), &_lineNoiseHandle);
    shell.stop();

    std::future<void> loop = std::async(std::launch::async, [&shell] { shell.startLoop(); });
    expectLoopEnds(shell, loop);
}

TEST_F(TuringShellStopTest, linesWrittenTogetherAllRunBeforeAStopEndsTheLoop) {
    TuringShell shell(_env->getDB(), &_env->getMem(), &_lineNoiseHandle);
    std::future<void> loop = std::async(std::launch::async, [&shell] { shell.startLoop(); });

    writeStdin("CREATE GRAPH first\nCREATE GRAPH second\n");
    EXPECT_TRUE(waitForGraph("first"));
    EXPECT_TRUE(waitForGraph("second"));

    shell.stop();
    expectLoopEnds(shell, loop);
}

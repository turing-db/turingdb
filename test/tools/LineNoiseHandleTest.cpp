#include <gtest/gtest.h>

#include <unistd.h>

#include <chrono>
#include <future>
#include <memory>
#include <thread>

#include <spdlog/spdlog.h>

#include "LineNoiseHandle.h"
#include "LogSetup.h"

#include "BioAssert.h"

using namespace db;

namespace {

class PipedStdin {
public:
    PipedStdin() {
        _savedStdin = dup(STDIN_FILENO);

        const int created = pipe(_pipe);
        bioassert(created == 0, "Failed to create the stdin pipe");

        dup2(_pipe[0], STDIN_FILENO);
    }

    ~PipedStdin() {
        dup2(_savedStdin, STDIN_FILENO);
        close(_savedStdin);
        close(_pipe[0]);
        close(_pipe[1]);
    }

private:
    int _savedStdin {-1};
    int _pipe[2] {-1, -1};
};

}

// A server started under a supervisor that keeps stdin open as a pipe logs from its
// worker threads while the shell waits for a line nobody will type
TEST(LineNoiseHandleTest, LogsWhileEditingFromAPipe) {
    const PipedStdin pipedStdin;

    LineNoiseHandle handle;
    handle.initProtectedLogger(std::make_shared<LogSetup::ConsoleSink>());
    handle.startEditing("turing> ");

    std::promise<void> logged;
    std::future<void> loggedFuture = logged.get_future();
    std::thread([&logged]() {
        spdlog::info("logged while editing");
        logged.set_value();
    }).detach();

    EXPECT_EQ(loggedFuture.wait_for(std::chrono::seconds(5)), std::future_status::ready);

    handle.stopEditing();
}

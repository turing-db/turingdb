#include <gtest/gtest.h>

#include <stdlib.h>
#include <sys/wait.h>
#include <unistd.h>

#include <functional>
#include <memory>
#include <string>

#include "DemonReadiness.h"

namespace {

pid_t forkDemon(DemonReadiness* readiness, const std::function<void()>& demon) {
    const pid_t pid = ::fork();
    if (pid == 0) {
        readiness->keepWriteEnd();
        demon();
        ::_exit(EXIT_SUCCESS);
    }

    readiness->keepReadEnd();
    return pid;
}

int waitForExit(pid_t pid) {
    int status = 0;
    ::waitpid(pid, &status, 0);
    return status;
}

}

TEST(DemonReadinessTest, aReadyDemonIsReportedReady) {
    DemonReadiness readiness;
    readiness.open();

    const pid_t demon = forkDemon(&readiness, [&readiness] { readiness.notifyReady(); });

    std::string error;
    EXPECT_EQ(readiness.waitForDemon(error), DemonStartResult::Ready);
    EXPECT_TRUE(error.empty());

    waitForExit(demon);
}

TEST(DemonReadinessTest, aFailingDemonReportsItsError) {
    DemonReadiness readiness;
    readiness.open();

    const std::string expected = "Could not start the visualizer on 127.0.0.1:8104: bind: Address already in use";
    const pid_t demon = forkDemon(&readiness, [&readiness, &expected] { readiness.notifyFailure(expected); });

    std::string error;
    EXPECT_EQ(readiness.waitForDemon(error), DemonStartResult::Failed);
    EXPECT_EQ(error, expected);

    waitForExit(demon);
}

TEST(DemonReadinessTest, aDemonThatExitsWithoutReportingIsReportedDead) {
    DemonReadiness readiness;
    readiness.open();

    const pid_t demon = forkDemon(&readiness, [] {});

    std::string error;
    EXPECT_EQ(readiness.waitForDemon(error), DemonStartResult::Died);
    EXPECT_TRUE(error.empty());

    waitForExit(demon);
}

TEST(DemonReadinessTest, anErrorLargerThanTheSocketBufferArrivesWhole) {
    DemonReadiness readiness;
    readiness.open();

    const std::string expected(1 << 20, 'e');
    const pid_t demon = forkDemon(&readiness, [&readiness, &expected] { readiness.notifyFailure(expected); });

    std::string error;
    EXPECT_EQ(readiness.waitForDemon(error), DemonStartResult::Failed);
    EXPECT_EQ(error, expected);

    waitForExit(demon);
}

TEST(DemonReadinessTest, aDemonOutlivesAParentThatStoppedWaiting) {
    auto readiness = std::make_unique<DemonReadiness>();
    readiness->open();

    int parentGone[2] {-1, -1};
    ASSERT_EQ(::pipe(parentGone), 0);

    DemonReadiness* demonReadiness = readiness.get();
    const pid_t demon = forkDemon(demonReadiness, [demonReadiness, &parentGone] {
        char byte = 0;
        if (::read(parentGone[0], &byte, 1) != 1) {
            ::_exit(EXIT_FAILURE);
        }

        demonReadiness->notifyReady();
    });

    readiness.reset();

    const char byte = 0;
    ASSERT_EQ(::write(parentGone[1], &byte, 1), 1);

    const int status = waitForExit(demon);
    EXPECT_TRUE(WIFEXITED(status));
    EXPECT_EQ(WEXITSTATUS(status), EXIT_SUCCESS);

    ::close(parentGone[0]);
    ::close(parentGone[1]);
}

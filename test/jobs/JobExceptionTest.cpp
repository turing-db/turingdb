#include <stdlib.h>

#include <atomic>
#include <chrono>
#include <future>
#include <stdexcept>
#include <string_view>
#include <thread>

#include "ExitWithTestOutcome.h"
#include "TuringTest.h"

#include "JobGroup.h"
#include "JobSystem.h"
#include "TuringException.h"

using namespace turing::test;
using namespace db;

namespace {

constexpr unsigned TIMEOUT_SECONDS = 60;
constexpr size_t THREAD_COUNT = 4;
constexpr size_t JOB_COUNT = 64;
constexpr size_t THROWING_JOB = 17;
constexpr std::string_view MESSAGE = "job failed";
constexpr std::chrono::milliseconds FIRST_JOB_DELAY {100};

template <typename Exception, typename Wait>
void expectRethrow(Wait wait, std::string_view message) {
    try {
        wait();
        ADD_FAILURE() << "the wait returned, expected it to throw: " << message;
    } catch (const Exception& e) {
        EXPECT_EQ(std::string_view {e.what()}, message);
    }
}

template <typename Exception>
void futureRethrows() {
    JobSystem jobSystem(THREAD_COUNT);
    jobSystem.init();

    std::atomic<size_t> completed {0};
    Future<void> failing;

    for (size_t i = 0; i < JOB_COUNT; i++) {
        if (i == THROWING_JOB) {
            failing = jobSystem.submit<void>([] {
                throw Exception(std::string {MESSAGE});
            });
        } else {
            jobSystem.submit<void>([&completed] {
                completed++;
            });
        }
    }

    expectRethrow<Exception>([&failing] { failing.get(); }, MESSAGE);

    jobSystem.wait();
    EXPECT_EQ(completed.load(), JOB_COUNT - 1);

    jobSystem.terminate();
}

void futureRethrowsAJobRunOnTheSubmittingThread() {
    JobSystem jobSystem(1);
    jobSystem.init();

    std::promise<void> started;
    std::promise<void> release;
    const std::shared_future<void> released = release.get_future().share();

    jobSystem.submit<void>([&started, released] {
        started.set_value();
        released.wait();
    });

    started.get_future().wait();

    Future<void> failing = jobSystem.submit<void>([] {
        throw TuringException(std::string {MESSAGE});
    });

    release.set_value();

    expectRethrow<TuringException>([&failing] { failing.get(); }, MESSAGE);

    jobSystem.terminate();
}

void groupWaitRethrows() {
    JobSystem jobSystem(THREAD_COUNT);
    jobSystem.init();

    std::atomic<size_t> completed {0};

    {
        JobGroup group = jobSystem.newGroup();

        for (size_t i = 0; i < JOB_COUNT; i++) {
            if (i == THROWING_JOB) {
                group.submit<void>([] {
                    throw TuringException(std::string {MESSAGE});
                });
            } else {
                group.submit<void>([&completed] {
                    completed++;
                });
            }
        }

        expectRethrow<TuringException>([&group] { group.wait(); }, MESSAGE);
        EXPECT_EQ(completed.load(), JOB_COUNT - 1);
    }

    jobSystem.terminate();
}

void groupWaitRethrowsTheFirstSubmittedFailure() {
    JobSystem jobSystem(THREAD_COUNT);
    jobSystem.init();

    {
        JobGroup group = jobSystem.newGroup();

        group.submit<void>([] {
            std::this_thread::sleep_for(FIRST_JOB_DELAY);
            throw std::runtime_error("first");
        });

        group.submit<void>([] {
            throw TuringException("second");
        });

        expectRethrow<std::runtime_error>([&group] { group.wait(); }, "first");
    }

    jobSystem.terminate();
}

void unwaitedFailuresLeaveTheProcessRunning() {
    JobSystem jobSystem(THREAD_COUNT);
    jobSystem.init();

    jobSystem.submit<void>([] {
        throw TuringException(std::string {MESSAGE});
    });

    {
        JobGroup group = jobSystem.newGroup();

        group.submit<void>([] {
            throw std::runtime_error(std::string {MESSAGE});
        });
    }

    jobSystem.wait();
    jobSystem.terminate();
}

}

class JobExceptionTest : public TuringTest {
protected:
    void initialize() override {}

    void terminate() override {}
};

TEST_F(JobExceptionTest, aFutureRethrowsTheTuringExceptionOfItsJob) {
    EXPECT_EXIT(exitWithTestOutcome(futureRethrows<TuringException>, TIMEOUT_SECONDS), testing::ExitedWithCode(EXIT_SUCCESS), "");
}

TEST_F(JobExceptionTest, aFutureRethrowsTheStandardExceptionOfItsJob) {
    EXPECT_EXIT(exitWithTestOutcome(futureRethrows<std::runtime_error>, TIMEOUT_SECONDS), testing::ExitedWithCode(EXIT_SUCCESS), "");
}

TEST_F(JobExceptionTest, aFutureRethrowsTheExceptionOfAJobRunOnTheSubmittingThread) {
    EXPECT_EXIT(exitWithTestOutcome(futureRethrowsAJobRunOnTheSubmittingThread, TIMEOUT_SECONDS), testing::ExitedWithCode(EXIT_SUCCESS), "");
}

TEST_F(JobExceptionTest, aGroupWaitRethrowsTheExceptionOfOneOfItsJobs) {
    EXPECT_EXIT(exitWithTestOutcome(groupWaitRethrows, TIMEOUT_SECONDS), testing::ExitedWithCode(EXIT_SUCCESS), "");
}

TEST_F(JobExceptionTest, aGroupWaitRethrowsTheFailureOfTheFirstSubmittedJob) {
    EXPECT_EXIT(exitWithTestOutcome(groupWaitRethrowsTheFirstSubmittedFailure, TIMEOUT_SECONDS), testing::ExitedWithCode(EXIT_SUCCESS), "");
}

TEST_F(JobExceptionTest, aFailureNobodyWaitsForLeavesTheProcessRunning) {
    EXPECT_EXIT(exitWithTestOutcome(unwaitedFailuresLeaveTheProcessRunning, TIMEOUT_SECONDS), testing::ExitedWithCode(EXIT_SUCCESS), "");
}

int main(int argc, char** argv) {
    return turing::test::turingTestMain(argc, argv, [] {
        GTEST_FLAG_SET(death_test_style, "threadsafe");
    });
}

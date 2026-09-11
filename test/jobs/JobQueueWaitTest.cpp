#include <time.h>

#include <chrono>
#include <memory>
#include <thread>

#include "TuringTest.h"

#include "JobSystem.h"

using namespace turing::test;
using namespace db;

class JobQueueWaitTest : public TuringTest {
protected:
    void initialize() override {}

    void terminate() override {}

    static constexpr size_t WORKER_COUNT = 8;
    static constexpr std::chrono::milliseconds JOB_DURATION {300};
};

TEST_F(JobQueueWaitTest, WaitingForAJobConsumesNoCpu) {
    const std::unique_ptr<JobSystem> jobSystem = std::make_unique<JobSystem>(WORKER_COUNT);
    jobSystem->init();

    jobSystem->submit<void>([](Promise*) {
        std::this_thread::sleep_for(JOB_DURATION);
    });

    const clock_t cpuStart = clock();
    const std::chrono::steady_clock::time_point wallStart = std::chrono::steady_clock::now();

    jobSystem->wait();

    const double cpuSeconds = double(clock() - cpuStart) / CLOCKS_PER_SEC;
    const double wallSeconds = std::chrono::duration<double>(std::chrono::steady_clock::now() - wallStart).count();

    // The job only sleeps, so every thread should be blocked for the whole wait.
    // A spinning wait burns at least a full core over that span, which is what
    // the ratio catches; a blocking one costs near zero whatever the core count.
    EXPECT_GE(wallSeconds, 0.2);
    EXPECT_LT(cpuSeconds, wallSeconds / 2.0);

    jobSystem->terminate();
}

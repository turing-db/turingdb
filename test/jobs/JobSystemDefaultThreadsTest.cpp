#include <memory>

#include "TuringTest.h"

#include "JobSystem.h"

using namespace turing::test;
using namespace db;

class JobSystemDefaultThreadsTest : public TuringTest {
protected:
    void initialize() override {}

    void terminate() override {}
};

TEST_F(JobSystemDefaultThreadsTest, DefaultConstructedSystemRunsOnOneThread) {
    const JobSystem jobSystem;

    EXPECT_EQ(jobSystem.getThreadCount(), 1);
}

TEST_F(JobSystemDefaultThreadsTest, AnExplicitThreadCountStillWins) {
    const JobSystem jobSystem(4);

    EXPECT_EQ(jobSystem.getThreadCount(), 4);
}

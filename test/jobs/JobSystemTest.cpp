#include "JobGroup.h"
#include "TuringTest.h"

#include "SharedStorage.h"
#include "JobSystem.h"

using namespace turing::test;
using namespace db;

class JobSystemTest : public TuringTest {
protected:
    void initialize() override {}

    void terminate() override {}

    static constexpr size_t COUNT_PER_THREAD = 100;
    static constexpr size_t JOB_COUNT = 10;
    SharedStorage _storage;
};


TEST_F(JobSystemTest, OneWorker) {
    auto jobsystem = std::make_unique<JobSystem>(1);
    jobsystem->initialize();

    for (size_t i = 0; i < JOB_COUNT; i++) {
        jobsystem->submit<void>([&](Promise*) {
            for (size_t j = 0; j < COUNT_PER_THREAD; j++) {
                _storage.inc();
            }
        });
    }

    jobsystem->wait();

    ASSERT_EQ(_storage.sum(), COUNT_PER_THREAD * JOB_COUNT);

    jobsystem->terminate();
}

TEST_F(JobSystemTest, TwoWorkers) {
    auto jobsystem = std::make_unique<JobSystem>(2);
    jobsystem->initialize();

    for (size_t i = 0; i < JOB_COUNT; i++) {
        jobsystem->submit<void>([&](Promise*) {
            for (size_t j = 0; j < COUNT_PER_THREAD; j++) {
                _storage.inc();
            }
        });
    }

    jobsystem->wait();

    ASSERT_EQ(_storage.sum(), COUNT_PER_THREAD * JOB_COUNT);

    jobsystem->terminate();
}

TEST_F(JobSystemTest, ManyWorkers) {
    auto jobsystem = std::make_unique<JobSystem>(64);
    jobsystem->initialize();

    for (size_t i = 0; i < JOB_COUNT; i++) {
        jobsystem->submit<void>([&](Promise*) {
            for (size_t j = 0; j < COUNT_PER_THREAD; j++) {
                _storage.inc();
            }
        });
    }

    jobsystem->wait();

    ASSERT_EQ(_storage.sum(), COUNT_PER_THREAD * JOB_COUNT);

    jobsystem->terminate();
}

TEST_F(JobSystemTest, OneWorkerShared) {
    auto jobsystem = std::make_unique<JobSystem>(1);
    jobsystem->initialize();

    for (size_t i = 0; i < JOB_COUNT; i++) {
        jobsystem->submitShared<void>([&](Promise*) {
            for (size_t j = 0; j < COUNT_PER_THREAD; j++) {
                _storage.inc();
            }
        });
    }

    jobsystem->wait();

    ASSERT_EQ(_storage.sum(), COUNT_PER_THREAD * JOB_COUNT);

    jobsystem->terminate();
}

TEST_F(JobSystemTest, TwoWorkersShared) {
    auto jobsystem = std::make_unique<JobSystem>(2);
    jobsystem->initialize();

    for (size_t i = 0; i < JOB_COUNT; i++) {
        jobsystem->submitShared<void>([&](Promise*) {
            for (size_t j = 0; j < COUNT_PER_THREAD; j++) {
                _storage.inc();
            }
        });
    }

    jobsystem->wait();

    ASSERT_EQ(_storage.sum(), COUNT_PER_THREAD * JOB_COUNT);

    jobsystem->terminate();
}

TEST_F(JobSystemTest, ManyWorkersShared) {
    auto jobsystem = std::make_unique<JobSystem>(64);
    jobsystem->initialize();

    for (size_t i = 0; i < JOB_COUNT; i++) {
        jobsystem->submitShared<void>([&](Promise*) {
            for (size_t j = 0; j < COUNT_PER_THREAD; j++) {
                _storage.inc();
            }
        });
    }

    jobsystem->wait();

    ASSERT_EQ(_storage.sum(), COUNT_PER_THREAD * JOB_COUNT);

    jobsystem->terminate();
}

TEST_F(JobSystemTest, OneWorkerJobGroup) {
    auto jobsystem = std::make_unique<JobSystem>(1);
    jobsystem->initialize();

    auto group = jobsystem->newGroup();

    for (size_t i = 0; i < JOB_COUNT; i++) {
        group.submit<void>([&](Promise*) {
            for (size_t j = 0; j < COUNT_PER_THREAD; j++) {
                _storage.inc();
            }
        });
    }

    group.wait();

    ASSERT_EQ(_storage.sum(), COUNT_PER_THREAD * JOB_COUNT);

    jobsystem->terminate();
}

TEST_F(JobSystemTest, TwoWorkerJobGroup) {
    auto jobsystem = std::make_unique<JobSystem>(2);
    jobsystem->initialize();

    auto group = jobsystem->newGroup();

    for (size_t i = 0; i < JOB_COUNT; i++) {
        group.submit<void>([&](Promise*) {
            for (size_t j = 0; j < COUNT_PER_THREAD; j++) {
                _storage.inc();
            }
        });
    }

    group.wait();

    ASSERT_EQ(_storage.sum(), COUNT_PER_THREAD * JOB_COUNT);

    jobsystem->terminate();
}

TEST_F(JobSystemTest, ManyWorkerJobGroup) {
    auto jobsystem = std::make_unique<JobSystem>(64);
    jobsystem->initialize();

    auto group = jobsystem->newGroup();

    for (size_t i = 0; i < JOB_COUNT; i++) {
        group.submit<void>([&](Promise*) {
            for (size_t j = 0; j < COUNT_PER_THREAD; j++) {
                _storage.inc();
            }
        });
    }

    group.wait();

    ASSERT_EQ(_storage.sum(), COUNT_PER_THREAD * JOB_COUNT);

    jobsystem->terminate();
}

TEST_F(JobSystemTest, TooManyWorkerJobGroup) {
    auto jobsystem = std::make_unique<JobSystem>(1024);
    jobsystem->initialize();

    auto group = jobsystem->newGroup();

    for (size_t i = 0; i < JOB_COUNT; i++) {
        group.submit<void>([&](Promise*) {
            for (size_t j = 0; j < COUNT_PER_THREAD; j++) {
                _storage.inc();
            }
        });
    }

    group.wait();

    ASSERT_EQ(_storage.sum(), COUNT_PER_THREAD * JOB_COUNT);

    jobsystem->terminate();
}

TEST_F(JobSystemTest, SubmitFromWorker) {
    spdlog::info("Submit from worker");

    for (size_t i = 0; i < 5; i++) {

        auto jobsystem = std::make_unique<JobSystem>(1);
        jobsystem->initialize();

        for (size_t j = 0; j < 6; j++) {
            jobsystem->submit<void>([&](Promise*) {
                for (size_t k = 0; k < 5; k++) {
                    jobsystem->submit<void>([&](Promise*) {
                        for (size_t l = 0; l < 5; l++) {
                            _storage.inc();
                        }
                    });
                }
            });
        }

        jobsystem->terminate();
    }

    ASSERT_EQ(_storage.sum(), 5 * 6 * 5 * 5);
}


TEST_F(JobSystemTest, ManyJobSystems) {
    spdlog::info("Many systems test");
    for (size_t i = 0; i < 10; i++) {
        auto jobsystem = std::make_unique<JobSystem>(1);
        jobsystem->initialize();

        for (size_t j = 0; j < 6; j++) {
            jobsystem->submit<void>([&](Promise*) {
                for (size_t k = 0; k < 50; k++) {
                    _storage.inc();
                }
            });
        }

        jobsystem->terminate();
    }

    ASSERT_EQ(_storage.sum(), 10 * 6 * 50);
}

int main(int argc, char** argv) {
    return turing::test::turingTestMain(argc, argv, [] {
        testing::GTEST_FLAG(repeat) = 50;
    });
}

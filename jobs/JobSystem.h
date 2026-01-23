#pragma once

#include <thread>
#include <memory>

#include "Job.h"
#include "Future.h"
#include "Promise.h"
#include "JobQueue.h"

namespace db {

class JobGroup;

class JobSystem {
public:
    static std::unique_ptr<JobSystem> create();
    static std::unique_ptr<JobSystem> create(size_t nthreads);

    JobSystem(const JobSystem&) = delete;
    JobSystem(JobSystem&&) = delete;
    JobSystem& operator=(const JobSystem&) = delete;
    JobSystem& operator=(JobSystem&&) = delete;
    ~JobSystem();

    /* @brief submits a job to be executed
     *
     * @param operation closure to be executed on one of the threads
     * @return Future<T> holding the state of the TypedPromise<T>
     * */
    template <typename T>
    Future<T> submit(JobOperation&& operation) {
        TypedPromise<T>* promise = new TypedPromise<T>();
        Future<T> future {promise->get_future()};

        Job job {
            std::move(operation),
            std::unique_ptr<Promise>(static_cast<Promise*>(promise)),
        };

        _jobs.submit(std::move(job));

        return future;
    }

    /* @brief submits a job to be executed
     *
     * @param operation closure to be executed on one of the threads
     * @return SharedFuture<T> holding the shared state of the TypedPromise<T>
     * */
    template <typename T>
    SharedFuture<T> submitShared(JobOperation&& operation) {
        TypedPromise<T>* promise = new TypedPromise<T>;
        SharedFuture<T> future {promise->get_future().share()};

        Job job {
            std::move(operation),
            std::unique_ptr<Promise>(static_cast<Promise*>(promise)),
        };

        _jobs.submit(std::move(job));

        return future;
    }

    /* @brief Creates a new job group
     *
     * Allows to wait on a subset of the submitted jobs
     * */
    JobGroup newGroup();

    void wait();
    void terminate();

private:
    size_t _nThreads {0};
    JobQueue _jobs;
    std::vector<std::jthread> _workers;
    std::atomic<bool> _stopRequested {false};
    bool _terminated {false};

    JobSystem();
    explicit JobSystem(size_t nThreads);
    void initialize();
};

}

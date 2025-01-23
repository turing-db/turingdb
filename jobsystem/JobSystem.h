#pragma once

#include <thread>

#include "Job.h"
#include "Future.h"
#include "Promise.h"
#include "JobQueue.h"

namespace js {

class JobGroup;

class JobSystem {
public:
    JobSystem();
    explicit JobSystem(size_t nThreads);

    JobSystem(const JobSystem&) = delete;
    JobSystem(JobSystem&&) = delete;
    JobSystem& operator=(const JobSystem&) = delete;
    JobSystem& operator=(JobSystem&&) = delete;
    ~JobSystem();

    void initialize();

    /* @brief submits a job to be executed
     *
     * @param operation closure to be executed on one of the threads
     * @return Future<T> holding the state of the TypedPromise<T>
     * */
    template <typename T>
    Future<T> submit(JobOperation&& operation) {
        _submitedCount += 1;
        TypedPromise<T>* promise = new TypedPromise<T>();
        Future<T> future {promise->get_future()};

        Job job {
            std::move(operation),
            std::unique_ptr<Promise>(static_cast<Promise*>(promise)),
        };

        std::unique_lock lock(_queueMutex);
        if (std::this_thread::get_id() == _mainThreadID) {
            // Submitted from main
            _jobs.push(std::move(job));
            _wakeCondition.notify_one();
            return future;
        }


        // Not running from main, make sure at least
        // one thread remains available to execute
        // jobs
        if (_jobs.size() < _nThreads - 1) {
            // Submit for parallel execution
            _jobs.push(std::move(job));
            _wakeCondition.notify_one();
        } else {
            // Execute sequentially
            job._operation(job._promise.get());
            job._promise->finish();
            _finishedCount.fetch_add(1);
        }

        return future;
    }

    /* @brief submits a job to be executed
     *
     * @param operation closure to be executed on one of the threads
     * @return SharedFuture<T> holding the shared state of the TypedPromise<T>
     * */
    template <typename T>
    SharedFuture<T> submitShared(JobOperation&& operation) {
        std::unique_lock lock(_queueMutex);
        _submitedCount += 1;
        TypedPromise<T>* promise = new TypedPromise<T>;
        SharedFuture<T> future {promise->get_future().share()};

        Job job {
            std::move(operation),
            std::unique_ptr<Promise>(static_cast<Promise*>(promise)),
        };

        if (std::this_thread::get_id() == _mainThreadID) {
            // Submitted from main
            _jobs.push(std::move(job));
            _wakeCondition.notify_one();
            return future;
        }


        // Not running from main, make sure at least
        // one thread remains available to execute
        // jobs
        if (_jobs.size() < _nThreads - 1) {
            // Submit for parallel execution
            _jobs.push(std::move(job));
            _wakeCondition.notify_one();
        } else {
            // Execute sequentially
            job._operation(job._promise.get());
            job._promise->finish();
            _finishedCount.fetch_add(1);
        }

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
    std::thread::id _mainThreadID;
    size_t _nThreads {0};
    std::mutex _queueMutex;
    std::mutex _wakeMutex;
    std::condition_variable _wakeCondition;
    JobQueue _jobs;
    std::vector<std::jthread> _workers;
    std::atomic<size_t> _finishedCount {0};
    std::atomic<size_t> _submitedCount {0};
    std::atomic<bool> _stopRequested {false};
    bool _terminated {false};
};

}

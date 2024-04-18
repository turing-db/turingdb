#pragma once

#include <future>
#include <optional>
#include <thread>
#include <vector>

template <typename T>
using Future = std::future<T>;
template <typename T>
using SharedFuture = std::shared_future<T>;

class Promise {};

template <typename T>
class TypedPromise : public Promise, public std::promise<T> {};

using JobOperation = std::function<void(Promise*)>;
using JobID = uint64_t;

struct Job {
    JobOperation _operation;
    std::unique_ptr<Promise> _promise = nullptr;
};

class JobQueue {
public:
    void push(Job job);
    std::optional<Job> pop();
    size_t size() const;

private:
    std::vector<Job> _jobs;
};

class JobSystem {
public:
    JobSystem() = default;
    JobSystem(const JobSystem&) = delete;
    JobSystem(JobSystem&&) = delete;
    JobSystem& operator=(const JobSystem&) = delete;
    JobSystem& operator=(JobSystem&&) = delete;
    ~JobSystem();

    void initialize(size_t numThreads = 0);

    template <typename T>
    Future<T> submit(JobOperation&& operation) {
        std::unique_lock lock(_queueMutex);
        _submitedCount += 1;
        TypedPromise<T>* promise = new TypedPromise<T>;
        Future<T> future = promise->get_future();

        _jobs.push({
            std::move(operation),
            std::unique_ptr<Promise>(static_cast<Promise*>(promise)),
        });
        _wakeCondition.notify_one();
        return future;
    }

    template <typename T>
    SharedFuture<T> submitShared(JobOperation&& operation) {
        std::unique_lock lock(_queueMutex);
        _submitedCount += 1;
        TypedPromise<T>* promise = new TypedPromise<T>;
        SharedFuture<T> future = promise->get_future();

        _jobs.push({
            std::move(operation),
            std::unique_ptr<Promise>(static_cast<Promise*>(promise)),
        });
        _wakeCondition.notify_one();
        return future;
    }

    void wait();
    void terminate();

    template <typename T>
    static TypedPromise<T>* cast(Promise* p) {
        return static_cast<TypedPromise<T>*>(p);
    }

private:
    std::mutex _queueMutex;
    std::mutex _wakeMutex;
    std::condition_variable _wakeCondition;
    JobQueue _jobs;
    std::vector<std::jthread> _workers;
    std::atomic<size_t> _finishedCount = 0;
    std::atomic<size_t> _submitedCount = 0;
    std::atomic<bool> _stopRequested = false;
    bool _terminated = false;
};

#pragma once

#include <condition_variable>
#include <mutex>
#include <optional>
#include <queue>
#include <thread>

#include "Job.h"

namespace db {

class JobSystem;

class JobQueue {
public:
    explicit JobQueue(size_t threadCount)
        : _threadCount(threadCount)
    {
    }

    void submit(Job job) {
        std::unique_lock lock {_mutex};
        _submitted++;

        if (_runningCount >= _threadCount) {
            // Execute from sequentially
            _runningCount++;

            lock.unlock();
            job._operation(job._promise.get());
            job._promise->finish();
            lock.lock();

            _finished++;
            _runningCount--;

        } else {
            _jobs.emplace(std::move(job));
            _wakeCondition.notify_one();
        }
    }

    std::optional<Job> pop() {
        std::scoped_lock lock {_mutex};
        if (_jobs.empty()) {
            return std::nullopt;
        }

        Job job = std::move(_jobs.front());
        _jobs.pop();
        _runningCount++;
        return job;
    }

    size_t size() const {
        std::scoped_lock lock {_mutex};
        return _jobs.size();
    }

    bool empty() const {
        std::scoped_lock lock {_mutex};
        return _jobs.empty();
    }

    std::optional<Job> waitJob(std::invocable auto extraCondition) {
        std::unique_lock lock {_mutex};

        _wakeCondition.wait(lock, [&] {
            return !_jobs.empty() || extraCondition();
        });

        if (_jobs.empty()) {
            return std::nullopt;
        }

        Job job = std::move(_jobs.front());
        _jobs.pop();
        _runningCount++;
        return job;
    }

    void incrementFinished() {
        std::scoped_lock lock {_mutex};
        _finished++;
        _runningCount--;
    }

    void wait() {
        std::unique_lock lock {_mutex};

        while (_finished != _submitted) {
            lock.unlock();
            _wakeCondition.notify_one();
            std::this_thread::yield();
            lock.lock();
        }

        _wakeCondition.notify_all();
    }

private:
    friend JobSystem;

    size_t _threadCount {1};
    std::condition_variable _wakeCondition;
    mutable std::mutex _mutex;
    std::queue<Job> _jobs;
    size_t _finished {0};
    size_t _submitted {0};
    size_t _runningCount {0};

    std::mutex& getMutex() { return _mutex; }
};

}

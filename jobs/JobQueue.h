#pragma once

#include <condition_variable>
#include <memory>
#include <mutex>
#include <queue>

#include "Job.h"

namespace db {

class JobSystem;

class JobQueue {
public:
    explicit JobQueue(size_t threadCount)
        : _threadCount(threadCount)
    {
    }

    void submit(std::unique_ptr<Job> job) {
        std::unique_lock lock(_mutex);
        _submitted++;

        if (_runningCount >= _threadCount) {
            // Execute from sequentially
            _runningCount++;

            lock.unlock();
            job->run();
            lock.lock();

            _finished++;
            _runningCount--;

            if (_finished == _submitted) {
                _doneCondition.notify_all();
            }

        } else {
            _jobs.emplace(std::move(job));
            _wakeCondition.notify_one();
        }
    }

    size_t size() const {
        std::scoped_lock lock(_mutex);
        return _jobs.size();
    }

    bool empty() const {
        std::scoped_lock lock(_mutex);
        return _jobs.empty();
    }

    std::unique_ptr<Job> waitJob(std::invocable auto extraCondition) {
        std::unique_lock lock(_mutex);

        _wakeCondition.wait(lock, [&] {
            return !_jobs.empty() || extraCondition();
        });

        if (_jobs.empty()) {
            return nullptr;
        }

        std::unique_ptr<Job> job = std::move(_jobs.front());
        _jobs.pop();
        _runningCount++;
        return job;
    }

    void incrementFinished() {
        std::scoped_lock lock(_mutex);
        _finished++;
        _runningCount--;

        if (_finished == _submitted) {
            _doneCondition.notify_all();
        }
    }

    void wait() {
        std::unique_lock lock(_mutex);

        _doneCondition.wait(lock, [&] {
            return _finished == _submitted;
        });

        _wakeCondition.notify_all();
    }

private:
    friend JobSystem;

    size_t _threadCount {1};
    std::condition_variable _wakeCondition;
    std::condition_variable _doneCondition;
    mutable std::mutex _mutex;
    std::queue<std::unique_ptr<Job>> _jobs;
    size_t _finished {0};
    size_t _submitted {0};
    size_t _runningCount {0};

    std::mutex& getMutex() { return _mutex; }
};

}

#include "JobSystem.h"

#include <spdlog/spdlog.h>
#include <cassert>

#include "JobGroup.h"

using namespace js;

JobSystem::JobSystem()
    : _mainThreadID(std::this_thread::get_id())
{
}

JobSystem::JobSystem(size_t nThreads)
    : _mainThreadID(std::this_thread::get_id()),
      _nThreads(nThreads)
{
}

JobSystem::~JobSystem() {
    if (!_terminated) {
        terminate();
    }
}

void JobSystem::initialize() {
    if (_nThreads == 0) {
        size_t numCores = std::thread::hardware_concurrency();
        _nThreads = std::max(1ul, numCores);
    }

    for (size_t i = 0; i < _nThreads; i++) {
        _workers.emplace_back([&] {
            while (true) {
                std::optional<Job> j;
                {
                    std::unique_lock queueLock {_queueMutex};

                    // Wait until new job is pushed or stop is requested
                    _wakeCondition.wait(queueLock, [&] {
                        return !_jobs.empty() || _stopRequested.load();
                    });

                    // Retrieving job while queue is still locked
                    j = _jobs.pop();

                    // Release JobQueue at the end of this block
                }

                if (!j && _stopRequested.load()) {
                    // No more job available and stop requested
                    return;
                }

                auto& job = j.value();

                job._operation(job._promise.get());
                job._promise->finish();
                _finishedCount.fetch_add(1);
            }
        });
    }

    spdlog::info("Job system initialized with {} threads", _nThreads);
}

void JobSystem::wait() {
    while (_finishedCount.load() < _submitedCount.load()) {
        _wakeCondition.notify_one();
        std::this_thread::yield();
    }
}

void JobSystem::terminate() {
    assert(!_terminated);
    {
        std::unique_lock<std::mutex> lock(_queueMutex);
        spdlog::info("Job system terminating, {} jobs remaining", _jobs.size());
    }

    _stopRequested.store(true);
    _wakeCondition.notify_all();
    wait();
    _terminated = true;
    spdlog::info("Job system terminated");
}

JobGroup JobSystem::newGroup() {
    return JobGroup(this);
}

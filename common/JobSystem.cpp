#include "JobSystem.h"
#include "BioAssert.h"

#include <spdlog/spdlog.h>

Job& JobQueue::push(Job job) {
    return _jobs.emplace(std::move(job));
}

std::optional<Job> JobQueue::pop() {
    if (_jobs.empty()) {
        return std::nullopt;
    }

    Job job = std::move(_jobs.front());
    _jobs.pop();
    return job;
}

size_t JobQueue::size() const {
    return _jobs.size();
}

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
                _queueMutex.lock();
                auto j = _jobs.pop();
                _queueMutex.unlock();

                if (_stopRequested.load()) {
                    return;
                }

                if (j.has_value()) {
                    auto& jobValue = j.value();
                    jobValue._operation(jobValue._promise.get());
                    jobValue._promise->finish();
                    _finishedCount.fetch_add(1);
                } else {
                    // no job, put thread to sleep
                    std::unique_lock<std::mutex> lock(_wakeMutex);
                    _wakeCondition.wait(lock);
                }
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
    msgbioassert(!_terminated, "Attempting to terminate the job system twice");

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

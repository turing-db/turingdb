#include "JobSystem.h"
#include "BioAssert.h"
#include "BioLog.h"
#include "MsgCommon.h"

void JobQueue::push(Job job) {
    _jobs.emplace_back(std::move(job));
}

std::optional<Job> JobQueue::pop() {
    if (_jobs.empty()) {
        return std::nullopt;
    }

    Job job = std::move(_jobs.front());
    _jobs.erase(_jobs.begin());
    return job;
}

size_t JobQueue::size() const {
    return _jobs.size();
}

JobSystem::~JobSystem() {
    if (!_terminated) {
        terminate();
    }
}

void JobSystem::initialize(size_t numThreads) {
    if (numThreads == 0) {
        size_t numCores = std::thread::hardware_concurrency();
        numThreads = std::max(1ul, numCores);
    }

    for (size_t i = 0; i < numThreads; i++) {
        _workers.emplace_back([&] {
            JobOperation job;

            while (true) {
                _queueMutex.lock();
                auto job = _jobs.pop();
                _queueMutex.unlock();

                if (_stopRequested.load()) {
                    return;
                }

                if (job.has_value()) {
                    auto& jobValue = job.value();
                    jobValue._operation(jobValue._promise.get());
                    _finishedCount.fetch_add(1);
                } else {
                    // no job, put thread to sleep
                    std::unique_lock<std::mutex> lock(_wakeMutex);
                    _wakeCondition.wait(lock);
                }
            }
        });
    }

    Log::BioLog::log(msg::INFO_JOB_SYSTEM_INITIALIZED() << numThreads);
}

void JobSystem::wait() {
    while (_finishedCount.load() < _submitedCount.load()) {
        _wakeCondition.notify_one();
        std::this_thread::yield();
    }
}

void JobSystem::terminate() {
    msgbioassert(!_terminated, "Attempting to terminate the job system twice");
    _queueMutex.lock();
    Log::BioLog::log(msg::INFO_JOB_SYSTEM_TERMINATING() << _jobs.size());
    _queueMutex.unlock();
    _stopRequested.store(true);
    _wakeCondition.notify_all();
    wait();
    _terminated = true;
    Log::BioLog::log(msg::INFO_JOB_SYSTEM_TERMINATED());
}

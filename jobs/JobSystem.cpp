#include "JobSystem.h"

#include <spdlog/spdlog.h>
#include <assert.h>

#include "JobGroup.h"

using namespace db;

JobSystem::JobSystem()
    : _nThreads(std::max(1ul, (size_t)std::thread::hardware_concurrency())),
    _jobs(_nThreads)
{
}

JobSystem::JobSystem(size_t nThreads)
    : _nThreads(nThreads == 0
                    ? std::max(1ul, (size_t)std::thread::hardware_concurrency())
                    : nThreads),
    _jobs(_nThreads)
{
}

JobSystem::~JobSystem() {
    if (!_terminated) {
        terminate();
    }
}

void JobSystem::initialize() {
    for (size_t i = 0; i < _nThreads; i++) {
        _workers.emplace_back([&] {
            while (true) {
                std::optional<Job> j = _jobs.waitJob([&] {
                    return _stopRequested.load();
                });

                if (!j && _stopRequested.load()) {
                    // No more job available and stop requested
                    return;
                }

                auto& job = j.value();
                job._operation(job._promise.get());
                job._promise->finish();
                _jobs.incrementFinished();
            }
        });
    }
}

void JobSystem::wait() {
    _jobs.wait();
}

void JobSystem::terminate() {
    assert(!_terminated);
    _stopRequested.store(true);
    _jobs.wait();
    _terminated = true;
}

JobGroup JobSystem::newGroup() {
    return JobGroup(this);
}

std::unique_ptr<JobSystem> JobSystem::create() {
    auto jobSystem = std::unique_ptr<JobSystem>(new JobSystem());
    jobSystem->initialize();
    return jobSystem;
}

std::unique_ptr<JobSystem> JobSystem::create(size_t nthreads) {
    auto jobSystem = std::unique_ptr<JobSystem>(new JobSystem(nthreads));
    jobSystem->initialize();
    return jobSystem;
}

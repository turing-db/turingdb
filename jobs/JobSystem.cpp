#include "JobSystem.h"

#include <assert.h>

#include <spdlog/spdlog.h>

#include "JobGroup.h"
#include "ThreadName.h"

using namespace db;

namespace {

constexpr const char* THREAD_NAME = "turingdb.worker";

size_t getThreadCount(size_t requestedThreads) {
    const size_t hardwareThreads = std::max<size_t>(1, std::thread::hardware_concurrency());
    if (requestedThreads == 0) {
        return hardwareThreads;
    } else {
        return requestedThreads;
    }
}

}

JobSystem::JobSystem()
    : _nThreads(getThreadCount(0)),
    _jobs(_nThreads)
{
}

JobSystem::JobSystem(size_t nthreads)
    : _nThreads(getThreadCount(nthreads)),
    _jobs(_nThreads)
{
}

JobSystem::~JobSystem() {
    if (!_terminated) {
        terminate();
    }
}

void JobSystem::init() {
    for (size_t i = 0; i < _nThreads; i++) {
        _workers.emplace_back([&] {
            ThreadName::set(THREAD_NAME);

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

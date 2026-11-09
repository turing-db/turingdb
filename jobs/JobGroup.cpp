#include "JobGroup.h"

using namespace db;

JobGroup::JobGroup(JobSystem* jobSystem)
    : _jobSystem(jobSystem)
{
}

JobGroup::~JobGroup() {
    for (const std::unique_ptr<AbstractFuture>& future : _futures) {
        future->wait();
    }
}

void JobGroup::wait() {
    std::vector<std::unique_ptr<AbstractFuture>> futures;
    futures.swap(_futures);

    // Every job must finish before a failure is rethrown: a job still running
    // may read the caller's frame, which the exception unwinds.
    for (const std::unique_ptr<AbstractFuture>& future : futures) {
        future->wait();
    }

    for (const std::unique_ptr<AbstractFuture>& future : futures) {
        future->throwIfFailed();
    }
}

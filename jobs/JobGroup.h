#pragma once

#include "JobSystem.h"

namespace db {

class JobGroup {
public:
    explicit JobGroup(JobSystem* jobSystem)
        : _jobSystem(jobSystem)
    {
    }

    ~JobGroup() {
        wait();
    }

    void wait() {
        for (auto& future : _futures) {
            future->wait();
        }

        _futures.clear();
    }

    size_t size() const {
        return _futures.size();
    }

    template <typename T>
    SharedFuture<T> submit(JobOperation&& operation) {
        auto future = _jobSystem->submitShared<T>(std::move(operation));
        SharedFuture<T>* ptr = new SharedFuture<T>(future);
        _futures.emplace_back(static_cast<AbstractFuture*>(ptr));
        return future;
    }

private:
    std::vector<std::shared_ptr<AbstractFuture>> _futures;
    JobSystem* _jobSystem {nullptr};
};

}

#pragma once

#include <memory>
#include <vector>

#include "JobSystem.h"

namespace db {

class JobGroup {
public:
    explicit JobGroup(JobSystem* jobSystem);
    ~JobGroup();

    void wait();

    size_t size() const {
        return _futures.size();
    }

    template <typename T>
    SharedFuture<T> submit(JobOperation<T>&& operation) {
        SharedFuture<T> future = _jobSystem->submitShared<T>(std::move(operation));
        _futures.emplace_back(std::make_unique<SharedFuture<T>>(future));
        return future;
    }

private:
    std::vector<std::unique_ptr<AbstractFuture>> _futures;
    JobSystem* _jobSystem {nullptr};
};

}

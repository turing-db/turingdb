#pragma once

#include <functional>
#include <future>

namespace db {

template <typename T>
using JobOperation = std::function<T()>;

class Job {
public:
    virtual ~Job();

    // Never throws: the operation's exception is stored in the job's future.
    virtual void run() = 0;
};

template <typename T>
class TypedJob : public Job {
public:
    explicit TypedJob(JobOperation<T>&& operation)
        : _task(std::move(operation))
    {
    }

    std::future<T> getFuture() {
        return _task.get_future();
    }

    void run() override {
        _task();
    }

private:
    std::packaged_task<T()> _task;
};

}

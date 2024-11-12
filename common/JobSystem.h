#pragma once

#include <future>
#include <optional>
#include <thread>
#include <queue>

class AbstractFuture {
public:
    AbstractFuture() = default;
    AbstractFuture(const AbstractFuture&) = default;
    AbstractFuture(AbstractFuture&&) = default;
    AbstractFuture& operator=(const AbstractFuture&) = default;
    AbstractFuture& operator=(AbstractFuture&&) = default;
    virtual ~AbstractFuture() = default;

    virtual void wait() = 0;
};

/* @brief Wrapper class around std::future<T>
 *
 * The contained state is only accessible by move operations, which consume
 * the future object. If multiple access to the same state is required,
 * use ShareFuture<T> instead.
 * */
template <typename T>
class Future : public std::future<T>, public AbstractFuture {
public:
    Future() = default;
    Future(const Future&) = default;
    Future(Future&&) noexcept = default;
    Future& operator=(const Future&) = default;
    Future& operator=(Future&&) noexcept = default;
    ~Future() override = default;

    explicit Future(std::future<T>&& future)
        : std::future<T>(std::move(future))
    {
    }

    void wait() override {
        std::future<T>::wait();
    }
};

/* @brief Wrapper class around std::shared_future<T>
 *
 * The contained state is shared by multiple SharedFuture<T>
 * ojects. Accessing the internal state is thread safe if called from
 * different SharedFuture<T> objects.
 * Mostly used with JobGroups
 * */
template <typename T>
class SharedFuture : public std::shared_future<T>, public AbstractFuture {
public:
    SharedFuture() = default;
    SharedFuture(const SharedFuture&) = default;
    SharedFuture(SharedFuture&&) noexcept = default;
    SharedFuture& operator=(const SharedFuture&) = default;
    SharedFuture& operator=(SharedFuture&&) noexcept = default;
    ~SharedFuture() override = default;

    explicit SharedFuture(std::shared_future<T>&& future)
        : std::shared_future<T>(std::move(future))
    {
    }

    void wait() override {
        std::shared_future<T>::wait();
    }
};

template <typename T>
class TypedPromise;

class Promise {
public:
    Promise() = default;
    Promise(const Promise&) = default;
    Promise(Promise&&) = delete;
    Promise& operator=(const Promise&) = default;
    Promise& operator=(Promise&&) = delete;
    virtual ~Promise() = default;

    template <typename T>
    constexpr TypedPromise<T>* cast() {
        return static_cast<TypedPromise<T>*>(this);
    }

    virtual void finish() = 0;
};

template <typename T>
class TypedPromise : public Promise, public std::promise<T> {
public:
    void finish() override {
        if constexpr (std::is_same_v<T, void>) {
            this->set_value();
        }
    }
};



/* @brief Closure to be executed by the JobSystem
 *
 * Takes in an abstract Promise* that can be casted
 * into a TypedPromise<T>
 * */
using JobOperation = std::function<void(Promise*)>;
using JobID = uint64_t;

struct Job {
    JobOperation _operation;
    std::unique_ptr<Promise> _promise;
};

class JobQueue {
public:
    Job& push(Job job);
    std::optional<Job> pop();
    size_t size() const;

private:
    std::queue<Job> _jobs;
};

class JobGroup;

class JobSystem {
public:
    JobSystem();
    explicit JobSystem(size_t nThreads);

    JobSystem(const JobSystem&) = delete;
    JobSystem(JobSystem&&) = delete;
    JobSystem& operator=(const JobSystem&) = delete;
    JobSystem& operator=(JobSystem&&) = delete;
    ~JobSystem();

    void initialize();

    /* @brief submits a job to be executed
     *
     * @param operation closure to be executed on one of the threads
     * @return Future<T> holding the state of the TypedPromise<T>
     * */
    template <typename T>
    Future<T> submit(JobOperation&& operation) {
        std::unique_lock lock(_queueMutex);
        _submitedCount += 1;
        TypedPromise<T>* promise = new TypedPromise<T>();
        Future<T> future {promise->get_future()};

        Job job{
            std::move(operation),
            std::unique_ptr<Promise>(static_cast<Promise*>(promise)),
        };

        if (std::this_thread::get_id() == _mainThreadID) {
            // Submitted from main
            _jobs.push(std::move(job));
            _wakeCondition.notify_one();
            return future;
        }


        // Not running from main, make sure at least
        // one thread remains available to execute
        // jobs
        if (_jobs.size() < _nThreads - 1) {
            // Submit for parallel execution
            _jobs.push(std::move(job));
            _wakeCondition.notify_one();
        } else {
            // Execute sequentially
            job._operation(job._promise.get());
            job._promise->finish();
            _finishedCount.fetch_add(1);
        }

        return future;
    }

    /* @brief submits a job to be executed
     *
     * @param operation closure to be executed on one of the threads
     * @return SharedFuture<T> holding the shared state of the TypedPromise<T>
     * */
    template <typename T>
    SharedFuture<T> submitShared(JobOperation&& operation) {
        std::unique_lock lock(_queueMutex);
        _submitedCount += 1;
        TypedPromise<T>* promise = new TypedPromise<T>;
        SharedFuture<T> future {promise->get_future().share()};

        Job job{
            std::move(operation),
            std::unique_ptr<Promise>(static_cast<Promise*>(promise)),
        };

        if (std::this_thread::get_id() == _mainThreadID) {
            // Submitted from main
            _jobs.push(std::move(job));
            _wakeCondition.notify_one();
            return future;
        }


        // Not running from main, make sure at least
        // one thread remains available to execute
        // jobs
        if (_jobs.size() < _nThreads - 1) {
            // Submit for parallel execution
            _jobs.push(std::move(job));
            _wakeCondition.notify_one();
        } else {
            // Execute sequentially
            job._operation(job._promise.get());
            job._promise->finish();
            _finishedCount.fetch_add(1);
        }

        return future;
    }

    /* @brief Creates a new job group
     *
     * Allows to wait on a subset of the submitted jobs
     * */
    JobGroup newGroup();

    void wait();
    void terminate();

private:
    std::thread::id _mainThreadID;
    size_t _nThreads {0};
    std::mutex _queueMutex;
    std::mutex _wakeMutex;
    std::condition_variable _wakeCondition;
    JobQueue _jobs;
    std::vector<std::jthread> _workers;
    std::atomic<size_t> _finishedCount {0};
    std::atomic<size_t> _submitedCount {0};
    std::atomic<bool> _stopRequested {false};
    bool _terminated {false};
};

class JobGroup {
public:
    explicit JobGroup(JobSystem* jobSystem)
        : _jobSystem(jobSystem)
    {
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
    JobSystem* _jobSystem;
};

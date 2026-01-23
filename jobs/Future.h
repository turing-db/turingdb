#pragma once

#include <future>

namespace db {

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

}

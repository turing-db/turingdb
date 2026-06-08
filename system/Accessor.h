#pragma once

#include <mutex>
#include <shared_mutex>

namespace db {

class Accessor {
public:
    // Type tags to indicate if an accessor is in unique or shared mode
    struct UniqueAccess {};
    struct SharedAccess {};

    Accessor(const Accessor&) = delete;
    Accessor& operator=(const Accessor&) = delete;

    bool isShared() const { return _sharedLock.owns_lock(); }
    bool isUnique() const { return _uniqueLock.owns_lock(); }

protected:
    // Only callable by inheriting classes
    // We need to pass the mutex to lock onto, but otherwise it's a very bad practice
    Accessor(std::shared_mutex& mutex, UniqueAccess)
        : _uniqueLock(mutex)
    {
    }

    Accessor(std::shared_mutex& mutex, SharedAccess)
        : _sharedLock(mutex)
    {
    }

    ~Accessor() = default;

private:
    mutable std::unique_lock<std::shared_mutex> _uniqueLock;
    mutable std::shared_lock<std::shared_mutex> _sharedLock;
};

}

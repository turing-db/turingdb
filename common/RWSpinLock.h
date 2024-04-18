#pragma once

#include <atomic>
#include <stdint.h>

class RWSpinLock {
public:
    static_assert(std::atomic<uint32_t>::is_always_lock_free);

    RWSpinLock();
    explicit RWSpinLock(std::atomic<uint32_t> status);
    ~RWSpinLock();

    RWSpinLock(const RWSpinLock&) = delete;
    RWSpinLock(RWSpinLock&&) = delete;
    RWSpinLock& operator=(const RWSpinLock&) = delete;
    RWSpinLock& operator=(RWSpinLock&&) = delete;

    RWSpinLock copy() const;
    void assign(const RWSpinLock& other);

    void lock();
    void lock_shared();

    void unlock();
    void unlock_shared();

private:
    // The least significant bit is used the unique lock bit
    // 0x000...01 means unique lock being held
    // The other bits are used to count the number of readers
    std::atomic<uint32_t> _status;
};

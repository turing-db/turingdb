#pragma once

#include <pthread.h>

// This class is a wrapper around the pthread spinlock.
// It should be used to lock short operations and when
// the waiting time is estimated to be very short.
// The spinlock does not make system calls or context switches
// during the lock or unlock operations, as the locked threads are waiting
// actively around the lock.
class SpinLock {
public:
    SpinLock();
    ~SpinLock();

    void lock();
    void unlock();

private:
    pthread_spinlock_t _lock;
};
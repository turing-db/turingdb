#include "SpinLock.h"

#include "BioAssert.h"

SpinLock::SpinLock()
{
    const bool res = (pthread_spin_init(&_lock, PTHREAD_PROCESS_PRIVATE) == 0);
    bioassert(res);
}

SpinLock::~SpinLock() {
    const bool res = (pthread_spin_destroy(&_lock) == 0);
    bioassert(res);
}

void SpinLock::lock() {
    const bool res = (pthread_spin_lock(&_lock) == 0);
    bioassert(res);
}

void SpinLock::unlock() {
    const bool res = (pthread_spin_unlock(&_lock) == 0);
    bioassert(res);
}
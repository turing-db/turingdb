#include "RWSpinLock.h"

#include <thread>

static constexpr uint32_t UNIQUE_LOCKED = 1u;
static constexpr uint32_t READER = 2u;

namespace {

inline void yield() {
    #if defined(__x86_64__)
    __builtin_ia32_pause();
    #else
    std::this_thread::yield();
    #endif
}

}

RWSpinLock::RWSpinLock()
{
}

RWSpinLock::~RWSpinLock() {
}

void RWSpinLock::lock() {
    for (;;) {
        // We try to get the UNIQUE lock
        const auto phase1 = _status.fetch_or(UNIQUE_LOCKED, std::memory_order_acquire); 
        if (phase1 == 0) {
            [[likely]]
            // We were granted the unique lock (no previous unique bit set) 
            // and no current readers because the previous status was zero
            return;
        }

        if ((phase1 & UNIQUE_LOCKED) != UNIQUE_LOCKED) {
            [[likely]]
            // We were granted the unique lock (no previous unique bit set)
            // but there are readers
            // because some bits were non-zero in the previous status.
            // So we have to wait for readers
            break;
        }

        // At this point, we know that the previous unique bit was one
        // so there is already a writer.

        // We do a yield loop to find a good time to go back to phase 1,
        // so that we go easy on the cache coherency traffic between cores.
        yield();
        for (;;) {
            const auto phase2 = _status.load(std::memory_order_relaxed);
            if ((phase2 & UNIQUE_LOCKED) != UNIQUE_LOCKED) {
                [[likely]]
                break;
            }

            yield();
        }
    }

    // The previous unique bit was zero in phase1 and there are readers
    // Wait for readers to leave
    yield();
    for (;;) {
        const auto phase3 = _status.load(std::memory_order_relaxed);
        if (phase3 == UNIQUE_LOCKED) {
            return;
        }

        yield();
    }
}

void RWSpinLock::lock_shared() {
    for (;;) {
        // We assume that are alone, add one reader
        const auto phase1 = _status.fetch_add(READER, std::memory_order_acquire);
        if ((phase1 & UNIQUE_LOCKED) != UNIQUE_LOCKED) {
            [[likely]]
            // We incremented the reader count without a unique lock being held
            return;
        }

        // At this point there was in fact a unique lock being held
        // Correct our assumption
        _status.fetch_sub(READER, std::memory_order_release);

        // There is a writer
        // Do a yield loop so that we go easy on cache coherency
        yield();
        for (;;) {
            const auto phase2 = _status.load(std::memory_order_relaxed);
            if ((phase2 & UNIQUE_LOCKED) != UNIQUE_LOCKED) {
                [[likely]]
                // The unique lock has been released
                break;
            }

            yield();
        }
    }
}

void RWSpinLock::unlock() {
    _status.fetch_and(~UNIQUE_LOCKED, std::memory_order_release);
}

void RWSpinLock::unlock_shared() {
    _status.fetch_sub(READER, std::memory_order_release);
}

#include "JobSystem.h" // include our interface

#include <algorithm> // std::max
#include <assert.h>
#include <atomic>             // to use std::atomic<uint64_t>
#include <condition_variable> // to use std::condition_variable
#include <iostream>
#include <shared_mutex>
#include <sstream>
#include <thread> // to use std::thread
#include <unordered_set>

struct Job {
    uint64_t label;
    std::function<void()> op;
};

// Fixed size very simple thread safe ring buffer
template <typename T, size_t capacity>
class ThreadSafeRingBuffer {
public:
    // Push an item to the end if there is free space
    //	Returns true if succesful
    //	Returns false if there is not enough space
    inline bool push_back(const T& item) {
        bool result = false;
        lock.lock();
        size_t next = (head + 1) % capacity;
        if (next != tail) {
            data[head] = item;
            head = next;
            result = true;
        }
        lock.unlock();
        return result;
    }

    // Get an item if there are any
    //	Returns true if succesful
    //	Returns false if there are no items
    inline bool pop_front(T& item) {
        bool result = false;
        lock.lock();
        if (tail != head) {
            item = data[tail];
            tail = (tail + 1) % capacity;
            result = true;
        }
        lock.unlock();
        return result;
    }

private:
    std::array<T, capacity> data;
    size_t head = 0;
    size_t tail = 0;
    std::mutex lock; // this just works better than a spinlock here (on windows)
};

namespace JobSystem {
uint32_t numThreads = 0;                // number of worker threads, it will be initialized in the Initialize() function
ThreadSafeRingBuffer<Job, 256> jobPool; // a thread safe queue to put pending jobs onto the end (with a capacity of 256 jobs). A worker thread can grab a job from the beginning
std::condition_variable wakeCondition;  // used in conjunction with the wakeMutex below. Worker threads just sleep when there is no job, and the main thread can wake them up
std::mutex wakeMutex;                   // used in conjunction with the wakeCondition above
uint64_t currentLabel = 0;              // tracks the state of execution of the main thread
std::atomic<uint64_t> finishedLabel;    // track the state of execution across background worker threads
std::vector<std::jthread> threads;
std::atomic<bool> stopRequested = false;
std::mutex finishedLabelsMutex;
std::unordered_set<uint64_t> finishedLabels;

void Initialize() {
    // Initialize the worker execution state to 0:
    finishedLabel.store(0);

    // Retrieve the number of hardware threads in this system:
    auto numCores = std::thread::hardware_concurrency();

    // Calculate the actual number of worker threads we want:
    numThreads = std::max(1u, numCores);

    // Create all our worker threads while immediately starting them:
    for (uint32_t threadID = 0; threadID < numThreads; ++threadID) {
        threads.emplace_back([] {
            Job job; // the current job for the thread, it's empty at start.

            // This is the infinite loop that a worker thread will do
            while (true) {
                if (stopRequested) {
                    return;
                }

                if (jobPool.pop_front(job)) // try to grab a job from the jobPool queue
                {
                    // It found a job, execute it:
                    job.op();                   // execute job
                    finishedLabel.fetch_add(1); // update worker label state
                    std::unique_lock lock(finishedLabelsMutex);
                    finishedLabels.emplace(job.label);
                } else {
                    // no job, put thread to sleep
                    std::unique_lock<std::mutex> lock(wakeMutex);
                    wakeCondition.wait(lock);
                }
            }
        });
    }
}

// This little helper function will not let the system to be deadlocked while the main thread is waiting for something
inline void poll() {
    wakeCondition.notify_one(); // wake one worker thread
    std::this_thread::yield();  // allow this thread to be rescheduled
}

uint64_t Submit(const std::function<void()>& fn,
                const std::vector<uint64_t>& dependencies) {
    // The main thread label state is updated:
    currentLabel += 1;

    // Check if dependencies are met
    while (!std::all_of(dependencies.begin(), dependencies.end(),
                        [&](uint64_t dep) {
                            std::unique_lock lock(finishedLabelsMutex);
                            return finishedLabels.find(dep) != finishedLabels.end();
                        })) {
        poll();
    }

    Job job {
        .label = currentLabel,
        .op = fn,
    };

    // Try to push a new job until it is pushed successfully:
    while (!jobPool.push_back(job)) {
        poll();
    }

    wakeCondition.notify_one(); // wake one thread

    return currentLabel;
}

bool IsBusy() {
    // Whenever the main thread label is not reached by the workers, it indicates that some worker is still alive
    return finishedLabel.load() < currentLabel;
}

void Wait() {
    while (IsBusy()) {
        poll();
    }
}

void Terminate() {
    while (IsBusy()) {
        poll();
    }
    stopRequested.store(true, std::memory_order_consume);
    wakeCondition.notify_all();
}

}

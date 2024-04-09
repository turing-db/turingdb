#pragma once

#include <cstdint>
#include <functional>

// A Dispatched job will receive this as function argument:
struct JobDispatchArgs {
    uint32_t jobIndex;
    uint32_t groupIndex;
};

namespace JobSystem {
// Create the internal resources such as worker threads, etc. Call it once when initializing the application.
void Initialize();

// Add a job to execute asynchronously. Any idle thread will execute this job.
uint64_t Submit(const std::function<void()>& fn,
                const std::vector<uint64_t>& dependencies = {});

// Check if any threads are working currently or not
bool IsBusy();

// Wait until all threads become idle
void Wait();

void Terminate();
}

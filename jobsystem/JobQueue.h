#pragma once

#include <optional>
#include <queue>

#include "Job.h"

namespace js {

class JobQueue {
public:
    Job& push(Job job) {
        return _jobs.emplace(std::move(job));
    }

    std::optional<Job> pop() {
        if (_jobs.empty()) {
            return std::nullopt;
        }

        Job job = std::move(_jobs.front());
        _jobs.pop();
        return job;
    }

    size_t size() const {
        return _jobs.size();
    }

    bool empty() const {
        return _jobs.empty();
    }


private:
    std::queue<Job> _jobs;
};

}

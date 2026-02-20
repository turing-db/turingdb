#pragma once

#include <cstddef>
#include <mutex>

class SharedStorage {
public:
    void inc() {
        std::scoped_lock lock(_mutex);
        _sum++;
    }

    size_t sum() const {
        std::scoped_lock lock(_mutex);
        return _sum;
    }

private:
    mutable std::mutex _mutex;
    size_t _sum {0};
};


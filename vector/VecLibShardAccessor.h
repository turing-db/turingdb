#pragma once

#include <mutex>

namespace vec {

struct VecLibShard;

class VecLibShardAccessor {
public:
    VecLibShardAccessor() = default;

    explicit VecLibShardAccessor(VecLibShard* shard);
    ~VecLibShardAccessor();

    VecLibShardAccessor(const VecLibShardAccessor&) = delete;
    VecLibShardAccessor(VecLibShardAccessor&&) = delete;
    VecLibShardAccessor& operator=(const VecLibShardAccessor&) = delete;
    VecLibShardAccessor& operator=(VecLibShardAccessor&&) = delete;

    VecLibShard* operator->() { return _shard; }
    const VecLibShard* operator->() const { return _shard; }

    VecLibShard& operator*() { return *_shard; }
    const VecLibShard& operator*() const { return *_shard; }

private:
    VecLibShard* _shard {nullptr};
    std::unique_lock<std::mutex> _lock;
};

}

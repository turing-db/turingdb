#pragma once

#include <mutex>

#include "AgingRingCacheHandle.h"
#include "ShardIdentifier.h"

namespace vec {

struct VecLibShard;

class VecLibShardAccessor {
public:
    using Handle = AgingRingCacheHandle<ShardIdentifier,
                                        std::unique_ptr<VecLibShard>,
                                        ShardIdentifier::Hash,
                                        ShardIdentifier::Equal>;
    VecLibShardAccessor() = default;

    explicit VecLibShardAccessor(Handle&& handle);
    ~VecLibShardAccessor();

    VecLibShardAccessor(const VecLibShardAccessor&) = delete;
    VecLibShardAccessor(VecLibShardAccessor&&) = delete;
    VecLibShardAccessor& operator=(const VecLibShardAccessor&) = delete;
    VecLibShardAccessor& operator=(VecLibShardAccessor&&) = delete;

    VecLibShard& get() { return **_handle; }
    const VecLibShard& get() const { return **_handle; }

private:
    std::unique_lock<std::mutex> _lock;
    Handle _handle;
};

}

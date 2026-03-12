#pragma once

#include <memory>
#include <shared_mutex>

#include "ShardIdentifier.h"
#include "VecLibShardAccessor.h"

template <typename Key,
          typename Payload,
          typename Hash,
          typename Equal>
    requires std::is_default_constructible_v<Payload>
class AgingRingCache;

namespace vec {

class StorageManager;

class ShardCache {
public:
    using Cache = AgingRingCache<ShardIdentifier, 
                                 std::unique_ptr<VecLibShard>,
                                 ShardIdentifier::Hash,
                                 ShardIdentifier::Equal>;

    explicit ShardCache(StorageManager& storageManager);
    ~ShardCache() noexcept;

    ShardCache(const ShardCache&) = delete;
    ShardCache(ShardCache&&) = delete;
    ShardCache& operator=(const ShardCache&) = delete;
    ShardCache& operator=(ShardCache&&) = delete;

    [[nodiscard]] VecLibShardAccessor getShard(const VecLibMetadata& meta, LSHSignature signature);

    void evictShard(const ShardIdentifier& id);
    void setMemLimit(ssize_t memLimit);

private:
    StorageManager* _storageManager {nullptr};
    std::unique_ptr<Cache> _cache;
};

}

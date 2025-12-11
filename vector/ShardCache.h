#pragma once

#include <list>
#include <memory>
#include <shared_mutex>
#include <unordered_map>

#include "LSHSignature.h"
#include "VecLibMetadata.h"

namespace vec {

class VecLibShard;
class StorageManager;

class ShardCache {
public:
    struct ShardIdentifier {
        VecLibID _libID {};
        LSHSignature _signature {};

        struct Hash {
            std::size_t operator()(const ShardIdentifier& id) const {
                constexpr std::hash<VecLibID> h;
                return h(id._libID) ^ h(id._signature);
            }
        };

        struct Equal {
            bool operator()(const ShardIdentifier& lhs, const ShardIdentifier& rhs) const {
                return lhs._libID == rhs._libID
                    && lhs._signature == rhs._signature;
            }
        };

        template <typename Value>
        using Map = std::unordered_map<ShardIdentifier, Value, Hash, Equal>;
    };

    explicit ShardCache(StorageManager& storageManager);
    ~ShardCache() noexcept;

    ShardCache(const ShardCache&) = delete;
    ShardCache(ShardCache&&) = delete;
    ShardCache& operator=(const ShardCache&) = delete;
    ShardCache& operator=(ShardCache&&) = delete;

    [[nodiscard]] VecLibShard& getShard(const VecLibMetadata& meta, LSHSignature signature);

    void updateMemUsage();

    void setMemLimit(ssize_t memLimit) {
        _memLimit = memLimit;
    }

private:
    mutable std::shared_mutex _mutex;
    StorageManager* _storageManager {nullptr};

    struct ShardEntry {
        std::unique_ptr<VecLibShard> shard;
        size_t accessCount {0};
    };

    using AccessedList = std::list<std::pair<ShardIdentifier, ShardEntry>>;
    AccessedList _accessed;
    ShardIdentifier::Map<AccessedList::iterator> _accessedMap;

    ssize_t _memLimit {256ull * 1024 * 1024}; // 20 GB
    ssize_t _memUsage {0};

    ssize_t evictOne();
};

}

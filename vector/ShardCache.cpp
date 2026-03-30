#include "ShardCache.h"

#include <mutex>

#include <faiss/IndexFlat.h>
#include <spdlog/spdlog.h>

#include "StorageManager.h"
#include "VecLibShard.h"
#include "VecLibShardAccessor.h"
#include "AgingRingCache.h"

#include "BioAssert.h"
#include "VectorException.h"

using namespace vec;

namespace {

struct LoadData {
    const StorageManager& _storageManager;
    const VecLibMetadata& _meta;
};

bool onEvict(const ShardIdentifier& id,
             std::unique_ptr<VecLibShard>& shard) {
    const std::unique_lock lock(shard->_mutex);

    if (auto res = shard->save(); !res) {
        spdlog::error(fmt::format("Could not save shard '{}'.\n{}",
                                  id._libID, res.error().fmtMessage()));
        return false;
    }

    return true;
}

bool onLoad(const ShardIdentifier& id,
            std::unique_ptr<VecLibShard>& shard,
            void* data) {
    const auto& [storageManager, meta] = *static_cast<LoadData*>(data);

    const fs::Path indexPath = storageManager.getShardPath(id._libID, id._signature);

    shard = std::make_unique<VecLibShard>();
    shard->_indexPath = indexPath;

    if (auto res = shard->load(meta); !res) {
        spdlog::error(fmt::format("Could not load shard '{}'.\n{}",
                                  indexPath.c_str(), res.error().fmtMessage()));
        return false;
    }

    return true;
}

size_t calculateMemoryUsage(const std::unique_ptr<VecLibShard>& shard) {
    return shard->getUsedMem();
}

}

ShardCache::ShardCache(StorageManager& storageManager)
    : _storageManager(&storageManager),
    _cache(std::make_unique<Cache>())
{
    _cache->setOnLoad(onLoad);
    _cache->setOnEvict(onEvict);
    _cache->setCalculateMemoryUsage(calculateMemoryUsage);
    _cache->setMaxMemUsage(10ull * 1024 * 1024 * 1024); // 10 GB
}

ShardCache::~ShardCache() noexcept {
    _cache->shutdown();
}

VecLibShardAccessor ShardCache::getShard(const VecLibMetadata& meta, LSHSignature signature) {
    const ShardIdentifier id(meta._id, signature);

    LoadData data {*_storageManager, meta};

    auto handle = _cache->acquire(id, &data);
    if (!handle) {
        throw VectorException(fmt::format("Could not acquire shard '{}'.\n{}",
                                          meta._id, handle.error().fmtMessage()));
    }

    return VecLibShardAccessor(std::move(*handle));
}

void ShardCache::evictShard(const ShardIdentifier& id) {
    _cache->tryEvict(id);
}

void ShardCache::setMemLimit(ssize_t memLimit) {
    bioassert(memLimit > 0, "Shard cache memory limit must be positive");
    _cache->setMaxMemUsage(static_cast<size_t>(memLimit));
}

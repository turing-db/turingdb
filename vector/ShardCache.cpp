#include "ShardCache.h"

#include <mutex>

#include <faiss/IndexFlat.h>

#include "StorageManager.h"
#include "VecLibShard.h"
#include "VecLibShardAccessor.h"

#include "BioAssert.h"
#include "VectorException.h"

using namespace vec;

ShardCache::ShardCache(StorageManager& storageManager)
    : _storageManager(&storageManager)
{
}

ShardCache::~ShardCache() noexcept {
    for (auto& [id, it] : _accessedMap) {
        try {
            const std::unique_lock shardLock(it->second._shard->_mutex);
            it->second._shard->save();
        } catch (...) {
            fmt::println("Error writing shard {}", id._signature);
        }
    }
}

VecLibShardAccessor ShardCache::getShard(const VecLibMetadata& meta, LSHSignature signature) {
    const std::unique_lock lock(_mutex);

    const ShardIdentifier id(meta._id, signature);

    auto it = _accessedMap.find(id);

    // If already in cache, increment access count and return
    if (it != _accessedMap.end()) {
        it->second->second._accessCount++;
        return VecLibShardAccessor(it->second->second._shard.get());
    }

    // If not in cache, load/create the shard
    const fs::Path indexPath = _storageManager->getShardPath(meta._id, signature);
    const fs::Path idsPath = _storageManager->getExternalIDsPath(meta._id, signature);

    auto shard = std::make_unique<VecLibShard>();
    shard->_indexPath = indexPath;

    if (auto res = fs::File::createAndOpen(idsPath); !res) {
        throw VectorException(fmt::format("Could not open shard ids file '{}'.\n{}",
                                          idsPath.c_str(), res.error().fmtMessage()));
    } else {
        shard->_idsFile = std::move(res.value());
    }

    shard->_idsWriter = fs::FileWriter<4096>();
    shard->_idsReader = fs::FileReader();
    shard->_idsReader.setFile(&shard->_idsFile);
    shard->_idsWriter.setFile(&shard->_idsFile);

    if (auto res = shard->load(meta); !res) {
        throw VectorException(fmt::format("Could not load shard ids file '{}'.\n{}",
                                          idsPath.c_str(), res.error().fmtMessage()));
    }

    const size_t memUsage = shard->getUsedMem();
    _memUsage += memUsage;

    ShardEntry entry;
    entry._shard = std::move(shard);
    entry._accessCount = 1;

    _accessed.emplace_front(id, std::move(entry));
    _accessedMap[id] = _accessed.begin();

    while (_memUsage > _memLimit && _accessed.size() > 1) {
        _memUsage -= evictOne();
    }

    return VecLibShardAccessor(_accessedMap.at(id)->second._shard.get());
}

void ShardCache::updateMemUsage() {
    const std::unique_lock lock(_mutex);

    ssize_t memUsage = 0;

    for (auto& [id, it] : _accessedMap) {
        memUsage += it->second._shard->getUsedMem();
    }

    while (memUsage > _memLimit && _accessed.size() > 1) {
        memUsage -= evictOne();
    }

    bioassert(memUsage >= 0, "Shard cache memory usage cannot be negative");

    _memUsage = memUsage;
}

void ShardCache::evictLibraryShards(VecLibID libID) {
    const std::unique_lock lock(_mutex);

    for (auto it = _accessedMap.begin(); it != _accessedMap.end();) {
        if (it->first._libID == libID) {
            {
                const std::unique_lock shardLock(it->second->second._shard->_mutex);
                it->second->second._shard->save();
            }
            _accessed.erase(it->second);
            it = _accessedMap.erase(it);
        } else {
            ++it;
        }
    }
}

void ShardCache::flush() {
    const std::unique_lock lock(_mutex);

    for (auto& [id, it] : _accessedMap) {
        const std::unique_lock shardLock(it->second._shard->_mutex);
        it->second._shard->save();
    }
}

ssize_t ShardCache::evictOne() {
    bioassert(!_accessed.empty(), "Shard cache is empty");

    // Find the unlocked shard with the lowest access count
    auto victim = _accessed.end();
    size_t minAccessCount = SIZE_MAX;
    std::unique_lock<std::mutex> victimLock;

    for (auto it = _accessed.begin(); it != _accessed.end(); ++it) {
        if (it->second._accessCount >= minAccessCount) {
            continue;
        }

        std::unique_lock<std::mutex> lock(it->second._shard->_mutex, std::try_to_lock);
        if (!lock.owns_lock()) {
            continue;
        }

        // Better candidate found — take ownership of its lock, releasing the previous winner
        victimLock = std::move(lock);
        victim = it;
        minAccessCount = it->second._accessCount;
    }

    if (victim == _accessed.end()) {
        return 0; // all shards in use, nothing to evict
    }

    victim->second._shard->save();

    const ssize_t freedMem = victim->second._shard->getUsedMem();
    _accessedMap.erase(victim->first);
    _accessed.erase(victim);

    return freedMem;
}

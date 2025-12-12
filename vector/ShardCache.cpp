#include "ShardCache.h"

#include <faiss/IndexFlat.h>
#include <mutex>

#include "StorageManager.h"
#include "VecLibShard.h"

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
            it->second.shard->save();
        } catch (...) {
            fmt::println("Error writing shard {}", id._signature);
        }
    }
}

VecLibShard& ShardCache::getShard(const VecLibMetadata& meta, LSHSignature signature) {
    std::unique_lock lock {_mutex};

    const ShardIdentifier id {meta._id, signature};

    auto it = _accessedMap.find(id);

    // If already in cache, increment access count and return
    if (it != _accessedMap.end()) {
        it->second->second.accessCount++;
        return *it->second->second.shard;
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
    entry.shard = std::move(shard);
    entry.accessCount = 1;

    _accessed.emplace_front(id, std::move(entry));
    _accessedMap[id] = _accessed.begin();

    while (_memUsage > _memLimit && _accessed.size() > 1) {
        _memUsage -= evictOne();
    }

    return *_accessed.front().second.shard;
}

void ShardCache::updateMemUsage() {
    std::unique_lock lock {_mutex};

    ssize_t memUsage = 0;

    for (auto& [id, it] : _accessedMap) {
        memUsage += it->second.shard->getUsedMem();
    }

    while (memUsage > _memLimit && _accessed.size() > 1) {
        memUsage -= evictOne();
    }

    bioassert(memUsage >= 0, "Shard cache memory usage cannot be negative");

    _memUsage = memUsage;
}

void ShardCache::evictLibraryShards(VecLibID libID) {
    std::unique_lock lock {_mutex};

    for (auto it = _accessedMap.begin(); it != _accessedMap.end();) {
        if (it->first._libID == libID) {
            it->second->second.shard->save();
            it = _accessedMap.erase(it);
        } else {
            ++it;
        }
    }
}

ssize_t ShardCache::evictOne() {
    bioassert(!_accessed.empty(), "Shard cache is empty");

    // Find shard with lowest access count
    auto victim = _accessed.begin();
    size_t minAccessCount = victim->second.accessCount;

    for (auto it = _accessed.begin(); it != _accessed.end(); ++it) {
        if (it->second.accessCount < minAccessCount) {
            minAccessCount = it->second.accessCount;
            victim = it;
        }
    }

    victim->second.shard->save();

    ssize_t freedMem = victim->second.shard->getUsedMem();
    _accessedMap.erase(victim->first);
    _accessed.erase(victim);

    return freedMem;
}

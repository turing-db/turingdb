#pragma once

#include "ID.h"
#include "RWSpinLock.h"

#include <mutex>
#include <shared_mutex>
#include <unordered_map>

namespace db {

class IDMapper {
public:
    void registerID(uint64_t rawID, NodeID dbID) {
        std::unique_lock guard(_lock);
        _idMapper.emplace(rawID, dbID);
    }

    NodeID getID(uint64_t rawID) const {
        std::shared_lock guard(_lock);
        return _idMapper.at(rawID);
    }

    void reserve(size_t count) {
        std::unique_lock guard(_lock);
        _idMapper.reserve(count);
    }

private:
    mutable RWSpinLock _lock;
    std::unordered_map<uint64_t, NodeID> _idMapper;
};

}

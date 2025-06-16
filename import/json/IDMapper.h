#pragma once

#include "ID.h"
#include "RWSpinLock.h"

#include <mutex>
#include <shared_mutex>
#include <unordered_map>

namespace db {

class IDMapper {
public:
    struct NodeIdentifier {
        NodeID nodeID;
        size_t partIndex {};
    };

    void registerID(uint64_t rawID, size_t partIndex, NodeID dbID) {
        std::unique_lock guard(_lock);
        _idMapper.emplace(rawID, NodeIdentifier{dbID, partIndex});
    }

    NodeIdentifier getID(uint64_t rawID) const {
        std::shared_lock guard(_lock);
        return _idMapper.at(rawID);
    }

    void reserve(size_t count) {
        std::unique_lock guard(_lock);
        _idMapper.reserve(count);
    }

private:
    mutable RWSpinLock _lock;
    std::unordered_map<uint64_t, NodeIdentifier> _idMapper;
};

}

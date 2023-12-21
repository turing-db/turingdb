#pragma once

#include <mutex>
#include <shared_mutex>

#include "EntityID.h"
#include "NodeAccessor.h"

namespace db {

class Storage;

class StorageAccessor {
public:
    StorageAccessor(StorageAccessor&& other);
    ~StorageAccessor();

    StorageAccessor(const StorageAccessor&) = delete;
    StorageAccessor& operator=(const StorageAccessor&) = delete;
    StorageAccessor& operator=(StorageAccessor &&) = delete;

    NodeAccessor getNode(EntityID id) const;
    
private:
    Storage* _storage {nullptr};
    std::unique_lock<std::shared_mutex> _uniqueLock;
    std::shared_lock<std::shared_mutex> _sharedLock;

    StorageAccessor(Storage* storage);
};

}
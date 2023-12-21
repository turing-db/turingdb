#include "StorageAccessor.h"

#include "Storage.h"

using namespace db;

StorageAccessor::StorageAccessor(Storage* storage)
    : _storage(storage),
    _uniqueLock(storage->_mainLock, std::defer_lock),
    _sharedLock(storage->_mainLock)
{
}

StorageAccessor::StorageAccessor(StorageAccessor&& other)
    : _storage(other._storage),
    _uniqueLock(std::move(other._uniqueLock)),
    _sharedLock(std::move(other._sharedLock))
{
}

StorageAccessor::~StorageAccessor() {
}
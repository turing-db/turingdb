#include "VectorDatabase.h"

#include <mutex>

#include "RandomGenerator.h"
#include "ShardCache.h"
#include "StorageManager.h"
#include "VecLib.h"
#include "VecLibAccessor.h"

using namespace vec;

VectorDatabase::VectorDatabase()
{
}

VectorDatabase::~VectorDatabase() {
}

VectorResult<std::unique_ptr<VectorDatabase>> VectorDatabase::create(const fs::Path& rootPath) {
    if (!RandomGenerator::initialized()) {
        RandomGenerator::initialize();
    }

    std::unique_ptr<VectorDatabase> database {new VectorDatabase};

    auto storage = StorageManager::create(rootPath);
    if (!storage) {
        return storage.get_unexpected();
    }

    database->_storageManager = std::move(storage.value());
    database->_shardCache = std::make_unique<ShardCache>(*database->_storageManager);

    // Build the libraries using the storage
    if (auto res = database->load(); !res) {
        return res.get_unexpected();
    }

    return database;
}

VectorResult<VecLibID> VectorDatabase::createLibrary(std::string_view libName,
                                                     Dimension dim,
                                                     DistanceMetric metric) {
    if (libName.empty()) {
        return VectorError::result(VectorErrorCode::EmptyLibName);
    }

    std::unique_lock lock {_mutex};

    if (_vecLibIDs.contains(libName)) {
        return VectorError::result(VectorErrorCode::LibraryAlreadyExists);
    }

    VecLibID id = RandomGenerator::generateUnique<uint64_t>([&](uint64_t v) {
        return _vecLibs.contains(v);
    });

    auto lib = VecLib::Builder()
                   .setStorage(_storageManager.get())
                   .setShardCache(_shardCache.get())
                   .setID(id)
                   .setName(libName)
                   .setDimension(dim)
                   .setMetric(metric)
                   .build();

    if (!lib) {
        return lib.get_unexpected();
    }

    _vecLibIDs.emplace(lib.value()->name(), id);
    _vecLibs.emplace(id, std::move(lib.value()));

    return id;
}

VectorResult<void> VectorDatabase::deleteLibrary(std::string_view libName) {
    std::unique_lock lock {_mutex};

    const auto itID = _vecLibIDs.find(libName);

    if (itID == _vecLibIDs.end()) {
        return VectorError::result(VectorErrorCode::LibraryDoesNotExist);
    }

    const VecLibID libID = itID->second;

    if (!_vecLibs.contains(libID)) {
        return VectorError::result(VectorErrorCode::LibraryDoesNotExist);
    }

    _shardCache->evictLibraryShards(libID);

    if (auto res = _storageManager->deleteLibraryStorage(libID); !res) {
        return res.get_unexpected();
    }

    _vecLibs.erase(libID);
    _vecLibIDs.erase(itID);

    return {};
}

void VectorDatabase::listLibraries(std::vector<VecLibID>& out) const {
    std::shared_lock lock {_mutex};

    for (const auto& [id, lib] : _vecLibs) {
        out.push_back(id);
    }
}

bool VectorDatabase::libraryExists(const VecLibID& libID) const {
    std::shared_lock lock {_mutex};

    return _vecLibs.contains(libID);
}

bool VectorDatabase::libraryExists(std::string_view libName) const {
    std::shared_lock lock {_mutex};

    return _vecLibIDs.contains(libName);
}

VecLibAccessor VectorDatabase::getLibrary(const VecLibID& libID) {
    std::shared_lock lock {_mutex};

    auto it = _vecLibs.find(libID);
    if (it == _vecLibs.end()) {
        return VecLibAccessor {};
    }

    return it->second->access();
}

VecLibAccessor VectorDatabase::getLibrary(std::string_view libName) {
    std::shared_lock lock {_mutex};

    auto it = _vecLibIDs.find(libName);
    if (it == _vecLibIDs.end()) {
        return VecLibAccessor {};
    }

    return getLibrary(it->second);
}

VectorResult<void> VectorDatabase::load() {
    // Load libraries from storage
    for (const auto& [id, storage] : *_storageManager) {
        auto lib = VecLib::Loader()
                       .setStorageManager(_storageManager.get())
                       .setShardCache(_shardCache.get())
                       .load(*storage);

        if (!lib) {
            return lib.get_unexpected();
        }

        _vecLibIDs.emplace(lib.value()->name(), id);
        _vecLibs.emplace(id, std::move(lib.value()));
    }

    return {};
}

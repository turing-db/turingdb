#include "VectorDatabase.h"

#include <mutex>

#include "RandomGenerator.h"
#include "ShardCache.h"
#include "VectorStorageManager.h"
#include "VecLib.h"
#include "VecLibAccessor.h"
#include "VecLibWriteAccessor.h"

using namespace vec;

VectorDatabase::VectorDatabase()
{
}

VectorDatabase::~VectorDatabase() {
}

VectorResult<void> VectorDatabase::init(const fs::Path& rootPath) {
    static constexpr uint64_t RANDOM_SEED = 982451653;

    if (!RandomGenerator::initialized()) {
        RandomGenerator::initialize(RANDOM_SEED);
    }

    auto storage = VectorStorageManager::create(rootPath);
    if (!storage) {
        return nonstd::make_unexpected(storage.error());
    }

    _storageManager = std::move(storage.value());
    _shardCache = std::make_unique<ShardCache>(*_storageManager);

    // Build the libraries using the storage
    if (auto res = load(); !res) {
        return nonstd::make_unexpected(res.error());
    }

    return {};
}

VectorResult<VecLibID> VectorDatabase::createLibrary(std::string_view libName,
                                                     Dimension dim,
                                                     DistanceMetric metric) {
    if (libName.empty()) {
        return VectorError::result(VectorErrorCode::EmptyLibName);
    }

    const std::unique_lock lock(_mutex);

    if (_vecLibIDs.contains(libName)) {
        return VectorError::result(VectorErrorCode::LibraryAlreadyExists);
    }

    const VecLibID id = RandomGenerator::generateUnique<uint64_t>([&](uint64_t v) {
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
        return nonstd::make_unexpected(lib.error());
    }

    _vecLibIDs.emplace(lib.value()->name(), id);
    _vecLibs.emplace(id, std::move(lib.value()));

    return id;
}

VectorResult<void> VectorDatabase::deleteLibrary(std::string_view libName) {
    const std::unique_lock lock(_mutex);

    const auto itID = _vecLibIDs.find(libName);

    if (itID == _vecLibIDs.end()) {
        return VectorError::result(VectorErrorCode::LibraryDoesNotExist);
    }

    const VecLibID libID = itID->second;

    auto it = _vecLibs.find(libID);
    if (it == _vecLibs.end()) {
        return VectorError::result(VectorErrorCode::LibraryDoesNotExist);
    }

    // Extract the library
    std::unique_ptr<VecLib> lib = std::move(it->second);
    {
        const std::unique_lock libLock(lib->_mutex);

        // Perform the deletion

        lib->evictAllShards();

        if (auto res = _storageManager->deleteLibraryStorage(libID); !res) {
            return nonstd::make_unexpected(res.error());
        }

        _vecLibs.erase(libID);
        _vecLibIDs.erase(itID);
    }

    // The library is deleted at the end of the scope

    return {};
}

void VectorDatabase::listLibraries(std::vector<VecLibID>& out) const {
    const std::shared_lock lock(_mutex);

    for (const auto& [id, lib] : _vecLibs) {
        out.push_back(id);
    }
}

bool VectorDatabase::libraryExists(const VecLibID& libID) const {
    const std::shared_lock lock(_mutex);

    return _vecLibs.contains(libID);
}

bool VectorDatabase::libraryExists(std::string_view libName) const {
    const std::shared_lock lock(_mutex);

    return _vecLibIDs.contains(libName);
}

VecLibAccessor VectorDatabase::getLibrary(const VecLibID& libID) {
    const std::shared_lock lock(_mutex);

    // Find the library
    const auto it = _vecLibs.find(libID);
    if (it == _vecLibs.end()) {
        return VecLibAccessor();
    }

    // Request read access
    return it->second->access();
}

VecLibAccessor VectorDatabase::getLibrary(std::string_view libName) {
    const std::shared_lock lock(_mutex);

    // Find the library ID
    const auto itID = _vecLibIDs.find(libName);
    if (itID == _vecLibIDs.end()) {
        return VecLibAccessor();
    }

    // Find the library itself
    const auto it = _vecLibs.find(itID->second);
    if (it == _vecLibs.end()) {
        return VecLibAccessor();
    }

    // Request read access
    return it->second->access();
}

VecLibWriteAccessor VectorDatabase::getLibraryForWrite(const VecLibID& libID) {
    const std::shared_lock lock(_mutex);

    const auto it = _vecLibs.find(libID);
    if (it == _vecLibs.end()) {
        return VecLibWriteAccessor {};
    }

    return VecLibWriteAccessor {*it->second};
}

VecLibWriteAccessor VectorDatabase::getLibraryForWrite(std::string_view libName) {
    const std::shared_lock lock(_mutex);

    const auto itID = _vecLibIDs.find(libName);
    if (itID == _vecLibIDs.end()) {
        return VecLibWriteAccessor {};
    }

    const auto it = _vecLibs.find(itID->second);
    if (it == _vecLibs.end()) {
        return VecLibWriteAccessor {};
    }

    return VecLibWriteAccessor {*it->second};
}

void VectorDatabase::listLibraryNames(std::vector<std::string>& out) const {
    const std::shared_lock lock(_mutex);

    out.clear();
    out.reserve(_vecLibs.size());

    for (const auto& [id, lib] : _vecLibs) {
        out.emplace_back(lib->name());
    }
}

VectorResult<void> VectorDatabase::load() {
    // Load libraries from storage
    for (const auto& [id, storage] : *_storageManager) {
        auto lib = VecLib::Loader()
                       .setStorageManager(_storageManager.get())
                       .setShardCache(_shardCache.get())
                       .load(*storage);

        if (!lib) {
            return nonstd::make_unexpected(lib.error());
        }

        _vecLibIDs.emplace(lib.value()->name(), id);
        _vecLibs.emplace(id, std::move(lib.value()));
    }

    return {};
}

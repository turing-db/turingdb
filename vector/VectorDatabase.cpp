#include "VectorDatabase.h"

#include <mutex>

#include "RandomGenerator.h"
#include "ShardCache.h"
#include "StorageManager.h"
#include "VecLib.h"

using namespace vec;

VectorDatabase::VectorDatabase() {
}

VectorDatabase::~VectorDatabase() {
}

VectorResult<std::unique_ptr<VectorDatabase>> VectorDatabase::create(const fs::Path& rootPath) {
    RandomGenerator::initialize();

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

    std::unique_ptr<VecLib> lib = VecLib::Builder()
                                      .setStorage(_storageManager.get())
                                      .setShardCache(_shardCache.get())
                                      .setID(id)
                                      .setName(libName)
                                      .setDimension(dim)
                                      .setMetric(metric)
                                      .build();

    _vecLibIDs.emplace(lib->name(), id);
    _vecLibs.emplace(id, std::move(lib));

    return id;
}

VectorResult<void> VectorDatabase::deleteLibrary(const VecLibID& libID) {
    std::unique_lock lock {_mutex};

    if (!_vecLibs.contains(libID)) {
        return VectorError::result(VectorErrorCode::LibraryDoesNotExist);
    }

    _vecLibs.erase(libID);

    // TODO Here delete library storage

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

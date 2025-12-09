#include "VectorDatabase.h"

#include <mutex>

#include "RandomGenerator.h"
#include "StorageManager.h"
#include "LSHShardRouter.h"
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

    return database;
}

VectorResult<VecLibID> VectorDatabase::createLibrary(std::string_view libName,
                                                     Dimension dim,
                                                     DistanceMetric metric) {
    if (libName.empty()) {
        return VectorError::result(VectorErrorCode::EmptyLibName);
    }

    std::unique_lock lock {_mutex};

    VecLibIdentifier identifier = VecLibIdentifier::alloc(libName);
    VecLibID id = identifier.view();

    // Random generation until we find a unique ID
    while (_vecLibs.contains(id)) {
        identifier = VecLibIdentifier::alloc(libName);
        id = identifier.view();
    }

    std::unique_ptr<VecLib> lib = VecLib::Builder()
                                      .setStorage(_storageManager.get())
                                      .setIdentifier(std::move(identifier))
                                      .setDimension(dim)
                                      .setMetric(metric)
                                      .build();

    _vecLibs.emplace(id, std::move(lib));

    return id;
}

VectorResult<void> VectorDatabase::deleteLibrary(const VecLibID& libID) {
    std::unique_lock lock {_mutex};

    if (!_vecLibs.contains(libID)) {
        return VectorError::result(VectorErrorCode::DoesNotExist);
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

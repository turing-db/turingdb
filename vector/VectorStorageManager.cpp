#include "VectorStorageManager.h"

#include <charconv>
#include <mutex>

#include <faiss/IndexFlat.h>
#include <faiss/index_io.h>

#include "LSHShardRouter.h"
#include "VecLibStorage.h"
#include "VecLib.h"

using namespace vec;

VectorStorageManager::VectorStorageManager()
{
}

VectorStorageManager::~VectorStorageManager() {
}

VectorResult<std::unique_ptr<VectorStorageManager>> VectorStorageManager::create(const fs::Path& rootPath) {
    auto storage = std::make_unique<VectorStorageManager>();
    storage->_rootPath = rootPath;

    const auto info = rootPath.getFileInfo();

    if (info) {
        // Already exists, check if it's a directory
        if (info->_type != fs::FileType::Directory) {
            return VectorError::result(VectorErrorCode::InvalidStorageRoot);
        }
    } else {
        // Doesn't exist, create it
        if (!rootPath.mkdir()) {
            return VectorError::result(VectorErrorCode::CouldNotCreateStorage);
        }
    }

    if (auto res = storage->initialize(); !res) {
        return nonstd::make_unexpected(res.error());
    }

    return storage;
}

VectorResult<void> VectorStorageManager::createLibraryStorage(const VecLib& lib) {
    const auto* meta = lib.metadata();

    const fs::Path libRoot = getLibraryPath(meta->_id);
    const fs::Path metadataPath = getMetadataPath(meta->_id);
    const fs::Path shardRouterPath = getShardRouterPath(meta->_id);

    std::unique_lock lock(_mutex);

    if (_storages.contains(meta->_id)) {
        return VectorError::result(VectorErrorCode::LibraryStorageAlreadyExists);
    }

    auto storage = std::make_unique<VecLibStorage>();

    // Create library directory
    if (auto res = libRoot.mkdir(); !res) {
        return VectorError::result(VectorErrorCode::CouldNotCreateLibraryStorage, res.error());
    }

    // Create metadata file
    if (auto res = fs::File::createAndOpen(metadataPath)) {
        storage->_metadataFile = std::move(res.value());
        storage->_metadataWriter.setFile(&storage->_metadataFile);
    } else {
        return VectorError::result(VectorErrorCode::CouldNotCreateMetadataStorage, res.error());
    }

    // Create shard router file
    if (auto res = fs::File::createAndOpen(shardRouterPath)) {
        storage->_shardRouterFile = std::move(res.value());
        storage->_shardRouterWriter.setFile(&storage->_shardRouterFile);
    } else {
        return VectorError::result(VectorErrorCode::CouldNotCreateShardRouterStorage, res.error());
    }

    // Write shard router file
    if (auto res = storage->_shardRouterWriter.write(lib.shardRouter()); !res) {
        return nonstd::make_unexpected(res.error());
    }

    // Write metadata file
    if (auto res = storage->_metadataWriter.write(meta); !res) {
        return nonstd::make_unexpected(res.error());
    }

    _storages.emplace(meta->_id, std::move(storage));

    return {};
}

VectorResult<void> VectorStorageManager::persistShardRouter(const VecLib& lib) {
    const auto* meta = lib.metadata();

    std::shared_lock lock(_mutex);

    const auto it = _storages.find(meta->_id);
    if (it == _storages.end()) {
        return VectorError::result(VectorErrorCode::LibraryDoesNotExist);
    }

    // Re-serialize the router so the shard signature set registered while adding
    // embeddings survives a restart. createLibraryStorage() only writes the empty
    // router at index-creation time; without this, an exact search after reload
    // would iterate an empty signature set and return no results.
    return it->second->_shardRouterWriter.write(lib.shardRouter());
}

bool VectorStorageManager::libraryExists(const VecLibID& libID) const {
    return getLibraryPath(libID).exists();
}

VectorResult<void> VectorStorageManager::deleteLibraryStorage(const VecLibID& libID) {
    const fs::Path libPath = getLibraryPath(libID);

    std::unique_lock lock(_mutex);

    if (!libPath.exists()) {
        return VectorError::result(VectorErrorCode::LibraryDoesNotExist);
    }

    if (auto res = libPath.rm(); !res) {
        return VectorError::result(VectorErrorCode::CouldNotDeleteLibraryStorage, res.error());
    }

    _storages.erase(libID);

    return {};
}

const VecLibStorage& VectorStorageManager::getStorage(const VecLibID& libID) const {
    std::shared_lock lock(_mutex);

    return *_storages.at(libID);
}

VecLibStorage& VectorStorageManager::getStorage(const VecLibID& libID) {
    std::shared_lock lock(_mutex);

    return *_storages.at(libID);
}

fs::Path VectorStorageManager::getLibraryPath(const VecLibID& libID) const {
    return _rootPath / std::to_string(libID);
}

fs::Path VectorStorageManager::getMetadataPath(const VecLibID& libID) const {
    return getLibraryPath(libID) / "metadata.json";
}

fs::Path VectorStorageManager::getShardRouterPath(const VecLibID& libID) const {
    return getLibraryPath(libID) / "shard_router.bin";
}

fs::Path VectorStorageManager::getShardPath(const VecLibID& libID, LSHSignature sig) const {
    return getLibraryPath(libID) / "embeddings-" + std::to_string(sig) + ".index";
}

VectorResult<void> VectorStorageManager::initialize() {
    auto listRes = _rootPath.listDir();
    if (!listRes) {
        return VectorError::result(VectorErrorCode::CouldNotListLibraries);
    }

    const std::vector<fs::Path> libPaths = std::move(listRes.value());

    for (const fs::Path& libPath : libPaths) {
        const auto info = libPath.getFileInfo();

        if (!info) {
            return VectorError::result(VectorErrorCode::CouldNotListLibraries);
        }

        if (info->_type != fs::FileType::Directory) {
            return VectorError::result(VectorErrorCode::NotVecLibFile);
        }

        // Parse library ID
        const std::string_view idStr = libPath.filename();
        uint64_t id = 0;
        const std::from_chars_result idRes = std::from_chars(idStr.begin(), idStr.end(), id);
        const bool idFailure = (idRes.ec == std::errc::result_out_of_range)
                            || (idRes.ec == std::errc::invalid_argument);
        const size_t charCount = std::distance(idStr.begin(), idRes.ptr);

        if (idFailure || charCount != idStr.size()) {
            return VectorError::result(VectorErrorCode::NotVecLibFile);
        }

        if (_storages.contains(id)) {
            return VectorError::result(VectorErrorCode::LibraryAlreadyLoaded);
        }

        auto storage = std::make_unique<VecLibStorage>();

        // Read metadata
        const fs::Path metadataPath = getMetadataPath(id);
        if (auto res = fs::File::open(metadataPath); !res) {
            return VectorError::result(VectorErrorCode::CouldNotOpenMetadataFile);
        } else {
            storage->_metadataFile = std::move(res.value());
            storage->_metadataWriter.setFile(&storage->_metadataFile);
        }

        // Read shard router
        const fs::Path shardRouterPath = getShardRouterPath(id);
        if (auto res = fs::File::open(shardRouterPath); !res) {
            return VectorError::result(VectorErrorCode::CouldNotOpenShardRouterFile);
        } else {
            storage->_shardRouterFile = std::move(res.value());
            storage->_shardRouterWriter.setFile(&storage->_shardRouterFile);
        }

        // Storage library storage
        _storages.emplace(id, std::move(storage));
    }

    return {};
}

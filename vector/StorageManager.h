#pragma once

#include "LSHSignature.h"
#include "Path.h"
#include "VecLibStorage.h"
#include "VectorResult.h"
#include "VecLibMetadata.h"

namespace vec {

class VecLib;
class LSHShardRouter;
struct VecLibMetadata;

class StorageManager {
public:
    using StorageMap = std::unordered_map<VecLibID, std::unique_ptr<VecLibStorage>>;

    StorageManager();
    ~StorageManager();

    [[nodiscard]] static VectorResult<std::unique_ptr<StorageManager>> create(const fs::Path& rootPath);

    StorageManager(const StorageManager&) = delete;
    StorageManager(StorageManager&&) = delete;
    StorageManager& operator=(const StorageManager&) = delete;
    StorageManager& operator=(StorageManager&&) = delete;

    [[nodiscard]] VectorResult<void> createLibraryStorage(const VecLib& lib);
    [[nodiscard]] bool libraryExists(const VecLibID& libID) const;

    [[nodiscard]] const VecLibStorage& getStorage(const VecLibID& libID) const;
    [[nodiscard]] VecLibStorage& getStorage(const VecLibID& libID);

    [[nodiscard]] fs::Path getLibraryPath(const VecLibID& libID) const;
    [[nodiscard]] fs::Path getMetadataPath(const VecLibID& libID) const;
    [[nodiscard]] fs::Path getShardRouterPath(const VecLibID& libID) const;
    [[nodiscard]] fs::Path getExternalIDsPath(const VecLibID& libID, LSHSignature sig) const;
    [[nodiscard]] fs::Path getShardPath(const VecLibID& libID, LSHSignature sig) const;

    [[nodiscard]] StorageMap::const_iterator begin() const {
        return _storages.begin();
    }

    [[nodiscard]] StorageMap::const_iterator end() const {
        return _storages.end();
    }

private:
    fs::Path _rootPath;
    StorageMap _storages;

    VectorResult<void> initialize();
};

}

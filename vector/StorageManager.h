#pragma once

#include "Path.h"
#include "VecLibIdentifier.h"
#include "VecLibStorage.h"
#include "VectorResult.h"

namespace vec {

class VecLib;
class VecLibMetadata;

class StorageManager {
public:
    StorageManager();
    ~StorageManager();

    [[nodiscard]] static VectorResult<std::unique_ptr<StorageManager>> create(const fs::Path& rootPath);

    StorageManager(const StorageManager&) = delete;
    StorageManager(StorageManager&&) = delete;
    StorageManager& operator=(const StorageManager&) = delete;
    StorageManager& operator=(StorageManager&&) = delete;

    [[nodiscard]] VectorResult<void> createLibraryStorage(const VecLib& lib);
    [[nodiscard]] VectorResult<void> loadLibraryStorage(const VecLib& lib);
    [[nodiscard]] bool libraryExists(const VecLibID& libID) const;

    [[nodiscard]] const VecLibStorage& getStorage(const VecLibID& libID) const;
    [[nodiscard]] VecLibStorage& getStorage(const VecLibID& libID);

    [[nodiscard]] fs::Path getLibraryPath(const VecLibID& libID) const;
    [[nodiscard]] fs::Path getMetadataPath(const VecLibID& libID) const;
    [[nodiscard]] fs::Path getNodeIdsPath(const VecLibID& libID) const;
    [[nodiscard]] fs::Path getShardPath(const VecLibID& libID, LSHSignature sig) const;

private:
    fs::Path _rootPath;
    std::unordered_map<VecLibID, std::unique_ptr<VecLibStorage>> _storages;

    [[nodiscard]] VectorResult<void> writeMetadata(MetadataWriter& writer, const VecLibMetadata& metadata);
};

}

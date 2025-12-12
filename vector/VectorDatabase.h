#pragma once

#include <unordered_map>
#include <memory>
#include <shared_mutex>

#include "Path.h"
#include "VecLibMetadata.h"
#include "VectorResult.h"

namespace vec {

class VecLib;
class StorageManager;
class ShardCache;
class VecLibAccessor;

class VectorDatabase {
public:
    using VecLibMap = std::unordered_map<VecLibID, std::unique_ptr<VecLib>>;
    using VecLibIDs = std::unordered_map<std::string_view, VecLibID>;

    ~VectorDatabase();

    VectorDatabase(const VectorDatabase&) = delete;
    VectorDatabase(VectorDatabase&&) = delete;
    VectorDatabase& operator=(const VectorDatabase&) = delete;
    VectorDatabase& operator=(VectorDatabase&&) = delete;

    [[nodiscard]] static VectorResult<std::unique_ptr<VectorDatabase>> create(const fs::Path& rootPath);

    [[nodiscard]] VectorResult<VecLibID> createLibrary(std::string_view libName,
                                                       Dimension dim,
                                                       DistanceMetric metric = DistanceMetric::INNER_PRODUCT);
    [[nodiscard]] VectorResult<void> deleteLibrary(std::string_view libName);

    void listLibraries(std::vector<VecLibID>& out) const;

    [[nodiscard]] bool libraryExists(const VecLibID& libID) const;
    [[nodiscard]] bool libraryExists(std::string_view libName) const;

    [[nodiscard]] VecLibAccessor getLibrary(const VecLibID& libID);
    [[nodiscard]] VecLibAccessor getLibrary(std::string_view libName);

private:
    mutable std::shared_mutex _mutex;
    std::unique_ptr<StorageManager> _storageManager;
    std::unique_ptr<ShardCache> _shardCache;
    VecLibMap _vecLibs;
    VecLibIDs _vecLibIDs;

    VectorDatabase();

    [[nodiscard]] VectorResult<void> load();
};

}

#pragma once

#include <unordered_map>
#include <memory>
#include <shared_mutex>

#include "Path.h"
#include "VecLibMetadata.h"
#include "VectorResult.h"
#include "VecLibAccessor.h"
#include "VecLibWriteAccessor.h"

namespace vec {

class VecLib;
class VectorStorageManager;
class ShardCache;

class VectorDatabase {
public:
    using VecLibMap = std::unordered_map<VecLibID, std::unique_ptr<VecLib>>;
    using VecLibIDs = std::unordered_map<std::string_view, VecLibID>;

    VectorDatabase();
    ~VectorDatabase();

    VectorDatabase(const VectorDatabase&) = delete;
    VectorDatabase(VectorDatabase&&) = delete;

    [[nodiscard]] VectorResult<void> init(const fs::Path& rootPath);

    VectorResult<VecLibID> createLibrary(std::string_view libName,
                                         Dimension dim,
                                         DistanceMetric metric = DistanceMetric::INNER_PRODUCT,
                                         IndexType indexType = IndexType::FLAT);

    VectorResult<void> deleteLibrary(std::string_view libName);

    void listLibraries(std::vector<VecLibID>& out) const;

    bool libraryExists(const VecLibID& libID) const;
    bool libraryExists(std::string_view libName) const;

    VecLibAccessor getLibrary(const VecLibID& libID);
    VecLibAccessor getLibrary(std::string_view libName);

    VecLibWriteAccessor getLibraryForWrite(const VecLibID& libID);
    VecLibWriteAccessor getLibraryForWrite(std::string_view libName);

    void listLibraryNames(std::vector<std::string>& out) const;

private:
    mutable std::shared_mutex _mutex;
    std::unique_ptr<VectorStorageManager> _storageManager;
    std::unique_ptr<ShardCache> _shardCache;
    VecLibMap _vecLibs;
    VecLibIDs _vecLibIDs;

    [[nodiscard]] VectorResult<void> load();
};

}

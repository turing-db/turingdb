#pragma once

#include <unordered_map>
#include <memory>
#include <shared_mutex>

#include "Path.h"
#include "VecLibMetadata.h"
#include "VectorResult.h"
#include "VecLibIdentifier.h"

namespace vec {

class VecLib;
class StorageManager;

class VectorDatabase {
public:
    using VecLibMap = std::unordered_map<VecLibID, std::unique_ptr<VecLib>>;

    ~VectorDatabase();

    VectorDatabase(const VectorDatabase&) = delete;
    VectorDatabase(VectorDatabase&&) = delete;
    VectorDatabase& operator=(const VectorDatabase&) = delete;
    VectorDatabase& operator=(VectorDatabase&&) = delete;

    [[nodiscard]] static VectorResult<std::unique_ptr<VectorDatabase>> create(const fs::Path& rootPath);

    [[nodiscard]] VectorResult<VecLibID> createLibrary(std::string_view libName,
                                                       Dimension dim,
                                                       DistanceMetric metric = DistanceMetric::INNER_PRODUCT);
    [[nodiscard]] VectorResult<void> deleteLibrary(const VecLibID& libID);
    void listLibraries(std::vector<VecLibID>& out) const;

    [[nodiscard]] bool libraryExists(const VecLibID& libID) const;

    [[nodiscard]] VecLib* getLibrary(const VecLibID& libID) {
        auto it = _vecLibs.find(libID);
        if (it == _vecLibs.end()) {
            return nullptr;
        }

        return it->second.get();
    }

private:
    mutable std::shared_mutex _mutex;
    std::unique_ptr<StorageManager> _storageManager;
    VecLibMap _vecLibs;

    VectorDatabase();
};

}

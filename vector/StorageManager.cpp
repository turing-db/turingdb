#include "StorageManager.h"

#include <faiss/IndexFlat.h>
#include <faiss/index_io.h>

#include "VecLibStorage.h"
#include "VecLib.h"
#include "VecLibShard.h"

#include "TuringTime.h"
#include "VectorException.h"

using namespace vec;

StorageManager::StorageManager() {
}

StorageManager::~StorageManager() {
}

VectorResult<std::unique_ptr<StorageManager>> StorageManager::create(const fs::Path& rootPath) {
    auto storage = std::make_unique<StorageManager>();
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

    return storage;
}

VectorResult<void> StorageManager::createLibraryStorage(const VecLib& lib) {
    const auto& meta = lib.metadata();

    const fs::Path libRoot = getLibraryPath(meta._id);
    const fs::Path metadataPath = getMetadataPath(meta._id);
    const fs::Path nodeIdsPath = getNodeIdsPath(meta._id);

    fmt::println("Lib root: {}", libRoot.c_str());
    fmt::println("Metadata path: {}", metadataPath.c_str());
    fmt::println("Node IDs path: {}", nodeIdsPath.c_str());

    if (_storages.contains(meta._id)) {
        return VectorError::result(VectorErrorCode::LibraryStorageAlreadyExists);
    }

    auto storage = std::make_unique<VecLibStorage>();

    // Create library directory
    if (auto res = libRoot.mkdir(); !res) {
        return VectorError::result(VectorErrorCode::CouldNotCreateLibraryStorage);
    }

    // Create node IDs file
    fmt::print("Creating node IDs file at {}\n", nodeIdsPath.c_str());
    if (auto res = fs::File::createAndOpen(nodeIdsPath)) {
        storage->_nodeIdsFile = std::move(res.value());
    } else {
        return VectorError::result(VectorErrorCode::CouldNotCreateNodeIdsStorage);
    }

    // Create metadata file
    if (auto res = fs::File::createAndOpen(metadataPath)) {
        storage->_metadataFile = std::move(res.value());
        storage->_metadataWriter.setFile(&storage->_metadataFile);
    } else {
        return VectorError::result(VectorErrorCode::CouldNotCreateMetadataStorage);
    }

    // Write metadata file
    if (auto res = writeMetadata(storage->_metadataWriter, meta); !res) {
        return res.get_unexpected();
    }

    _storages.emplace(meta._id, std::move(storage));

    return {};
}

bool StorageManager::libraryExists(const VecLibID& libID) const {
    const fs::Path p = _rootPath / libID;
    return p.exists();
}

// VecLibShard& StorageManager::addShard(VecLib& lib) {
//     auto& meta = lib.metadata();
//     auto& storage = _storages.at(meta._id);
// 
//     if (!storage->_shards.empty()) {
//         auto& prevShard = storage->_shards.back();
//         prevShard._isFull = true;
//         faiss::write_index(prevShard._index.get(), prevShard._path.c_str());
//         prevShard.unload();
//     }
// 
//     const size_t shardIndex = storage->_shards.size();
//     auto& shard = storage->_shards.emplace_back();
//     shard._path = getShardPath(meta._id, shardIndex);
// 
//     const auto now = Clock::now()
//                          .time_since_epoch()
//                          .count();
// 
//     meta._shardCount = storage->_shards.size();
//     meta._modifiedAt = now;
// 
//     if (auto res = writeMetadata(storage->_metadataWriter, meta); !res) {
//         throw VectorException("Could not write metadata file");
//     }
// 
//     switch (meta._metric) {
//         case DistanceMetric::EUCLIDEAN_DIST:
//             shard._index = std::make_unique<faiss::IndexFlatL2>(meta._dimension);
//             break;
//         case DistanceMetric::INNER_PRODUCT:
//             shard._index = std::make_unique<faiss::IndexFlatIP>(meta._dimension);
//             break;
//     }
// 
//     shard._dim = meta._dimension;
// 
//     return storage->_shards.back();
// }

const VecLibStorage& StorageManager::getStorage(const VecLibID& libID) const {
    return *_storages.at(libID);
}

VecLibStorage& StorageManager::getStorage(const VecLibID& libID) {
    return *_storages.at(libID);
}

fs::Path StorageManager::getLibraryPath(const VecLibID& libID) const {
    return _rootPath / libID;
}

fs::Path StorageManager::getMetadataPath(const VecLibID& libID) const {
    return getLibraryPath(libID) / "metadata.json";
}

fs::Path StorageManager::getNodeIdsPath(const VecLibID& libID) const {
    return getLibraryPath(libID) / "node_ids.bin";
}

fs::Path StorageManager::getShardPath(const VecLibID& libID, LSHSignature sig) const {
    return getLibraryPath(libID) / "embeddings-" + std::to_string(sig) + ".index";
}

VectorResult<void> StorageManager::writeMetadata(MetadataWriter& writer, const VecLibMetadata& metadata) {
    if (auto res = writer.file().clearContent(); !res) {
        return VectorError::result(VectorErrorCode::CouldNotClearMetadataFile);
    }

    const fs::Path metadataPath = getMetadataPath(metadata._id);

    writer.write("{\n");

    // Library ID
    writer.write("    \"library_id\": \"");
    writer.write(metadata._id.view());
    writer.write("\",\n");

    // Dimension
    writer.write("    \"dimension\": ");
    writer.write(std::to_string(metadata._dimension));
    writer.write(",\n");

    // Metric
    writer.write("    \"metric\": \"");
    writer.write(metadata._metric == DistanceMetric::EUCLIDEAN_DIST
                     ? "EUCLIDEAN_DIST"
                     : "INNER_PRODUCT");
    writer.write("\",\n");

    // Precision
    writer.write("    \"precision\": 32,\n");

    // Created timestamp
    writer.write("    \"created_at\": ");
    writer.write(std::to_string(metadata._createdAt));
    writer.write(",\n");

    // Modified timestamp
    writer.write("    \"modified_at\": ");
    writer.write(std::to_string(metadata._modifiedAt));
    writer.write(",\n");

    // Shard count
    writer.write("    \"shard_count\": ");
    writer.write(std::to_string(metadata._shardCount));
    writer.write(",\n");

    if (writer.errorOccured()) {
        return VectorError::result(VectorErrorCode::CouldNotWriteMetadataFile);
    }

    return {};
}

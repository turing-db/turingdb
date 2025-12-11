#pragma once

#include "LSHSignature.h"
#include "VecLibMetadata.h"
#include "VectorResult.h"

#include <memory>

namespace faiss {
struct Index;
}

namespace vec {

struct VecLibShard;
struct VecLibStorage;
class StorageManager;
class ShardCache;
class BatchVectorCreate;
class BatchVectorSearch;
class VectorSearchResult;
class LSHShardRouter;

class VecLib {
public:
    ~VecLib();

    VecLib(const VecLib&) = delete;
    VecLib(VecLib&&) = delete;
    VecLib& operator=(const VecLib&) = delete;
    VecLib& operator=(VecLib&&) = delete;

    class Builder {
    public:
        Builder();

        Builder& setStorage(StorageManager* storage) {
            _vecLib->_storage = storage;
            return *this;
        }

        Builder& setShardCache(ShardCache* shardCache) {
            _vecLib->_shardCache = shardCache;
            return *this;
        }

        Builder& setID(VecLibID id) {
            _vecLib->_metadata._id = id;
            return *this;
        }

        Builder& setName(std::string_view name) {
            _vecLib->_metadata._name = name;
            return *this;
        }

        Builder& setDimension(Dimension dimension) {
            _vecLib->_metadata._dimension = dimension;
            return *this;
        }

        Builder& setMetric(DistanceMetric metric) {
            _vecLib->_metadata._metric = metric;
            return *this;
        }

        [[nodiscard]] std::unique_ptr<VecLib> build();

    private:
        std::unique_ptr<VecLib> _vecLib;
    };

    class Loader {
    public:
        Loader();

        Loader& setStorageManager(StorageManager* storage) {
            _vecLib->_storage = storage;
            return *this;
        }

        Loader& setShardCache(ShardCache* shardCache) {
            _vecLib->_shardCache = shardCache;
            return *this;
        }

        [[nodiscard]] VectorResult<std::unique_ptr<VecLib>> load(VecLibStorage& storage);

    private:
        std::unique_ptr<VecLib> _vecLib;
    };

    [[nodiscard]] VectorResult<void> addEmbeddings(const BatchVectorCreate& batch);
    [[nodiscard]] VectorResult<void> search(const BatchVectorSearch& query, VectorSearchResult& results);
    [[nodiscard]] BatchVectorCreate prepareCreateBatch(Dimension dimension);

    [[nodiscard]] VecLibID id() const {
        return _metadata._id;
    }

    [[nodiscard]] std::string_view name() const {
        return _metadata._name;
    }

    [[nodiscard]] const VecLibMetadata& metadata() const {
        return _metadata;
    }

    [[nodiscard]] VecLibMetadata& metadata() {
        return _metadata;
    }

    [[nodiscard]] const LSHShardRouter& shardRouter() const {
        return *_shardRouter;
    }

    [[nodiscard]] const VecLibStorage& getStorage() const;

private:
    friend Builder;

    StorageManager* _storage {nullptr};
    ShardCache* _shardCache {nullptr};

    VecLibMetadata _metadata;
    std::unique_ptr<LSHShardRouter> _shardRouter;

    VecLib();
};

};

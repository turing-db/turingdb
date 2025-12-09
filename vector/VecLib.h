#pragma once

#include "StorageManager.h"
#include "VecLibIdentifier.h"
#include "VecLibMetadata.h"

#include <memory>

namespace faiss {
struct Index;
}

namespace vec {

struct VecLibShard;
class BatchVectorCreate;
class BatchVectorSearch;
class VectorSearchResult;

class VecLib {
public:
    class Builder {
    public:
        Builder();

        Builder& setStorage(StorageManager* storage) {
            _vecLib->_storage = storage;
            return *this;
        }

        Builder& setIdentifier(VecLibIdentifier&& identifier) {
            _vecLib->_metadata._id = std::move(identifier);
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

    [[nodiscard]] VectorResult<void> addEmbeddings(const BatchVectorCreate& batch);
    [[nodiscard]] VectorResult<void> search(const BatchVectorSearch& query, VectorSearchResult& results);

    [[nodiscard]] VecLibID id() const {
        return _metadata._id.view();
    }

    [[nodiscard]] const VecLibMetadata& metadata() const {
        return _metadata;
    }

    [[nodiscard]] VecLibMetadata& metadata() {
        return _metadata;
    }

    [[nodiscard]] size_t getShardCount() const {
        return _metadata._shardCount;
    }

    [[nodiscard]] const VecLibShard& getShard(size_t index) const;

private:
    friend Builder;

    StorageManager* _storage {nullptr};

    VecLibIdentifier _identifier;
    VecLibMetadata _metadata;

    VecLib();
};

};

#pragma once

#include <stddef.h>
#include <span>
#include <vector>

#include "metadata/PropertyType.h"

namespace db {

class EmbeddingBucket {
public:
    static constexpr size_t MIN_BUCKET_BYTES = 4 * 1024 * 1024;
    static constexpr size_t MIN_EMBEDDINGS = 512;

    explicit EmbeddingBucket(size_t dimension);
    ~EmbeddingBucket();

    EmbeddingBucket(const EmbeddingBucket&) = delete;

    EmbeddingBucket(EmbeddingBucket&& other) noexcept
        : _dimension(other._dimension),
        _capacity(other._capacity),
        _embeddingCount(other._embeddingCount),
        _bucket(std::move(other._bucket))
    {
    }

    EmbeddingBucket& operator=(const EmbeddingBucket&) = delete;

    EmbeddingBucket& operator=(EmbeddingBucket&& other) noexcept {
        _embeddingCount = other._embeddingCount;
        _bucket = std::move(other._bucket);
        return *this;
    }

    size_t getDimension() const { return _dimension; }
    size_t getEmbeddingCount() const { return _embeddingCount; }
    size_t getAvailCount() const { return _capacity - _embeddingCount; }

    const float* data() const { return _bucket.data(); }
    float* data() { return _bucket.data(); }

    std::span<const float> span() const { return {_bucket.data(), _embeddingCount * _dimension}; }

    types::Embedding::Primitive alloc(types::Embedding::Primitive content);

private:
    const size_t _dimension {0};
    const size_t _capacity {0};
    size_t _embeddingCount {0};
    std::vector<float> _bucket;
};

}

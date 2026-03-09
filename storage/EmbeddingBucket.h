#pragma once

#include <stdint.h>
#include <string.h>
#include <span>
#include <vector>

#include "BioAssert.h"

namespace db {

class EmbeddingBucket {
public:
    static constexpr uint32_t BUCKET_SIZE = 256ul * 1024;
    static constexpr uint32_t BUCKET_FLOATS = BUCKET_SIZE / sizeof(float);

    explicit EmbeddingBucket(uint32_t dimension)
        : _bucket(BUCKET_FLOATS),
        _dimension(dimension)
    {
        bioassert(dimension > 0, "Embedding dimension must be > 0");
    }

    ~EmbeddingBucket() = default;

    EmbeddingBucket(const EmbeddingBucket&) = delete;
    EmbeddingBucket(EmbeddingBucket&&) noexcept = default;
    EmbeddingBucket& operator=(const EmbeddingBucket&) = delete;
    EmbeddingBucket& operator=(EmbeddingBucket&&) noexcept = default;

    std::span<const float> alloc(std::span<const float> embedding) {
        bioassert(embedding.size() == _dimension, "Embedding dimension mismatch");
        bioassert(availFloats() >= _dimension, "Embedding does not fit in bucket");

        float* dst = _bucket.data() + _floatCount;
        std::memcpy(dst, embedding.data(), _dimension * sizeof(float));
        _floatCount += _dimension;

        return {dst, _dimension};
    }

    uint32_t embeddingCount() const { return _floatCount / _dimension; }
    uint32_t availFloats() const { return BUCKET_FLOATS - _floatCount; }
    const float* data() const { return _bucket.data(); }
    uint32_t floatCount() const { return _floatCount; }
    uint32_t dimension() const { return _dimension; }

private:
    std::vector<float> _bucket;
    uint32_t _dimension;
    uint32_t _floatCount {0};
};

}

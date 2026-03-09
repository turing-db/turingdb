#pragma once

#include <span>
#include <vector>

#include "EmbeddingBucket.h"

#include "BioAssert.h"

namespace db {

class DataPartMerger;

class EmbeddingContainer {
public:
    using ViewVector = std::vector<std::span<const float>>;
    using BucketVector = std::vector<EmbeddingBucket>;

    explicit EmbeddingContainer(uint32_t dimension)
        : _dimension(dimension),
        _buckets()
    {
        _buckets.emplace_back(dimension);
    }

    ~EmbeddingContainer() = default;
    EmbeddingContainer(EmbeddingContainer&& other) noexcept = default;
    EmbeddingContainer& operator=(EmbeddingContainer&& other) noexcept = default;
    EmbeddingContainer(const EmbeddingContainer& other) = delete;
    EmbeddingContainer& operator=(const EmbeddingContainer& other) = delete;

    void alloc(std::span<const float> embedding) {
        bioassert(embedding.size() == _dimension, "Embedding dimension mismatch");

        EmbeddingBucket* bucket = &_buckets.back();
        if (bucket->availFloats() < _dimension) {
            bucket = &_buckets.emplace_back(_dimension);
        }

        _views.push_back(bucket->alloc(embedding));
    }

    void clear() {
        _views.clear();
        _buckets.clear();
    }

    void addBucket(EmbeddingBucket&& bucket) {
        _buckets.push_back(std::move(bucket));
        auto& b = _buckets.back();

        const uint32_t count = b.embeddingCount();
        const size_t prevCount = _views.size();
        const size_t newCount = prevCount + count;

        _views.resize(newCount);

        for (uint32_t i = 0; i < count; i++) {
            _views[i + prevCount] = {
                b.data() + i * _dimension,
                _dimension,
            };
        }
    }

    const std::span<const float>& getView(size_t index) const {
        bioassert(index < _views.size(), "Embedding index invalid");
        return _views[index];
    }

    size_t size() const { return _views.size(); }

    const ViewVector& get() const { return _views; }
    size_t bucketCount() const { return _buckets.size(); }

    const EmbeddingBucket& bucket(size_t i) const {
        return _buckets[i];
    }

    const BucketVector& buckets() const {
        return _buckets;
    }

    uint32_t dimension() const { return _dimension; }

    ViewVector::const_iterator begin() const { return _views.begin(); }
    ViewVector::const_iterator end() const { return _views.end(); }

private:
    friend DataPartMerger;

    uint32_t _dimension;
    BucketVector _buckets;
    ViewVector _views;
};

}

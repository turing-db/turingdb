#include "EmbeddingContainer.h"

#include "BioAssert.h"

using namespace db;

void EmbeddingContainer::alloc(std::span<const float> embedding) {
    bioassert(embedding.size() == _dimension, "Embedding dimension mismatch");

    EmbeddingBucket* bucket = &_buckets.back();
    if (bucket->availFloats() < _dimension) {
        bucket = &_buckets.emplace_back(_dimension);
    }

    _views.push_back(bucket->alloc(embedding));
}

void EmbeddingContainer::clear() {
    _views.clear();
    _buckets.clear();
}

void EmbeddingContainer::addBucket(EmbeddingBucket&& bucket) {
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

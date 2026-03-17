#include "EmbeddingContainer.h"

#include "BioAssert.h"

using namespace db;

EmbeddingContainer::EmbeddingContainer(size_t dimension)
    : _dimension(dimension)
{
    EmbeddingBucket& bucket = _buckets.emplace_back(_dimension);
    _capacity = bucket.getCapacity();
}

EmbeddingContainer::~EmbeddingContainer() {
}

void EmbeddingContainer::alloc(std::span<const float> content) {
    bioassert(content.size() == _dimension, "Embedding dimension mismatch");

    const EmbeddingBucket& last = _buckets.back();
    if (last.getAvailCount() == 0) {
        _buckets.emplace_back(_dimension);
    }

    EmbeddingBucket& bucket = _buckets.back();
    const std::span<float> view = bucket.alloc(content);
    _views.push_back(view);
    _count++;
}

void EmbeddingContainer::clear() {
    _buckets.clear();
    _views.clear();
    _count = 0;
}

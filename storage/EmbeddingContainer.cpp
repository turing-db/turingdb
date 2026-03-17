#include "EmbeddingContainer.h"

#include "BioAssert.h"

using namespace db;

EmbeddingContainer::EmbeddingContainer(size_t dimension)
    : _dimension(dimension)
{
    _buckets.push_back(std::make_unique<EmbeddingBucket>(_dimension));
}

EmbeddingContainer::~EmbeddingContainer() {
}

void EmbeddingContainer::alloc(types::Embedding::Primitive content) {
    bioassert(content.size() == _dimension, "Embedding dimension mismatch");

    EmbeddingBucket& last = *_buckets.back();
    if (last.getAvailCount() == 0) {
        _buckets.push_back(std::make_unique<EmbeddingBucket>(_dimension));
    }

    EmbeddingBucket& bucket = *_buckets.back();
    const types::Embedding::Primitive view = bucket.alloc(content);
    _views.push_back(view);
}

void EmbeddingContainer::clear() {
    _buckets.clear();
    _views.clear();
}

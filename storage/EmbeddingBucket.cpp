#include "EmbeddingBucket.h"

#include <string.h>

#include "BioAssert.h"

using namespace db;

namespace {

size_t computeCapacity(size_t dimension) {
    bioassert(dimension > 0, "Embedding dimension must be > 0");
    const size_t bytesPerEmbedding = dimension * sizeof(float);
    const size_t fromBytes = (EmbeddingBucket::MIN_BUCKET_BYTES + bytesPerEmbedding - 1) / bytesPerEmbedding;
    return fromBytes > EmbeddingBucket::MIN_EMBEDDINGS ? fromBytes : EmbeddingBucket::MIN_EMBEDDINGS;
}

}

EmbeddingBucket::EmbeddingBucket(size_t dimension)
    : _dimension(dimension),
    _capacity(computeCapacity(dimension)),
    _bucket(_capacity * dimension)
{
}

EmbeddingBucket::~EmbeddingBucket() {
}

std::span<float> EmbeddingBucket::alloc(types::Embedding::Primitive content) {
    bioassert(content.size() == _dimension, "Embedding dimension mismatch");
    bioassert(getAvailCount() > 0, "Embedding bucket is full");

    const size_t offset = _embeddingCount * _dimension;
    float* dst = _bucket.data() + offset;
    std::memcpy(dst, content.data(), _dimension * sizeof(float));
    _embeddingCount++;

    return {dst, _dimension};
}

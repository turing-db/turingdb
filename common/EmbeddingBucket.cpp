#include "EmbeddingBucket.h"

#include <string.h>

#include "BioAssert.h"
#include "TuringException.h"

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

std::span<float> EmbeddingBucket::alloc(std::span<const float> content) {
    bioassert(content.size() == _dimension, "Embedding dimension mismatch");
    bioassert(getAvailCount() > 0, "Embedding bucket is full");

    const size_t offset = _embeddingCount * _dimension;
    float* dst = _bucket.data() + offset;
    std::memcpy(dst, content.data(), _dimension * sizeof(float));
    _embeddingCount++;

    return {dst, _dimension};
}

void EmbeddingBucket::fill(std::vector<float>&& floats, size_t count) {
    if (_dimension == 0) {
        throw TuringException("Cannot fill EmbeddingBucket with dimension 0");
    }
    if (floats.size() < count * _dimension) {
        throw TuringException("Float data too small for requested embedding count");
    }

    _bucket = std::move(floats);
    _capacity = _bucket.size() / _dimension;
    _embeddingCount = count;
}

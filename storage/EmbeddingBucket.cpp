#include "EmbeddingBucket.h"

#include <string.h>

#include "BioAssert.h"

using namespace db;

std::span<const float> EmbeddingBucket::alloc(std::span<const float> embedding) {
    bioassert(embedding.size() == _dimension, "Embedding dimension mismatch");
    bioassert(availFloats() >= _dimension, "Embedding does not fit in bucket");

    float* dst = _bucket.data() + _floatCount;
    std::memcpy(dst, embedding.data(), _dimension * sizeof(float));
    _floatCount += _dimension;

    return {dst, _dimension};
}

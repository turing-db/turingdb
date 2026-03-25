#pragma once

#include <stddef.h>

#include <span>
#include <vector>

namespace net::proto {

/**
 * @brief Simple stable-address owning storage for decoded embedding floats.
 *
 * The Embedding type holds a non-owning view (std::span<const float>) over
 * its data. This buffer provides the underlying float storage so that the
 * spans remain valid for the lifetime of the buffer.
 *
 * This is intentionally simpler than the existing embedding bucket
 * infrastructure in the storage layer, which is tied to a fixed embedding
 * dimension. Here we need to store embeddings of arbitrary size as they
 * arrive off the wire, so a plain chunked float allocator is sufficient.
 */
class EmbeddingBuffer {
public:
    EmbeddingBuffer();
    ~EmbeddingBuffer();

    std::span<const float> alloc(const float* data, size_t count);
    float* alloc(size_t count);
    std::span<const float> getSpan(float* data, size_t count);
    void clear();

private:
    static constexpr size_t CHUNK_CAPACITY = 256 * 1024;

    std::vector<float>& ensureCapacity(size_t count);

    std::vector<std::vector<float>> _chunks;
};

}

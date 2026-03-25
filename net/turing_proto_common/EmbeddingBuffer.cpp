#include "EmbeddingBuffer.h"

using namespace net::proto;

EmbeddingBuffer::EmbeddingBuffer() {
}

EmbeddingBuffer::~EmbeddingBuffer() {
}

std::vector<float>& EmbeddingBuffer::ensureCapacity(size_t count) {
    const auto chunksHaveBeenAllocated = !_chunks.empty();
    const auto hasCapacity = _chunks.back().size() + count <= _chunks.back().capacity();

    if (!chunksHaveBeenAllocated || !hasCapacity) {
        _chunks.emplace_back();
        _chunks.back().reserve(std::max(CHUNK_CAPACITY, count));
    }
    return _chunks.back();
}

std::span<const float> EmbeddingBuffer::alloc(const float* data, size_t count) {
    auto& chunk = ensureCapacity(count);
    const size_t offset = chunk.size();
    chunk.insert(chunk.end(), data, data + count);
    return {chunk.data() + offset, count};
}

float* EmbeddingBuffer::alloc(size_t count) {
    auto& chunk = ensureCapacity(count);
    const size_t offset = chunk.size();
    chunk.resize(offset + count, 0.0f);
    return chunk.data() + offset;
}

std::span<const float> EmbeddingBuffer::getSpan(float* data, size_t count) {
    return {data, count};
}

void EmbeddingBuffer::clear() {
    _chunks.clear();
}

#include "ChunkedBuffer.h"

#include <algorithm>

using namespace net::proto;

template<typename T>
ChunkedBuffer<T>::ChunkedBuffer() {
}

template<typename T>
ChunkedBuffer<T>::~ChunkedBuffer() {
}

template<typename T>
std::vector<T>& ChunkedBuffer<T>::ensureCapacity(size_t count) {
    const bool chunksHaveBeenAllocated = !_chunks.empty();
    const bool hasCapacity = chunksHaveBeenAllocated
                          && _chunks.back().size() + count <= _chunks.back().capacity();

    if (!chunksHaveBeenAllocated || !hasCapacity) {
        _chunks.emplace_back();
        _chunks.back().reserve(std::max(CHUNK_CAPACITY, count));
    }
    return _chunks.back();
}

template<typename T>
ChunkedBuffer<T>::refType ChunkedBuffer<T>::alloc(const T* data, size_t count) {
    auto& chunk = ensureCapacity(count);
    const size_t offset = chunk.size();
    chunk.insert(chunk.end(), data, data + count);
    return {chunk.data() + offset, count};
}

template<typename T>
T* ChunkedBuffer<T>::alloc(size_t count) {
    auto& chunk = ensureCapacity(count);
    const size_t offset = chunk.size();
    chunk.resize(offset + count, T {});
    return chunk.data() + offset;
}

template<typename T>
ChunkedBuffer<T>::refType ChunkedBuffer<T>::getView(T* data, size_t count) {
    return {data, count};
}

template<typename T>
void ChunkedBuffer<T>::clear() {
    _chunks.clear();
}

namespace net::proto {
template class ChunkedBuffer<float>;
template class ChunkedBuffer<char>;
}

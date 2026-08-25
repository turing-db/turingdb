#include "BufferChunk.h"

using namespace db;

template <typename T, size_t N>
BufferChunk<T, N>::BufferChunk()
    : _buf(static_cast<T*>(std::aligned_alloc(alignof(T), sizeof(T) * N))),
    _capacity(N)
{
}

template <typename T, size_t N>
BufferChunk<T, N>::BufferChunk(size_t size)
    : _buf(static_cast<T*>(std::aligned_alloc(alignof(T), sizeof(T) * size))),
    _capacity(size)
{
}

template <typename T, size_t N>
BufferChunk<T, N>::~BufferChunk() {
    std::free(_buf);
}

namespace db {
template class BufferChunk<char>;
}

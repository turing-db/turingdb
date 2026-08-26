#include "BufferChunk.h"

#include <cstdlib>

using namespace db;

template <typename T, size_t N>
BufferChunk<T, N>::BufferChunk()
    : _buf(static_cast<T*>(std::malloc(sizeof(T) * N))),
    _capacity(N)
{
}

template <typename T, size_t N>
BufferChunk<T, N>::BufferChunk(size_t size)
    : _buf(static_cast<T*>(std::malloc(sizeof(T) * size))),
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

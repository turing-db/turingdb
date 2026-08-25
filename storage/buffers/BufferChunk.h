#pragma once

#include <cstddef>
#include <stdlib.h>

namespace db {

template <typename T, size_t N = 4096>
class BufferChunk {
public:
    BufferChunk();
    explicit BufferChunk(size_t size);
    ~BufferChunk();

    BufferChunk(const BufferChunk&) = delete;
    BufferChunk(BufferChunk&&) = delete;
    BufferChunk& operator=(const BufferChunk&) = delete;
    BufferChunk& operator=(BufferChunk&&) = delete;

    [[nodiscard]] bool canFit(size_t numTs) const { return _capacity - _size >= numTs; }

private:
    template <typename U, size_t M>
    friend class RawBuffer;

    T* _buf {nullptr};
    size_t _size {0};
    size_t _capacity {0};

    BufferChunk<T, N>* _next {nullptr};
};

}

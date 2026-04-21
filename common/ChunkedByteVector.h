#include "ChunkedVector.h"

#include "FatalException.h"

template <size_t N>
class ChunkedVector<std::byte, N> {
public:

    class Chunk {
    public:
        [[nodiscard]] bool canFit(size_t size) { return N - _size >= size; };

    private:
        std::array<std::byte, N> _buf;
        size_t _size {0};
        Chunk* _next {nullptr};
    };

    ChunkedVector<std::byte>();
    ~ChunkedVector();

    void reserve(size_t numBytes);
private:
    Chunk* _first {nullptr};
    Chunk* _last {nullptr};

    static_assert(N >= sizeof(std::byte));
    static_assert(N < std::numeric_limits<int64_t>::max());
};

template <size_t N>
void ChunkedVector<std::byte, N>::reserve(size_t numBytes) {
    const bool exceedsChunkSize = numBytes > N;
    if (exceedsChunkSize) {
        throw FatalException("Attempted to reserve");
    }
}

template<size_t N = 4096>
using ChunkedByteVector = ChunkedVector<std::byte, N>;

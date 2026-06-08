#pragma once

#include <stddef.h>
#include <span>
#include <string_view>
#include <vector>

namespace net::proto {

/// The non-owning view type handed out for a ChunkedBuffer of @tparam T: a
/// std::span<const T> in general, specialised to std::string_view for char so that a
/// char buffer reads as text rather than a byte span.
template <typename T>
struct ChunkedViewOf {
    using type = std::span<const T>;
};

template <>
struct ChunkedViewOf<char> {
    using type = std::string_view;
};

/**
 * @brief Simple stable-address owning storage for decoded values of type @tparam T.
 *
 * Hands out a non-owning view (@ref ChunkedViewOf) over data it owns. Storage is held in
 * a list of chunks, each reserved up front; a new chunk is allocated before any insert
 * that would exceed the current chunk's capacity, so existing data never relocates and
 * the views stay valid for the lifetime of the buffer.
 */
template <typename T>
class ChunkedBuffer {
public:
    using refType = typename ChunkedViewOf<T>::type;
    ChunkedBuffer();
    ~ChunkedBuffer();

    refType alloc(const T* data, size_t count);
    T* alloc(size_t count);
    refType getView(T* data, size_t count);
    void clear();

private:
    static constexpr size_t CHUNK_CAPACITY = 256 * 1024;

    std::vector<T>& ensureCapacity(size_t count);

    std::vector<std::vector<T>> _chunks;
};

}

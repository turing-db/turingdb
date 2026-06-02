#pragma once

#include <stddef.h>
#include <stdint.h>
#include <string.h>
#include <string_view>
#include <type_traits>

#include "MapBufferTypeTag.h"

namespace db {

/**
 * @brief Non-owning view of an entry (key-value pair) in a @ref MapByteBuffer.
 *
 * @detail Layout in the byte buffer: [key string_view (16 bytes)][value tag (1 byte)]
 * [value bytes (sizeof(T) bytes)].
 */
class MapEntryView {
public:
    MapEntryView() = default;

    explicit MapEntryView(const std::byte* data)
        : _start(data)
    {
    }

    /// Returns the key of this entry. Keys are always strings.
    std::string_view getKey() const {
        static_assert(sizeof(std::string_view) == 16,
                      "string_view size changed: function may need modifying.");

        std::string_view out;
        std::memcpy(&out, _start, sizeof(std::string_view));
        return out;
    }

    /// Returns the type tag of the value of this entry.
    MapBufferTypeTag getValueTag() const {
        static_assert(sizeof(MapBufferTypeTag) == 1,
                      "MapBufferTypeTag changed size: function may need modifying.");

        const std::byte* tagPtr = _start + sizeof(std::string_view);
        return MapBufferTypeTag {std::to_integer<uint8_t>(*tagPtr)};
    }

    /**
     * @brief Attempts to read the value of this entry as a @param T.
     * @warn Does not verify that the value is in fact a @param T.
     * @warn No bounds checking is performed on the read.
     */
    template <typename T>
    T getValueAs() const;

private:
    /// Pointer to the start of this entry (key bytes) in a MapByteBuffer
    const std::byte* _start {nullptr};
};

template <typename T>
T MapEntryView::getValueAs() const {
    static_assert(std::is_trivially_copyable_v<T>);

    constexpr size_t keySize = sizeof(std::string_view);
    constexpr size_t tagSize = sizeof(MapBufferTypeTag);
    static_assert(tagSize == 1, "Tag size changed: function may need modifying.");

    // Key is at _start, tag follows the key, value follows the tag
    const std::byte* dataPtr = _start + keySize + tagSize;

    T out {};

    {
        T* outAddr = &out;
        constexpr size_t sizeOfT = sizeof(T);
        std::memcpy(outAddr, dataPtr, sizeOfT);
    }

    return out;
}

}

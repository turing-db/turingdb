#pragma once

#include <cstddef>
#include <cstdint>
#include <cstring>
#include <string_view>
#include <type_traits>

#include "CellView.h"

namespace db {

/**
 * @brief Non-owning view of an entry (key-value pair) in a @ref MapByteBuffer.
 *
 * @detail Layout in the byte buffer: [key string_view (sizeof(std::string_view) bytes)]
 * [value tag (1 byte)][value bytes (sizeof(T) bytes)].
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
        static_assert(std::is_trivially_copyable_v<std::string_view>);

        std::string_view out;
        std::memcpy(&out, _start, sizeof(std::string_view));
        return out;
    }

    /// Returns the value of this entry: its tag and its bytes, laid out as a list element's.
    CellView getValue() const { return CellView {_start + sizeof(std::string_view)}; }

private:
    /// Pointer to the start of this entry (key bytes) in a MapByteBuffer
    const std::byte* _start {nullptr};
};

}

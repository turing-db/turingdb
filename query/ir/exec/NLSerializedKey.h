#pragma once

#include <stddef.h>

#include <algorithm>
#include <memory>
#include <string_view>

namespace db {

// The bytes a row's key serializes into, cleared and written again for each row. An append is
// always inlined into the serializer making it and only growing the buffer is a call, where
// std::string's append is a call GCC may or may not inline from one build to the next.
class NLSerializedKey {
public:
    NLSerializedKey();
    ~NLSerializedKey();

    [[gnu::always_inline]] void append(const char* bytes, size_t size) {
        if (size > _capacity - _size) [[unlikely]] {
            grow(size);
        }

        std::copy_n(bytes, size, _bytes.get() + _size);
        _size += size;
    }

    [[gnu::always_inline]] void append(std::string_view bytes) { append(bytes.data(), bytes.size()); }
    [[gnu::always_inline]] void push_back(char byte) { append(&byte, 1); }

    void clear() { _size = 0; }

    std::string_view getBytes() const { return {_bytes.get(), _size}; }

    bool operator==(const NLSerializedKey& other) const { return getBytes() == other.getBytes(); }

private:
    std::unique_ptr<char[]> _bytes;
    size_t _size {0};
    size_t _capacity {0};

    void grow(size_t size);
};

}

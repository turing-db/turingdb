#pragma once

#include <stddef.h>

#include <span>
#include <string_view>

#include "SpanBuffer.h"

namespace db {

class StringBuffer final : public SpanBuffer<char, std::string_view> {
public:
    std::string_view concatenate(std::string_view a, std::string_view b);

    std::string_view insert(std::string_view str);

    std::span<char> allocate(size_t size);
};

}

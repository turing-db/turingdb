#pragma once

#include <span>
#include <string_view>

#include "SpanBuffer.h"

namespace db {

class StringBuffer final : public SpanBuffer<char, std::string_view> {
public:
    std::string_view concatenate(std::string_view a, std::string_view b);

    std::string_view join(std::span<const std::string_view> strs, std::string_view sep);
};

}

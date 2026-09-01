#pragma once

#include <string_view>

#include "SpanBuffer.h"

namespace db {

class StringBuffer final : public SpanBuffer<char, std::string_view> {
public:
    std::string_view insert(std::string_view sv);
    std::string_view concatenate(std::string_view a, std::string_view b);
};

}

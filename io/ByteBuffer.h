#pragma once

#include <cstdint>
#include <span>
#include <vector>

namespace fs {

using ByteBuffer = std::vector<uint8_t>;
using ByteSpan = std::span<const uint8_t>;

}

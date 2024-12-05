#pragma once

#include <cstdint>
#include <vector>

namespace fs {

using Byte = uint8_t;
using ByteBuffer = std::vector<uint8_t>;

template <typename T>
concept TrivialPrimitive = std::integral<T>
                        || std::floating_point<T>
                        || std::same_as<T, bool>;

}

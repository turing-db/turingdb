#pragma once

#include <concepts>

namespace fs {

template <typename T>
concept CharPrimitive = std::same_as<T, char>
                     || std::same_as<T, signed char>
                     || std::same_as<T, unsigned char>
                     || std::same_as<T, wchar_t>
                     || std::same_as<T, char8_t>
                     || std::same_as<T, char16_t>
                     || std::same_as<T, char32_t>;

template <typename T>
concept TrivialPrimitive = std::is_trivial_v<T>
                        && std::is_standard_layout_v<T>;

template <typename T>
concept TrivialNonCharPrimitive = TrivialPrimitive<T> && !CharPrimitive<T>;

}

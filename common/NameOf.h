#pragma once

#include <string_view>

namespace db {

template <typename T> struct NameOf {
  static std::string_view get() {
#if defined(__clang__)
    const std::string_view sv = __PRETTY_FUNCTION__;
    const auto start = sv.find('[') + 5;
    const auto end = sv.rfind(']');

#elif defined(__GNUC__)
    const std::string_view sv = __PRETTY_FUNCTION__;
    const auto start = sv.find("T = ") + 4;
    const auto end = sv.rfind(']');

#endif
    return sv.substr(start, end - start);
  }
};

} // namespace db

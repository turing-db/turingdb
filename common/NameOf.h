#pragma once

#include <string_view>

namespace db {

template <typename T> struct NameOf {
  static std::string_view get() {
#if defined(__clang__)
    std::string_view sv = __PRETTY_FUNCTION__;
    auto start = sv.find('[') + 5;
    auto end = sv.rfind(']');

#elif defined(__GNUC__)
    std::string_view sv = __PRETTY_FUNCTION__;
    auto start = sv.find("T = ") + 4;
    auto end = sv.rfind(']');

#endif
    return sv.substr(start, end - start);
  }
};

} // namespace db

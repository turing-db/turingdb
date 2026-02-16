#pragma once

#include <string_view>

namespace db {

template <typename T> struct NameOf {
  static std::string_view get() {
#if defined(__clang__)
    const std::string_view sv = __PRETTY_FUNCTION__;
    const size_t start = sv.find('[') + 5;
    const size_t end = sv.rfind(']');

#elif defined(__GNUC__)
    const std::string_view sv = __PRETTY_FUNCTION__;
    const size_t start = sv.find("T = ") + 4;
    const size_t end = sv.rfind(']');

#endif
    return sv.substr(start, end - start);
  }
};

} // namespace db

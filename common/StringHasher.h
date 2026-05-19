#pragma once

#include <string>
#include <string_view>
#include <functional>
#include <type_traits>

struct StringHasher {
    using is_transparent = void;

    template <class T>
    std::size_t operator()(const T& s) const {
        static_assert(std::is_same_v<T, std::string> || std::is_same_v<T, std::string_view>,
                      "StringHasher only supports std::string and std::string_view");
        return std::hash<T>{}(s);
    }
};

#pragma once

#include <type_traits>
#include <stddef.h>

template <typename Type, typename... ArgsT>
struct PackIndexOfType {
};

template <typename Type, typename HeadT, typename... ArgsT>
struct PackIndexOfType<Type, HeadT, ArgsT...> {
    inline static consteval size_t get() {
        if constexpr (std::is_same_v<Type, HeadT>) {
            return 0;
        } else {
            return 1+PackIndexOfType<Type, ArgsT...>::get();
        }
    }
};
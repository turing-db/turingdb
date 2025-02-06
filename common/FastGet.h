#pragma once

#include <variant>

#include "PackTypeIndex.h"

template <typename Type, typename... ArgsT>
inline constexpr auto& FastGet(std::variant<ArgsT...>& variant) {
    constexpr size_t index = PackIndexOfType<Type, ArgsT...>::get();
    return std::__detail::__variant::__get<index>(variant);
}

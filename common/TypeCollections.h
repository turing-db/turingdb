#pragma once

#include <concepts>

template <typename T, typename... U>
concept any_type_of = (std::is_same_v<T, U> || ...);

// First index of type in tuple
template <class T, class Tuple>
struct TupleTypeIndex;

template <class T, class... Types>
struct TupleTypeIndex<T, std::tuple<T, Types...>> {
    static constexpr std::size_t value = 0;
};

template <class T, class U, class... Types>
struct TupleTypeIndex<T, std::tuple<U, Types...>> {
    static constexpr std::size_t value = 1 + TupleTypeIndex<T, std::tuple<Types...>>::value;
};

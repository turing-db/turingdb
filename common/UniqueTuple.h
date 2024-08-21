#pragma once

#include <tuple>

template <typename T, typename... Ts>
struct UniqueTuple : std::type_identity<T> {};

template <typename... Ts, typename U, typename... Us>
struct UniqueTuple<std::tuple<Ts...>, U, Us...>
    : std::conditional_t<(std::is_same_v<U, Ts> || ...)
    , UniqueTuple<std::tuple<Ts...>, Us...>
    , UniqueTuple<std::tuple<Ts..., U>, Us...>>
{
};

template <typename Tuple>
struct MakeUniqueTuple {};

template <typename... Types>
struct MakeUniqueTuple<std::tuple<Types...>> {
    using Type = typename UniqueTuple<std::tuple<>, Types...>::type;
};

#pragma once

#include <tuple>
#include <optional>

#include "columns/ContainerKind.h"

namespace db {

template <typename... Tuples>
struct TupleFlatten {
    using Type = decltype(std::tuple_cat(std::declval<Tuples>()...));
};

template <typename Tuple>
struct KindPairListFromTuple;

template <typename... Pairs>
struct KindPairList {
    static constexpr std::size_t size = sizeof...(Pairs);
};

template <typename T>
struct IsKindPairList : std::false_type {};

template <typename... Pairs>
struct IsKindPairList<KindPairList<Pairs...>> : std::true_type {};

template <typename T>
concept KindPairListExact = IsKindPairList<T>::value;

template <typename L, typename R>
struct KindPair {
    using Lhs = L;
    using Rhs = R;
};

template <typename... Pairs>
struct KindPairListFromTuple<std::tuple<Pairs...>> {
    using Type = KindPairList<Pairs...>;
};

template <typename L, typename R>
struct OptionalKindPairs {
    using Pairs = std::tuple<
        KindPair<L, R>,
        KindPair<std::optional<L>, R>,
        KindPair<L, std::optional<R>>,
        KindPair<std::optional<L>, std::optional<R>>>;
};

template <typename T>
struct OptionalKinds {
    using Types = std::tuple<
        T,
        std::optional<T>>;
};

template <typename... Tuples>
using GenerateKindList =
    TupleFlatten<typename TupleFlatten<Tuples...>::Type>::Type;

template <typename... Tuples>
using GenerateKindPairList =
    KindPairListFromTuple<typename TupleFlatten<Tuples...>::Type>::Type;

template <typename NonTemplated, typename K>
struct MixedKind {
    using NonTemplatedType = NonTemplated;
    using Kind = K;
};

template <typename... Mixed>
struct AllowedMixedList {
    static constexpr std::size_t size = sizeof...(Mixed);
};

template <ContainerKind::Code... Excluded>
struct ExcludedContainers {
    // Contains
    template <typename T>
    inline static consteval bool contains() {
        return ((ContainerKind::code<T>() == Excluded) || ...);
    }

    template <template <typename> class T>
    inline static consteval bool contains() {
        return ((ContainerKind::code<T>() == Excluded) || ...);
    }
};

template <typename Tuple>
struct IsTuple : std::false_type {};

template <typename... Ts>
struct IsTuple<std::tuple<Ts...>> : std::true_type {};

template <typename T>
concept TupleExact = IsTuple<T>::value;

}

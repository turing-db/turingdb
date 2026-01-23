#pragma once

#include <tuple>
#include <optional>

#include "columns/ColumnOperator.h"
#include "metadata/PropertyNull.h"
#include "metadata/PropertyType.h"
#include "columns/ContainerKind.h"
#include "range/v3/utility/optional.hpp"

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

template <ColumnOperator Op>
struct PairRestrictions;

template <ColumnOperator Op>
    requires (Op == OP_EQUAL) || (Op == OP_NOT_EQUAL)
struct PairRestrictions<Op> {
    using Allowed = GenerateKindPairList<
        OptionalKindPairs<types::Int64::Primitive, types::Int64::Primitive>::Pairs,
        OptionalKindPairs<types::Int64::Primitive, types::UInt64::Primitive>::Pairs,
        OptionalKindPairs<types::UInt64::Primitive, types::UInt64::Primitive>::Pairs,
        OptionalKindPairs<types::Bool::Primitive, types::Bool::Primitive>::Pairs,
        OptionalKindPairs<types::String::Primitive, types::String::Primitive>::Pairs,
        OptionalKindPairs<NodeID, NodeID>::Pairs,
        OptionalKindPairs<EdgeID, EdgeID>::Pairs/*,

        std::tuple<KindPair<PropertyNull, std::optional<NodeID>>,
                   KindPair<PropertyNull, std::optional<EdgeID>>,
                   KindPair<PropertyNull, std::optional<types::Int64::Primitive>>,
                   KindPair<PropertyNull, std::optional<types::UInt64::Primitive>>,
                   KindPair<PropertyNull, std::optional<types::Double::Primitive>>,
                   KindPair<PropertyNull, std::optional<types::String::Primitive>>,
                   KindPair<PropertyNull, std::optional<types::Double::Primitive>>,
                   KindPair<PropertyNull, std::optional<ValueType>>>*/>;
    using AllowedMixed =
        AllowedMixedList<MixedKind<ColumnMask, PropertyNull>,
                         MixedKind<ColumnMask, CustomBool>,
                         MixedKind<ColumnMask, std::optional<CustomBool>>>;

    using Excluded = ExcludedContainers<ContainerKind::code<ColumnSet>(),
                                        ContainerKind::code<ColumnMask>()>;
};

template <ColumnOperator Op>
    requires (Op == OP_GREATER_THAN) || (Op == OP_LESS_THAN)
struct PairRestrictions<Op> {
    using Allowed = GenerateKindPairList<OptionalKindPairs<int64_t, int64_t>::Pairs,
                                         OptionalKindPairs<int64_t, uint64_t>::Pairs,
                                         OptionalKindPairs<int64_t, double>::Pairs,
                                         OptionalKindPairs<uint64_t, uint64_t>::Pairs,
                                         OptionalKindPairs<uint64_t, double>::Pairs,
                                         OptionalKindPairs<double, double>::Pairs>;

    using AllowedMixed = AllowedMixedList<>;
    using Excluded = ExcludedContainers<>;
};

template <ColumnOperator Op>
    requires (Op == OP_GREATER_THAN_OR_EQUAL) || (Op == OP_LESS_THAN_OR_EQUAL)
struct PairRestrictions<Op> {
  using Allowed = GenerateKindPairList<
        OptionalKindPairs<int64_t, int64_t>::Pairs,
        OptionalKindPairs<int64_t, uint64_t>::Pairs,
        OptionalKindPairs<uint64_t, uint64_t>::Pairs>;

  using AllowedMixed = AllowedMixedList<>;
  using Excluded = ExcludedContainers<>;
};

template <ColumnOperator Op>
    requires (Op == OP_AND) || (Op == OP_OR)
struct PairRestrictions<Op> {
    using Allowed = GenerateKindPairList<
        OptionalKindPairs<CustomBool, CustomBool>::Pairs,
        std::tuple<KindPair<PropertyNull, CustomBool>,
                   KindPair<PropertyNull, std::optional<CustomBool>>>>;

    using AllowedMixed = AllowedMixedList<
        MixedKind<ColumnMask, PropertyNull>,
        MixedKind<ColumnMask, CustomBool>,
        MixedKind<ColumnMask, std::optional<CustomBool>>>;
    using Excluded = ExcludedContainers<>;
};

template <ColumnOperator Op>
    requires (Op == OP_PLUS) || (Op == OP_MINUS)
struct PairRestrictions<Op> {
    using Allowed = GenerateKindPairList<
        OptionalKindPairs<int64_t, int64_t>::Pairs,
        OptionalKindPairs<int64_t, uint64_t>::Pairs,
        OptionalKindPairs<int64_t, double>::Pairs,
        OptionalKindPairs<uint64_t, uint64_t>::Pairs,
        OptionalKindPairs<uint64_t, double>::Pairs,
        OptionalKindPairs<double, double>::Pairs>;

    using AllowedMixed = AllowedMixedList<>;
    using Excluded = ExcludedContainers<>;
};

}

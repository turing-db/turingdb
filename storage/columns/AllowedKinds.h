#pragma once

#include <tuple>
#include <optional>

#include "ID.h"
#include "versioning/ChangeID.h"
#include "ColumnOperator.h"
#include "metadata/PropertyNull.h"
#include "metadata/PropertyType.h"
#include "ContainerKind.h"

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

// Restriction for Binary operators
template <ColumnOperator Op>
struct PairRestrictions;

template <ColumnOperator Op>
    requires (Op == OP_EQUAL) || (Op == OP_NOT_EQUAL)
struct PairRestrictions<Op> {
    using Allowed = GenerateKindPairList<
        // Standard equality of property types - except doubles
        OptionalKindPairs<types::Int64::Primitive, types::Int64::Primitive>::Pairs,
        OptionalKindPairs<types::Int64::Primitive, types::UInt64::Primitive>::Pairs,
        OptionalKindPairs<types::UInt64::Primitive, types::UInt64::Primitive>::Pairs,
        OptionalKindPairs<types::Bool::Primitive, types::Bool::Primitive>::Pairs,
        OptionalKindPairs<types::String::Primitive, types::String::Primitive>::Pairs,

        std::tuple<
            // Filtering by ID or labels/edge type
            KindPair<NodeID, NodeID>,
            KindPair<EdgeID, EdgeID>,
            KindPair<LabelSetID, LabelSetID>,
            KindPair<EdgeTypeID, EdgeTypeID>,

            // IS (NOT) NULL
            KindPair<std::optional<types::Int64::Primitive>, PropertyNull>,
            KindPair<std::optional<types::UInt64::Primitive>, PropertyNull>,
            KindPair<std::optional<types::Double::Primitive>, PropertyNull>,
            KindPair<std::optional<types::String::Primitive>, PropertyNull>,
            KindPair<std::optional<types::Bool::Primitive>, PropertyNull>
        >
    >;

    using AllowedMixed = AllowedMixedList<
        MixedKind<ColumnMask, CustomBool>,
        MixedKind<ColumnMask, std::optional<CustomBool>>
    >;

    using Excluded = ExcludedContainers<
        ContainerKind::code<ColumnSet>(),
        ContainerKind::code<ColumnMask>()
    >;
};

template <ColumnOperator Op>
    requires (Op == OP_GREATER_THAN) || (Op == OP_LESS_THAN)
struct PairRestrictions<Op> {
    using Allowed = GenerateKindPairList<
        // Standard ordering of numeric types
        OptionalKindPairs<types::Int64::Primitive, types::Int64::Primitive>::Pairs,
        OptionalKindPairs<types::Int64::Primitive, types::UInt64::Primitive>::Pairs,
        OptionalKindPairs<types::Int64::Primitive, types::Double::Primitive>::Pairs,
        OptionalKindPairs<types::UInt64::Primitive, types::UInt64::Primitive>::Pairs,
        OptionalKindPairs<types::UInt64::Primitive, types::Double::Primitive>::Pairs,
        OptionalKindPairs<types::Double::Primitive, types::Double::Primitive>::Pairs
    >;

    using AllowedMixed = AllowedMixedList<>;

    using Excluded = ExcludedContainers<
        ContainerKind::code<ColumnSet>(),
        ContainerKind::code<ColumnMask>()
    >;
};

template <ColumnOperator Op>
    requires (Op == OP_GREATER_THAN_OR_EQUAL) || (Op == OP_LESS_THAN_OR_EQUAL)
struct PairRestrictions<Op> {
    using Allowed = GenerateKindPairList<
        // Standard ordering of numeric types - excluding doubles
        OptionalKindPairs<types::Int64::Primitive, types::Int64::Primitive>::Pairs,
        OptionalKindPairs<types::Int64::Primitive, types::UInt64::Primitive>::Pairs,
        OptionalKindPairs<types::UInt64::Primitive, types::UInt64::Primitive>::Pairs
    >;

    using AllowedMixed = AllowedMixedList<>;

    using Excluded = ExcludedContainers<
        ContainerKind::code<ColumnSet>(),
        ContainerKind::code<ColumnMask>()
    >;
};

template <ColumnOperator Op>
    requires (Op == OP_AND) || (Op == OP_OR)
struct PairRestrictions<Op> {
    using Allowed = GenerateKindPairList<
        // Boolean properties; optional (3-valued logic) and non-optional
        OptionalKindPairs<types::Bool::Primitive, types::Bool::Primitive>::Pairs
    >;

    using AllowedMixed = AllowedMixedList<>;

    // Mask operations also included
    using Excluded = ExcludedContainers<
        ContainerKind::code<ColumnSet>()
    >;
};

template <ColumnOperator Op>
    requires (Op == OP_ADD) || (Op == OP_SUB) || (Op == OP_MUL) || (Op == OP_DIV)
struct PairRestrictions<Op> {
    using Allowed = GenerateKindPairList<
        // Homogeneous arithmetic types
        OptionalKindPairs<types::Int64::Primitive, types::Int64::Primitive>::Pairs,
        OptionalKindPairs<types::UInt64::Primitive, types::UInt64::Primitive>::Pairs,
        OptionalKindPairs<types::Double::Primitive, types::Double::Primitive>::Pairs,

        // Mixed arithmetic types
        OptionalKindPairs<types::Int64::Primitive, types::UInt64::Primitive>::Pairs,
        OptionalKindPairs<types::Int64::Primitive, types::Double::Primitive>::Pairs,
        OptionalKindPairs<types::UInt64::Primitive, types::Double::Primitive>::Pairs
    >;

    using AllowedMixed = AllowedMixedList<>;

    using Excluded = ExcludedContainers<
        ContainerKind::code<ColumnSet>(),
        ContainerKind::code<ColumnMask>()
    >;
};

// Restriction for Unary operators
template <ColumnOperator Op>
struct TypeRestrictions;

template <>
struct TypeRestrictions<OP_NOT> {
    using Allowed = GenerateKindList<
        std::tuple<std::optional<CustomBool>>
    >;

    using Excluded = ExcludedContainers<
        ContainerKind::code<ColumnSet>(),
        ContainerKind::code<ColumnConst>()
    >;
};

/// Types that are outputted by queries, used in @ref QueryTestRunner
struct OutputtedTypes {
    using Allowed = GenerateKindList<std::tuple<
        types::Int64::Primitive,
        types::Int64::Primitive,
        types::UInt64::Primitive,
        types::Double::Primitive,
        types::String::Primitive,
        types::Bool::Primitive,
        std::optional<types::Int64::Primitive>,
        std::optional<types::Int64::Primitive>,
        std::optional<types::UInt64::Primitive>,
        std::optional<types::Double::Primitive>,
        std::optional<types::String::Primitive>,
        std::optional<types::Bool::Primitive>,

        NodeID,
        EdgeID,
        LabelID,
        LabelSetID,
        EdgeTypeID,
        ValueType,
        PropertyTypeID,
        ChangeID,
        size_t
    >>;

    using Excluded = ExcludedContainers<
        ContainerKind::code<ColumnSet>(),
        ContainerKind::code<ColumnMask>()
    >;
};

}

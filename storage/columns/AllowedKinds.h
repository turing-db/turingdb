#pragma once

#include <tuple>
#include <optional>
#include <type_traits>

#include "columns/ColumnConst.h"
#include "list/ListBuffer.h"
#include "ID.h"
#include "versioning/ChangeID.h"
#include "ColumnOperator.h"
#include "metadata/PropertyNull.h"
#include "metadata/PropertyType.h"
#include "GraphPath.h"
#include "ContainerKind.h"

namespace db {

class EntityList;

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

// A tagged scalar against a value of a known type, either way round, so a comparison
// finds the pair whichever side the type-erased column is on.
template <typename T>
struct ListElementKindPairs {
    using Pairs = std::tuple<
        KindPair<ListElementView, T>,
        KindPair<T, ListElementView>,
        KindPair<ListElementView, std::optional<T>>,
        KindPair<std::optional<T>, ListElementView>>;
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

template <ColumnOperator Function>
concept NumericConversionFunction = (Function == OP_TO_INTEGER)
                                 || (Function == OP_TO_FLOAT);

template <ColumnOperator Function>
concept BooleanConversionFunction = (Function == OP_TO_BOOLEAN);

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
        OptionalKindPairs<types::Embedding::Primitive, types::Embedding::Primitive>::Pairs,

        // Equality against a type-erased cell, which holds its own type
        ListElementKindPairs<types::Int64::Primitive>::Pairs,
        ListElementKindPairs<types::UInt64::Primitive>::Pairs,
        ListElementKindPairs<types::Double::Primitive>::Pairs,
        ListElementKindPairs<types::String::Primitive>::Pairs,
        ListElementKindPairs<types::Bool::Primitive>::Pairs,

        // A loaded CSV field owns its characters, so a comparison against a string
        // property meets a borrowed view on one side and an owned string on the other -
        // either way round, since which side the query writes it on is its choice
        OptionalKindPairs<types::String::Primitive, types::String::OwningPrimitive>::Pairs,
        OptionalKindPairs<types::String::OwningPrimitive, types::String::Primitive>::Pairs,
        OptionalKindPairs<types::String::OwningPrimitive, types::String::OwningPrimitive>::Pairs,

        std::tuple<
            KindPair<ListElementView, ListElementView>,

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
            KindPair<std::optional<types::Bool::Primitive>, PropertyNull>,
            KindPair<std::optional<types::Embedding::Primitive>, PropertyNull>,

            KindPair<NodeID, types::Int64::Primitive>,                // WHERE n = 1
            KindPair<NodeID, std::optional<types::Int64::Primitive>>, // WHERE e = e.age
            KindPair<EdgeID, types::Int64::Primitive>,                // WHERE n = 1
            KindPair<EdgeID, std::optional<types::Int64::Primitive>>  // WHERE e = e.dur
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
    requires (Op == OP_GREATER_THAN_OR_EQUAL) || (Op == OP_LESS_THAN_OR_EQUAL)
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
    requires (Op == OP_STARTS_WITH) || (Op == OP_ENDS_WITH) || (Op == OP_CONTAINS)
struct PairRestrictions<Op> {
    using Allowed = GenerateKindPairList<
        OptionalKindPairs<types::String::Primitive, types::String::Primitive>::Pairs,
        OptionalKindPairs<types::String::Primitive, types::String::OwningPrimitive>::Pairs,
        OptionalKindPairs<types::String::OwningPrimitive, types::String::Primitive>::Pairs,
        OptionalKindPairs<types::String::OwningPrimitive, types::String::OwningPrimitive>::Pairs,

        // A type-erased cell against a string, or against another cell, read by its tag
        ListElementKindPairs<types::String::Primitive>::Pairs,
        ListElementKindPairs<types::String::OwningPrimitive>::Pairs,

        std::tuple<KindPair<ListElementView, ListElementView>>
    >;

    using AllowedMixed = AllowedMixedList<>;

    using Excluded = ExcludedContainers<
        ContainerKind::code<ColumnSet>(),
        ContainerKind::code<ColumnMask>()
    >;
};

template <ColumnOperator Op>
    requires (Op == OP_AND) || (Op == OP_OR) || (Op == OP_XOR)
struct PairRestrictions<Op> {
    using Allowed = GenerateKindPairList<
        // Boolean properties; optional (3-valued logic) and non-optional
        OptionalKindPairs<types::Bool::Primitive, types::Bool::Primitive>::Pairs
    >;

    using AllowedMixed = AllowedMixedList<
        MixedKind<ColumnMask, CustomBool>,
        MixedKind<ColumnMask, std::optional<CustomBool>>
    >;

    // Mask operations also included
    using Excluded = ExcludedContainers<
        ContainerKind::code<ColumnSet>()
    >;
};

template <ColumnOperator Op>
    requires (Op == OP_ADD) || (Op == OP_SUB) || (Op == OP_MUL) || (Op == OP_DIV)
          || (Op == OP_MOD) || (Op == OP_POW)
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

template <ColumnOperator Op>
    requires (Op == OP_CONCAT)
struct PairRestrictions<Op> {
    using Allowed = GenerateKindPairList<
        OptionalKindPairs<types::String::Primitive, types::String::Primitive>::Pairs
    >;

    using AllowedMixed = AllowedMixedList<>;

    using Excluded = ExcludedContainers<
        ContainerKind::code<ColumnSet>(),
        ContainerKind::code<ColumnMask>()
    >;
};

struct MaskedPairs {
    using Allowed = GenerateKindPairList<>;

    using AllowedMixed = AllowedMixedList<
        MixedKind<ColumnMask, types::Int64::Primitive>,
        MixedKind<ColumnMask, types::Int64::Primitive>,
        MixedKind<ColumnMask, types::UInt64::Primitive>,
        MixedKind<ColumnMask, types::Double::Primitive>,
        MixedKind<ColumnMask, types::String::Primitive>,
        MixedKind<ColumnMask, types::Bool::Primitive>,
        MixedKind<ColumnMask, types::Embedding::Primitive>,
        MixedKind<ColumnMask, std::optional<types::Int64::Primitive>>,
        MixedKind<ColumnMask, std::optional<types::Int64::Primitive>>,
        MixedKind<ColumnMask, std::optional<types::UInt64::Primitive>>,
        MixedKind<ColumnMask, std::optional<types::Double::Primitive>>,
        MixedKind<ColumnMask, std::optional<types::String::Primitive>>,
        MixedKind<ColumnMask, std::optional<types::Bool::Primitive>>,
        MixedKind<ColumnMask, std::optional<types::Embedding::Primitive>>,
        MixedKind<ColumnMask, PropertyNull>,

        MixedKind<ColumnMask, EntityID>,
        MixedKind<ColumnMask, NodeID>,
        MixedKind<ColumnMask, EdgeID>,
        MixedKind<ColumnMask, LabelID>,
        MixedKind<ColumnMask, Path>,
        MixedKind<ColumnMask, LabelSetID>,
        MixedKind<ColumnMask, EdgeTypeID>,
        MixedKind<ColumnMask, ValueType>,
        MixedKind<ColumnMask, PropertyTypeID>,
        MixedKind<ColumnMask, CommitHash>,
        MixedKind<ColumnMask, ChangeID>,
        MixedKind<ColumnMask, std::string>,
        MixedKind<ColumnMask, size_t>,
        MixedKind<ColumnMask, EntityList>
    >;

    using Excluded = ExcludedContainers<
        ContainerKind::code<ColumnSet>(),
        ContainerKind::code<ColumnConst>()
    >;
};

// Restriction for Unary operators
template <ColumnOperator Op>
struct TypeRestrictions;

template <>
struct TypeRestrictions<OP_NOT> {
    using Allowed = GenerateKindList<std::tuple<
        types::Bool::Primitive,
        std::optional<types::Bool::Primitive>
    >>;

    using Excluded = ExcludedContainers<
        ContainerKind::code<ColumnSet>(),
        ContainerKind::code<ColumnMask>()
    >;
};

template <>
struct TypeRestrictions<OP_FUNC_LABELS> {
    using Allowed = GenerateKindList<std::tuple<
        NodeID
    >>;

    using Excluded = ExcludedContainers<
        ContainerKind::code<ColumnSet>(),
        ContainerKind::code<ColumnMask>(),
        ContainerKind::code<ColumnConst>()
    >;
};

template <>
struct TypeRestrictions<OP_FUNC_EDGE_TYPES> {
    using Allowed = GenerateKindList<std::tuple<
        EdgeID
    >>;

    using Excluded = ExcludedContainers<
        ContainerKind::code<ColumnSet>(),
        ContainerKind::code<ColumnMask>(),
        ContainerKind::code<ColumnConst>()
    >;
};

template <ColumnOperator Func>
    requires NumericConversionFunction<Func>
struct TypeRestrictions<Func> {
    using Allowed = GenerateKindList<std::tuple<
        std::string,
        types::String::Primitive,
        types::Int64::Primitive,
        types::UInt64::Primitive,
        types::Double::Primitive
    >>;

    using Excluded = ExcludedContainers<
        ContainerKind::code<ColumnSet>(),
        ContainerKind::code<ColumnMask>()
    >;
};

template <ColumnOperator Func>
    requires BooleanConversionFunction<Func>
struct TypeRestrictions<Func> {
    using Allowed = GenerateKindList<std::tuple<
        std::string,
        types::String::Primitive
    >>;

    using Excluded = ExcludedContainers<
        ContainerKind::code<ColumnSet>(),
        ContainerKind::code<ColumnMask>()
    >;
};

template <ColumnOperator Op>
requires (Op == OP_FUNC_COSINE_SIMILARITY) || (Op == OP_FUNC_EUCLIDEAN_DISTANCE)
struct PairRestrictions<Op> {
    using Allowed = GenerateKindPairList<
        OptionalKindPairs<types::Embedding::Primitive, types::Embedding::Primitive>::Pairs
    >;
    using AllowedMixed = AllowedMixedList<>;
    using Excluded = ExcludedContainers<
        ContainerKind::code<ColumnSet>(),
        ContainerKind::code<ColumnMask>()
    >;
};

/// Types that are outputted by queries, used in @ref QueryTestRunner
struct OutputtedTypes {
    using Allowed = GenerateKindList<std::tuple<
        types::Int64::Primitive,
        types::UInt64::Primitive,
        types::Double::Primitive,
        types::String::Primitive,
        types::Bool::Primitive,
        types::Embedding::Primitive,
        std::optional<types::Int64::Primitive>,
        std::optional<types::Int64::Primitive>,
        std::optional<types::UInt64::Primitive>,
        std::optional<types::Double::Primitive>,
        std::optional<types::String::Primitive>,
        std::optional<types::Bool::Primitive>,
        std::optional<types::Embedding::Primitive>,
        PropertyNull,

        std::optional<std::string>,

        NodeID,
        EdgeID,
        LabelID,
        Path,
        LabelSetID,
        EdgeTypeID,
        ValueType,
        PropertyTypeID,
        CommitHash,
        ChangeID,
        std::string,
        size_t,
        EntityList,

        ListView,
        ListElementView
    >>;

  using Excluded = ExcludedContainers<ContainerKind::code<ColumnSet>(),
                                      ContainerKind::code<ColumnMask>()>;
};

/// Totally ordered types, e.g. sorted in ORDER BY
struct OrderedTypes {
    using Allowed = GenerateKindList<std::tuple<
        // All property types and their optional variants
        types::Int64::Primitive,
        types::UInt64::Primitive,
        types::Double::Primitive,
        types::String::Primitive,
        types::Bool::Primitive,

        std::optional<types::Int64::Primitive>,
        std::optional<types::UInt64::Primitive>,
        std::optional<types::Double::Primitive>,
        std::optional<types::String::Primitive>,
        std::optional<types::Bool::Primitive>,

        // Entities
        NodeID,
        EdgeID,
        EdgeTypeID,
        PropertyTypeID,
        LabelID,
        LabelSetID,

        // Tagged scalars
        ListElementView
    >>;

    using Excluded = ExcludedContainers<
        ContainerKind::code<ColumnSet>(),
        ContainerKind::code<ColumnMask>()
    >;
};

struct WriteProcessorPropertyTypes {
    // Used for specialised dispatching logic for only vectors (can hold optional)
    using AllowedVector = GenerateKindList<std::tuple<
        types::Int64::Primitive,
        types::UInt64::Primitive,
        types::Double::Primitive,
        types::String::Primitive,
        types::Bool::Primitive,
        types::Embedding::Primitive,

        std::optional<types::Int64::Primitive>,
        std::optional<types::UInt64::Primitive>,
        std::optional<types::Double::Primitive>,
        std::optional<types::String::Primitive>,
        std::optional<types::Bool::Primitive>,
        std::optional<types::Embedding::Primitive>,

        std::string // For LOAD CSV inputs
    >>;

    // Used for specialised dispatching logic for only consts (shouldn't hold optional)
    using AllowedConst = GenerateKindList<std::tuple<
        types::Int64::Primitive,
        types::UInt64::Primitive,
        types::Double::Primitive,
        types::String::Primitive,
        types::Bool::Primitive,
        types::Embedding::Primitive,

        std::string // For LOAD CSV inputs
    >>;

    // Used for specialised dispatching logic for only vectors
    using ExcludedVector = ExcludedContainers<
        ContainerKind::code<ColumnSet>(),
        ContainerKind::code<ColumnConst>(),
        ContainerKind::code<ColumnMask>()
    >;

    // Used for specialised dispatching logic for only consts
    using ExcludedConst = ExcludedContainers<
        ContainerKind::code<ColumnSet>(),
        ContainerKind::code<ColumnVector>(),
        ContainerKind::code<ColumnMask>()
    >;

};

struct ConstScanTypes {
    using Allowed = GenerateKindList<std::tuple<
        NodeID,

        types::Int64::Primitive,
        types::UInt64::Primitive,
        types::Double::Primitive,
        types::String::Primitive,
        types::Bool::Primitive,
        types::Embedding::Primitive
    >>;

    using Excluded = ExcludedContainers<
        ContainerKind::code<ColumnConst>(),
        ContainerKind::code<ColumnSet>(),
        ContainerKind::code<ColumnMask>()
    >;
};

/// Types that are used as the Key in indexes
struct IndexedTypes {
    using Allowed = GenerateKindList<std::tuple<
        types::Int64::Primitive,
        types::UInt64::Primitive,
        types::Double::Primitive,
        types::String::Primitive,
        types::Bool::Primitive,
        ListView
    >>;
    using Excluded = ExcludedContainers<
        ContainerKind::code<ColumnConst>(),
        ContainerKind::code<ColumnSet>(),
        ContainerKind::code<ColumnMask>()
    >;
};

/// Types that are allowed in lists
struct ListableTypes {
    using Allowed = GenerateKindList<ListBuffer<>::ListableTypes>;

    /// For constructing list literals: all elements should be const
    using LiteralExcluded = ExcludedContainers<
        ContainerKind::code<ColumnVector>(),
        ContainerKind::code<ColumnSet>(),
        ContainerKind::code<ColumnMask>()
    >;
};

// NOTE: This should be synced with @ref JoinNode::joinableTypes
struct ValueHashJoinPairs {
    using Allowed = GenerateKindPairList<
        // Non-optionals on ID types
        std::tuple<
            KindPair<NodeID, NodeID>,
            KindPair<EdgeID, EdgeID>,

            KindPair<LabelID, LabelID>,
            KindPair<EdgeTypeID, EdgeTypeID>,

            KindPair<ValueType, ValueType>
        >,

        /// Property types: all optional variations
        OptionalKindPairs<types::Int64::Primitive, types::Int64::Primitive>::Pairs,
        OptionalKindPairs<types::UInt64::Primitive, types::UInt64::Primitive>::Pairs,
        OptionalKindPairs<types::Double::Primitive, types::Double::Primitive>::Pairs,
        OptionalKindPairs<types::Bool::Primitive, types::Bool::Primitive>::Pairs,
        OptionalKindPairs<types::String::Primitive, types::String::Primitive>::Pairs,

        // Occasionally need owning strings
        OptionalKindPairs<std::string, std::string>::Pairs,
        /// Enabled due to transparent string hashing in @ref HashJoinProcessor
        OptionalKindPairs<std::string, std::string_view>::Pairs,
        OptionalKindPairs<std::string_view, std::string_view>::Pairs
    >;

    using AllowedMixed = AllowedMixedList<>;

    using Excluded = ExcludedContainers<
        ContainerKind::code<ColumnConst>(),
        ContainerKind::code<ColumnSet>(),
        ContainerKind::code<ColumnMask>()
    >;
};

// Column types allowed as an input to @ref CartesianProductProcessor
struct CartesianProductKinds {
    using Allowed = GenerateKindList<std::tuple<
        // All property types and their optional variants
        types::Int64::Primitive,
        types::UInt64::Primitive,
        types::Double::Primitive,
        types::String::Primitive,
        types::Bool::Primitive,

        std::optional<types::Int64::Primitive>,
        std::optional<types::UInt64::Primitive>,
        std::optional<types::Double::Primitive>,
        std::optional<types::String::Primitive>,
        std::optional<types::Bool::Primitive>,

        // Entities and metadata
        NodeID,
        EdgeID,
        EdgeTypeID,
        PropertyTypeID,
        LabelID,
        LabelSetID,
        ValueType,

        // Lists
        ListView,
        ListElementView,
        EntityList,

        // Owning strings occasionally needed for procedures; tests
        std::string
    >>;

    using Excluded = ExcludedContainers<
        ContainerKind::code<ColumnSet>(),
        ContainerKind::code<ColumnMask>()
    >;
};

/// Totally ordered types, e.g. sorted in ORDER BY
struct NumericallyAggregatedTypes {
    using Allowed = GenerateKindList<std::tuple<
        // All property types and their optional variants
        types::Int64::Primitive,
        types::UInt64::Primitive,
        types::Double::Primitive,

        std::optional<types::Int64::Primitive>,
        std::optional<types::UInt64::Primitive>,
        std::optional<types::Double::Primitive>
    >>;

    using Excluded = ExcludedContainers<
        ContainerKind::code<ColumnSet>(),
        ContainerKind::code<ColumnMask>()
    >;
};

}

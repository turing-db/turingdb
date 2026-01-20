#include "EvalBinaryExpr.h"

#include "columns/ColumnOperationExecutor.h"

#include "Panic.h"

using namespace db;

namespace {

template <ColumnOperator Op>
struct EqualEval {
    Column* _res {nullptr};

    template <typename T, typename U>
    void operator()(const T* lhs, const U* rhs) {
        bioassert(_res && lhs && rhs, "Invalid inputs to Equal");

        if constexpr (Op == OP_EQUAL) {
            // using ResultType = ColumnCombinations<Equal, T, U>::ResultType;
            // execOperation<Equal>(static_cast<ResultType*>(_res), lhs, rhs);
            fmt::println("EQUAL {}, {}", typeid(*lhs).name(), typeid(*rhs).name());
        } else if constexpr (Op == OP_NOT_EQUAL) {
            // using ResultType = ColumnCombinations<NotEqual, T, U>::ResultType;
            // execOperation<NotEqual>(static_cast<ResultType*>(_res), lhs, rhs);
            fmt::println("NOT EQUAL {}, {}", typeid(*lhs).name(), typeid(*rhs).name());
        } else {
            COMPILE_ERROR("Invalid operator for Equal");
        }
    }
};

template <ColumnOperator Op>
struct CompareEval {
    Column* _res {nullptr};

    void operator()(const auto* lhs, const auto* rhs) {
        bioassert(_res && lhs && rhs, "Invalid inputs to Compare");

        if constexpr (Op == OP_GREATER_THAN) {
            fmt::println("GREATER_THAN_OR_EQUAL {}, {}", typeid(*lhs).name(), typeid(*rhs).name());
        } else if constexpr (Op == OP_LESS_THAN) {
            fmt::println("LESS_THAN_OR_EQUAL {}, {}", typeid(*lhs).name(), typeid(*rhs).name());
        } else {
            COMPILE_ERROR("Invalid operator for Compare");
        }
    }
};

template <ColumnOperator Op>
struct CompareEqualEval {
    Column* _res {nullptr};

    void operator()(const auto* lhs, const auto* rhs) {
        bioassert(_res && lhs && rhs, "Invalid inputs to CompareEqual");

        if constexpr (Op == OP_GREATER_THAN_OR_EQUAL) {
            fmt::println("GREATER_THAN {}, {}", typeid(*lhs).name(), typeid(*rhs).name());
        } else if constexpr (Op == OP_LESS_THAN_OR_EQUAL) {
            fmt::println("LESS_THAN_OR_EQUAL {}, {}", typeid(*lhs).name(), typeid(*rhs).name());
        } else {
            COMPILE_ERROR("Invalid operator for CompareEqual");
        }
    }
};

template <ColumnOperator Op>
struct BooleanEval {
    Column* _res {nullptr};

    void operator()(const auto* lhs, const auto* rhs) {
        bioassert(_res && lhs && rhs, "Invalid inputs to Boolean");

        if constexpr (Op == OP_AND) {
            fmt::println("AND {}, {}", typeid(*lhs).name(), typeid(*rhs).name());
        } else if constexpr (Op == OP_OR) {
            fmt::println("OR {}, {}", typeid(*lhs).name(), typeid(*rhs).name());
        } else if constexpr (Op == OP_NOT) {
            fmt::println("NOT {}", typeid(*lhs).name());
        } else {
            COMPILE_ERROR("Invalid operator for Boolean");
        }
    }
};

template <ColumnOperator Op>
struct ArithmeticEval {
    Column* _res {nullptr};

    void operator()(const auto* lhs, const auto* rhs) {
        bioassert(_res && lhs && rhs, "Invalid inputs to Arithmetic");

        if constexpr (Op == OP_MINUS) {
            fmt::println("MINUS {}, {}", typeid(*lhs).name(), typeid(*rhs).name());
        } else if constexpr (Op == OP_PLUS) {
            fmt::println("PLUS {}, {}", typeid(*lhs).name(), typeid(*rhs).name());
        } else {
            COMPILE_ERROR("Invalid operator for Arithmetic");
        }
    }
};

}

template <ColumnOperator Op>
void EvalBinaryExpr::opEqual(Column* res, const Column* lhs, const Column* rhs) {
    using Allowed = GenerateKindPairList<
        OptionalKindPairs<int64_t, int64_t>::Pairs,
        OptionalKindPairs<int64_t, uint64_t>::Pairs,
        OptionalKindPairs<uint64_t, uint64_t>::Pairs,
        OptionalKindPairs<CustomBool, CustomBool>::Pairs,
        OptionalKindPairs<std::string_view, std::string_view>::Pairs,
        OptionalKindPairs<EntityID, EntityID>::Pairs,
        OptionalKindPairs<EntityID, NodeID>::Pairs,
        OptionalKindPairs<EntityID, EdgeID>::Pairs,
        OptionalKindPairs<NodeID, NodeID>::Pairs,
        OptionalKindPairs<EdgeID, EdgeID>::Pairs,

        std::tuple<KindPair<PropertyNull, std::optional<EntityID>>,
                   KindPair<PropertyNull, std::optional<NodeID>>,
                   KindPair<PropertyNull, std::optional<EdgeID>>,
                   KindPair<PropertyNull, std::optional<int64_t>>,
                   KindPair<PropertyNull, std::optional<uint64_t>>,
                   KindPair<PropertyNull, std::optional<double>>,
                   KindPair<PropertyNull, std::optional<std::string_view>>,
                   KindPair<PropertyNull, std::optional<CustomBool>>,
                   KindPair<PropertyNull, std::optional<ValueType>>>>;

    using AllowedMixed = AllowedMixedList<
        MixedKind<ColumnMask, PropertyNull>,
        MixedKind<ColumnMask, CustomBool>,
        MixedKind<ColumnMask, std::optional<CustomBool>>>;

    using Excluded = ExcludedContainers<ContainerKind::code<ColumnSet>(),
                                        ContainerKind::code<ColumnMask>()>;

    EqualEval<Op> fn {res};
    ColumnDoubleDispatcher<Allowed, AllowedMixed, ::EqualEval<Op>, Excluded>::dispatch(lhs, rhs, fn);
}

template <ColumnOperator Op>
void EvalBinaryExpr::opCompare(Column* res, const Column* lhs, const Column* rhs) {
    using Allowed = GenerateKindPairList<
        OptionalKindPairs<int64_t, int64_t>::Pairs,
        OptionalKindPairs<int64_t, uint64_t>::Pairs,
        OptionalKindPairs<int64_t, double>::Pairs,
        OptionalKindPairs<uint64_t, uint64_t>::Pairs,
        OptionalKindPairs<uint64_t, double>::Pairs,
        OptionalKindPairs<double, double>::Pairs>;

    using AllowedMixed = AllowedMixedList<>;

    CompareEval<Op> fn {res};
    ColumnDoubleDispatcher<Allowed, AllowedMixed, ::CompareEval<Op>>::dispatch(lhs, rhs, fn);
}

template <ColumnOperator Op>
void EvalBinaryExpr::opCompareEqual(Column* res, const Column* lhs, const Column* rhs) {
    using Allowed = GenerateKindPairList<
        OptionalKindPairs<int64_t, int64_t>::Pairs,
        OptionalKindPairs<int64_t, uint64_t>::Pairs,
        OptionalKindPairs<uint64_t, uint64_t>::Pairs>;

    using AllowedMixed = AllowedMixedList<>;

    CompareEqualEval<Op> fn {res};
    ColumnDoubleDispatcher<Allowed, AllowedMixed, ::CompareEqualEval<Op>>::dispatch(lhs, rhs, fn);
}

template <ColumnOperator Op>
void EvalBinaryExpr::opBoolean(Column* res, const Column* lhs, const Column* rhs) {
    using Allowed = GenerateKindPairList<
        OptionalKindPairs<CustomBool, CustomBool>::Pairs,
        std::tuple<KindPair<PropertyNull, CustomBool>,
                   KindPair<PropertyNull, std::optional<CustomBool>>>>;

    using AllowedMixed = AllowedMixedList<
        MixedKind<ColumnMask, PropertyNull>,
        MixedKind<ColumnMask, CustomBool>,
        MixedKind<ColumnMask, std::optional<CustomBool>>>;

    BooleanEval<Op> fn {res};
    ColumnDoubleDispatcher<Allowed, AllowedMixed, ::BooleanEval<Op>>::dispatch(lhs, rhs, fn);
}

template <ColumnOperator Op>
void EvalBinaryExpr::opArithmetic(Column* res, const Column* lhs, const Column* rhs) {
    using Allowed = GenerateKindPairList<
        OptionalKindPairs<int64_t, int64_t>::Pairs,
        OptionalKindPairs<int64_t, uint64_t>::Pairs,
        OptionalKindPairs<int64_t, double>::Pairs,
        OptionalKindPairs<uint64_t, uint64_t>::Pairs,
        OptionalKindPairs<uint64_t, double>::Pairs,
        OptionalKindPairs<double, double>::Pairs>;

    using AllowedMixed = AllowedMixedList<>;

    ArithmeticEval<Op> fn {res};
    ColumnDoubleDispatcher<Allowed, AllowedMixed, ::ArithmeticEval<Op>>::dispatch(lhs, rhs, fn);
}

template void EvalBinaryExpr::opEqual<OP_EQUAL>(Column* res, const Column* lhs, const Column* rhs);
template void EvalBinaryExpr::opEqual<OP_NOT_EQUAL>(Column* res, const Column* lhs, const Column* rhs);

template void EvalBinaryExpr::opCompare<OP_GREATER_THAN>(Column* res, const Column* lhs, const Column* rhs);
template void EvalBinaryExpr::opCompare<OP_LESS_THAN>(Column* res, const Column* lhs, const Column* rhs);

template void EvalBinaryExpr::opCompareEqual<OP_GREATER_THAN_OR_EQUAL>(Column* res, const Column* lhs, const Column* rhs);
template void EvalBinaryExpr::opCompareEqual<OP_LESS_THAN_OR_EQUAL>(Column* res, const Column* lhs, const Column* rhs);

template void EvalBinaryExpr::opBoolean<OP_AND>(Column* res, const Column* lhs, const Column* rhs);
template void EvalBinaryExpr::opBoolean<OP_OR>(Column* res, const Column* lhs, const Column* rhs);

template void EvalBinaryExpr::opArithmetic<OP_MINUS>(Column* res, const Column* lhs, const Column* rhs);
template void EvalBinaryExpr::opArithmetic<OP_PLUS>(Column* res, const Column* lhs, const Column* rhs);

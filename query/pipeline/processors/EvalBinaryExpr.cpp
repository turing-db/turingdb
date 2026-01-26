#include "EvalBinaryExpr.h"

#include "columns/AllowedPairs.h"
#include "columns/ColumnOperationExecutor.h"
#include "columns/BinaryOperators.h"
#include "columns/BinaryPredicates.h"
#include "columns/ColumnCombinations.h"

#include "Panic.h"
#include "BioAssert.h"
#include "columns/ColumnOperator.h"

using namespace db;

namespace {

template <ColumnOperator Op>
struct EqualEval {
    Column* _res {nullptr};

    template <typename T, typename U>
    void operator()(const T* lhs, const U* rhs) {
        bioassert(_res && lhs && rhs, "Invalid inputs to Equal");

        if constexpr (Op == OP_EQUAL) {
            using ResultType = ColumnCombination<Eq, T, U>::ResultColumnType;
            auto* result = dynamic_cast<ResultType*>(_res);
            bioassert(result, "Invalid to cast for result column for Eq.");
            exec<Eq>(result, lhs, rhs);
        } else if constexpr (Op == OP_NOT_EQUAL) {
            using ResultType = ColumnCombination<Ne, T, U>::ResultColumnType;
            auto* result = dynamic_cast<ResultType*>(_res);
            bioassert(result, "Invalid to cast for result column for Ne.");
            exec<Ne>(result, lhs, rhs);
        } else {
            COMPILE_ERROR("Invalid operator for Equal");
        }
    }
};

template <ColumnOperator Op>
struct CompareEval {
    Column* _res {nullptr};

    template <typename T, typename U>
    void operator()(const T* lhs, const U* rhs) {
        bioassert(_res && lhs && rhs, "Invalid inputs to Compare");

        if constexpr (Op == OP_GREATER_THAN) {
            using ResultType = ColumnCombination<Gt, T, U>::ResultColumnType;
            auto* result = dynamic_cast<ResultType*>(_res);
            bioassert(result, "Invalid to cast for result column for Gt.");
            exec<Gt>(result, lhs, rhs);
        } else if constexpr (Op == OP_LESS_THAN) {
            using ResultType = ColumnCombination<Lt, T, U>::ResultColumnType;
            auto* result = dynamic_cast<ResultType*>(_res);
            bioassert(result, "Invalid to cast for result column for Lt.");
            exec<Lt>(result, lhs, rhs);
        } else {
            COMPILE_ERROR("Invalid operator for Compare");
        }
    }
};

template <ColumnOperator Op>
struct CompareEqualEval {
    Column* _res {nullptr};

    template <typename T, typename U>
    void operator()(const T* lhs, const U* rhs) {
        bioassert(_res && lhs && rhs, "Invalid inputs to CompareEqual");

        if constexpr (Op == OP_GREATER_THAN_OR_EQUAL) {
            using ResultType = ColumnCombination<Gte, T, U>::ResultColumnType;
            auto* result = dynamic_cast<ResultType*>(_res);
            bioassert(result, "Invalid to cast for result column for Gt.");
            exec<Gte>(result, lhs, rhs);
        } else if constexpr (Op == OP_LESS_THAN_OR_EQUAL) {
            using ResultType = ColumnCombination<Lte, T, U>::ResultColumnType;
            auto* result = dynamic_cast<ResultType*>(_res);
            bioassert(result, "Invalid to cast for result column for Gt.");
            exec<Lte>(result, lhs, rhs);
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
    using Pairs = PairRestrictions<Op>;
    EqualEval<Op> fn {res};
    ColumnDoubleDispatcher<typename Pairs::Allowed,
                           typename Pairs::AllowedMixed,
                           ::EqualEval<Op>,
                           typename Pairs::Excluded>::dispatch(lhs, rhs, fn);
}

template <ColumnOperator Op>
void EvalBinaryExpr::opCompare(Column* res, const Column* lhs, const Column* rhs) {
    using Pairs = PairRestrictions<Op>;
    CompareEval<Op> fn {res};
    ColumnDoubleDispatcher<typename Pairs::Allowed,
                           typename Pairs::AllowedMixed,
                           ::CompareEval<Op>,
                           typename Pairs::Excluded>::dispatch(lhs, rhs, fn);
}

template <ColumnOperator Op>
void EvalBinaryExpr::opCompareEqual(Column* res, const Column* lhs, const Column* rhs) {
    using Pairs = PairRestrictions<Op>;
    CompareEqualEval<Op> fn {res};
    ColumnDoubleDispatcher<typename Pairs::Allowed,
                           typename Pairs::AllowedMixed,
                           ::CompareEqualEval<Op>,
                           typename Pairs::Excluded>::dispatch(lhs, rhs, fn);
}

template <ColumnOperator Op>
void EvalBinaryExpr::opBoolean(Column* res, const Column* lhs, const Column* rhs) {
    using Pairs = PairRestrictions<Op>;
    BooleanEval<Op> fn {res};
    ColumnDoubleDispatcher<typename Pairs::Allowed,
                           typename Pairs::AllowedMixed,
                           ::BooleanEval<Op>,
                           typename Pairs::Excluded>::dispatch(lhs, rhs, fn);
}

template <ColumnOperator Op>
void EvalBinaryExpr::opArithmetic(Column* res, const Column* lhs, const Column* rhs) {
    using Pairs = PairRestrictions<Op>;
    ArithmeticEval<Op> fn {res};
    ColumnDoubleDispatcher<typename Pairs::Allowed,
                           typename Pairs::AllowedMixed,
                           ::ArithmeticEval<Op>,
                           typename Pairs::Excluded>::dispatch(lhs, rhs, fn);
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

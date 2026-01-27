#include "EvalBinaryExpr.h"

#include "PipelineException.h"
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
struct Eval {
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
        } else if constexpr (Op == OP_GREATER_THAN) {
            using ResultType = ColumnCombination<Gt, T, U>::ResultColumnType;
            auto* result = dynamic_cast<ResultType*>(_res);
            bioassert(result, "Invalid to cast for result column for Gt.");
            exec<Gt>(result, lhs, rhs);
        } else if constexpr (Op == OP_LESS_THAN) {
            using ResultType = ColumnCombination<Lt, T, U>::ResultColumnType;
            auto* result = dynamic_cast<ResultType*>(_res);
            bioassert(result, "Invalid to cast for result column for Lt.");
            exec<Lt>(result, lhs, rhs);
        } else if constexpr (Op == OP_GREATER_THAN_OR_EQUAL) {
            using ResultType = ColumnCombination<Gte, T, U>::ResultColumnType;
            auto* result = dynamic_cast<ResultType*>(_res);
            bioassert(result, "Invalid to cast for result column for Gte.");
            exec<Gte>(result, lhs, rhs);
        } else if constexpr (Op == OP_LESS_THAN_OR_EQUAL) {
            using ResultType = ColumnCombination<Lte, T, U>::ResultColumnType;
            auto* result = dynamic_cast<ResultType*>(_res);
            bioassert(result, "Invalid to cast for result column for Lte.");
            exec<Lte>(result, lhs, rhs);
        } else if constexpr (Op == OP_AND) {
            using ResultType = ColumnCombination<And, T, U>::ResultColumnType;
            auto* result = dynamic_cast<ResultType*>(_res);
            bioassert(result, "Invalid to cast for result column for And.");
            exec<And>(result, lhs, rhs);
        } else if constexpr (Op == OP_OR) {
            using ResultType = ColumnCombination<Or, T, U>::ResultColumnType;
            auto* result = dynamic_cast<ResultType*>(_res);
            bioassert(result, "Invalid to cast for result column for Or.");
            exec<Or>(result, lhs, rhs);
        } else if constexpr (Op == OP_MINUS) {
            fmt::println("MINUS {}, {}", typeid(*lhs).name(), typeid(*rhs).name());
            throw PipelineException("Sub not yet implemented");
        } else if constexpr (Op == OP_PLUS) {
            fmt::println("PLUS {}, {}", typeid(*lhs).name(), typeid(*rhs).name());
            throw PipelineException("Add not yet implemented");
        } else {
            COMPILE_ERROR("Unknown operator");
        }
    }
};

}

template <ColumnOperator Op>
void EvalBinaryExpr::eval(Column* res, const Column* lhs, const Column* rhs) {
    using Pairs = PairRestrictions<Op>;
    Eval<Op> fn {res};
    ColumnDoubleDispatcher<typename Pairs::Allowed,
                           typename Pairs::AllowedMixed,
                           Eval<Op>,
                           typename Pairs::Excluded>::dispatch(lhs, rhs, fn);
}

template void EvalBinaryExpr::eval<OP_EQUAL>(Column* res, const Column* lhs, const Column* rhs);
template void EvalBinaryExpr::eval<OP_NOT_EQUAL>(Column* res, const Column* lhs, const Column* rhs);

template void EvalBinaryExpr::eval<OP_GREATER_THAN>(Column* res, const Column* lhs, const Column* rhs);
template void EvalBinaryExpr::eval<OP_LESS_THAN>(Column* res, const Column* lhs, const Column* rhs);

template void EvalBinaryExpr::eval<OP_GREATER_THAN_OR_EQUAL>(Column* res, const Column* lhs, const Column* rhs);
template void EvalBinaryExpr::eval<OP_LESS_THAN_OR_EQUAL>(Column* res, const Column* lhs, const Column* rhs);

template void EvalBinaryExpr::eval<OP_AND>(Column* res, const Column* lhs, const Column* rhs);
template void EvalBinaryExpr::eval<OP_OR>(Column* res, const Column* lhs, const Column* rhs);

template void EvalBinaryExpr::eval<OP_MINUS>(Column* res, const Column* lhs, const Column* rhs);
template void EvalBinaryExpr::eval<OP_PLUS>(Column* res, const Column* lhs, const Column* rhs);

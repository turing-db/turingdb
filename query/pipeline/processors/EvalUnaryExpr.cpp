#include "EvalUnaryExpr.h"

#include "columns/AllowedKinds.h"
#include "columns/UnaryPredicates.h"
#include "columns/ColumnOperatorDispatcher.h"
#include "columns/ColumnOperators.h"

#include "Panic.h"

using namespace db;

namespace {

template <ColumnOperator Op>
struct Eval {
    Column* _res {nullptr};

    template <typename T>
    void operator()(const T* arg) {
        bioassert(_res && arg, "Invalid inputs to Boolean");

        if constexpr (Op == OP_NOT) {
            using ResultType = T; // XXX: Should have unary ColumnCombinations
            auto* result = dynamic_cast<ResultType*>(_res);
            bioassert(result, "Invalid to cast for result column for Not.");
            ColumnOperators::exec<Not>(result, arg);
        } else {
            COMPILE_ERROR("Invalid operator for Unary evaluation.");
        }
    }
};

}

template <ColumnOperator Op>
void EvalUnaryExpr::eval(Column* res, const Column* operand) {
    using Types = TypeRestrictions<Op>;
    Eval<Op> fn {res};
    ColumnSingleDispatcher<typename Types::Allowed,
                           Eval<Op>,
                           typename Types::Excluded>::dispatch(operand, fn);
}

template void EvalUnaryExpr::eval<OP_NOT>(Column* res, const Column* operand);

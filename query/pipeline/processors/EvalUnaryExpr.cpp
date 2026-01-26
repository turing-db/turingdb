#include "EvalUnaryExpr.h"

#include "columns/UnaryPredicates.h"
#include "columns/ColumnOperationExecutor.h"

#include "Panic.h"

using namespace db;

namespace {

template <ColumnOperator Op>
struct BooleanEval {
    Column* _res {nullptr};

    template <typename T>
    void operator()(const T* arg) {
        bioassert(_res && arg, "Invalid inputs to Boolean");

        if constexpr (Op == OP_NOT) {
            using ResultType = T; // XXX: Should have unary ColumnCombinations
            auto* result = dynamic_cast<ResultType*>(_res);
            bioassert(result, "Invalid to cast for result column for Not.");
            exec<Not>(result, arg);
        } else {
            COMPILE_ERROR("Invalid operator for Boolean");
        }
    }
};

}

template <ColumnOperator Op>
void EvalUnaryExpr::opBoolean(Column* res, const Column* operand) {
    using Allowed = GenerateKindList<
        std::tuple<std::optional<CustomBool>>
        // Optional types
        /*OptionalKinds<CustomBool>::Types,

        // Non-optional types
        // TODO: Need add ColumnMask::Bool_t here for ColumnMasks?
        std::tuple<PropertyNull>*/>;

    using Excluded = ExcludedContainers<ContainerKind::code<ColumnSet>(),
                                        ContainerKind::code<ColumnConst>()>;

    BooleanEval<Op> fn {res};
    ColumnSingleDispatcher<Allowed, ::BooleanEval<Op>, Excluded>::dispatch(operand, fn);
}

template void EvalUnaryExpr::opBoolean<OP_NOT>(Column* res, const Column* operand);

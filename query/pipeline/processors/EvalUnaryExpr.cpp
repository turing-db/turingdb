#include "EvalUnaryExpr.h"

#include "columns/ColumnOperationExecutor.h"

#include "Panic.h"

using namespace db;

namespace {

template <ColumnOperator Op>
struct BooleanEval {
    Column* _res {nullptr};

    void operator()(const auto* operand) {
        bioassert(_res && operand, "Invalid inputs to Boolean");

        if constexpr (Op == OP_NOT) {
            fmt::println("NOT {}", typeid(*operand).name());
        } else {
            COMPILE_ERROR("Invalid operator for Boolean");
        }
    }
};

}

template <ColumnOperator Op>
void EvalUnaryExpr::opBoolean(Column* res, const Column* operand) {
    using Allowed = GenerateKindList<
        // Optional types
        OptionalKinds<CustomBool>::Types,

        // Non-optional types
        std::tuple<PropertyNull>>;

    using Excluded = ExcludedContainers<ContainerKind::code<ColumnSet>()>;

    BooleanEval<Op> fn {res};
    ColumnSingleDispatcher<Allowed, ::BooleanEval<Op>, Excluded>::dispatch(operand, fn);
}

template void EvalUnaryExpr::opBoolean<OP_NOT>(Column* res, const Column* operand);

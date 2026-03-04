#pragma once

#include "columns/ColumnOperatorDispatcher.h"
#include "columns/AllowedKinds.h"
#include "columns/ColumnOperator.h"
#include "columns/ColumnOperators.h"
#include "columns/ColumnVector.h"
#include "columns/Functions.h"

namespace db {

class Column;

class EvalFunction {
public:
    template <ColumnOperator Op>
    static void eval(Column* res, const Column* arg, GraphView view);
};

template <ColumnOperator Op>
struct Eval {
    Column* _res {nullptr};
    GraphView _view;

    template <typename T>
    void operator()(const T* arg) {
        bioassert(_res && arg, "Null operands to function.");

        if constexpr (Op == OP_FUNC_LABELS) {
            // TODO: Get from function signature
            using ResultType = ColumnVector<std::string>;
            auto* result = dynamic_cast<ResultType*>(_res);
            bioassert(result, "Invalid cast to result column for labels().");
            ColumnOperators::exec<LabelsFunction>(result, arg, _view);
        } else {
            COMPILE_ERROR("Invalid function.");
        }
    }
};

template <ColumnOperator Op>
void EvalFunction::eval(Column* res, const Column* arg, GraphView view) {
    using Types = TypeRestrictions<Op>;
    Eval<Op> fn {res, view};
    using Dispatcher = ColumnSingleDispatcher<typename Types::Allowed,
                                              Eval<Op>,
                                              typename Types::Excluded>;
    Dispatcher::dispatch(arg, fn);
}

}

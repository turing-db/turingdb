#pragma once

#include "columns/ColumnCombinations.h"
#include "columns/AllowedKinds.h"
#include "columns/ColumnOperator.h"
#include "columns/ColumnOperatorDispatcher.h"
#include "columns/ColumnOperators.h"
#include "columns/Functions.h"

namespace db {

class Column;

class EvalFunction {
public:
    template <ColumnOperator Op>
    static void eval(Column* res, const Column* arg, GraphView view);
    template <ColumnOperator Op>
    static void eval(Column* res, const Column* arg);
};

template <ColumnOperator Op>
struct Eval {
    Column* _res {nullptr};
    GraphView _view;

    template <typename T>
    void operator()(const T* arg) {
        bioassert(_res && arg, "Null operands to function.");

        if constexpr (Op == OP_FUNC_LABELS) {
            using ResultType = FunctionColumnResult<LabelsFunction, T>::ResultColumnType;

            auto* result = dynamic_cast<ResultType*>(_res);
            bioassert(result, "Invalid cast to result column for labels().");

            ColumnFunctions::exec<LabelsFunction>(result, arg, _view);
        } else if constexpr (Op == OP_TO_INTEGER) {
            using ResultType = FunctionColumnResult<toIntegerFunction, T>::ResultColumnType;

            auto* result = dynamic_cast<ResultType*>(_res);
            bioassert(result, "Invalid cast to result column for toInteger().");

            ColumnFunctions::exec<toIntegerFunction>(result, arg);
        } else if constexpr (Op == OP_TO_FLOAT) {
            using ResultType = FunctionColumnResult<toFloatFunction, T>::ResultColumnType;

            auto* result = dynamic_cast<ResultType*>(_res);
            bioassert(result, "Invalid cast to result column for toFloat().");

            ColumnFunctions::exec<toFloatFunction>(result, arg);
        } else if constexpr (Op == OP_TO_BOOLEAN) {
            using ResultType = FunctionColumnResult<toBoolFunction, T>::ResultColumnType;

            auto* result = dynamic_cast<ResultType*>(_res);
            bioassert(result, "Invalid cast to result column for toFloat().");

            ColumnFunctions::exec<toBoolFunction>(result, arg);
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

template <ColumnOperator Op>
void EvalFunction::eval(Column* res, const Column* arg) {
    using Types = TypeRestrictions<Op>;
    Eval<Op> fn {res};
    using Dispatcher = ColumnSingleDispatcher<typename Types::Allowed,
                                              Eval<Op>,
                                              typename Types::Excluded>;
    Dispatcher::dispatch(arg, fn);
}

}

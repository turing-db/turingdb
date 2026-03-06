#pragma once

#include "columns/ColumnCombinations.h"
#include "columns/ColumnOperatorDispatcher.h"
#include "columns/AllowedKinds.h"
#include "columns/ColumnOperator.h"
#include "columns/ColumnOperators.h"
#include "columns/ColumnVector.h"
#include "columns/Functions.h"
#include "metadata/PropertyType.h"

namespace db {

class Column;

static void strToLower(std::string& lower, std::string_view src) {
    lower.clear();
    lower.reserve();
    for (const auto c : src) {
        lower += static_cast<char>(std::tolower(static_cast<unsigned char>(c)));
    }
}

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

/// Specialise toBoolean here at this level so that we can reuse @ref lower over iters
template <>
inline void EvalFunction::eval<OP_TO_BOOLEAN>(Column* res, const Column* arg) {
    using StringCol = ColumnVector<std::string>;
    using BoolCol = ColumnOptVector<types::Bool::Primitive>;

    const size_t size = arg->size();

    // Circumventing dispatcher, so dynamic cast and check manually here
    const auto* src = dynamic_cast<const StringCol*>(arg);
    bioassert(src, "Failed to cast source column of toBoolean().");
    auto* dst = dynamic_cast<BoolCol*>(res);
    bioassert(dst, "Failed to cast result column of toBoolean().");

    dst->resize(size);

    const auto& argd = src->getRaw();
    auto& resd = dst->getRaw();

    std::string lower;
    for (size_t i = 0; i < size; i++) {
        const auto& srcStr = argd[i];

        strToLower(lower, srcStr);
        
        if (lower == "true") {
            resd[i] = CustomBool(true);
        } else if (lower == "false") {
            resd[i] = CustomBool(false);
        } else {
            throw PipelineException(
                fmt::format("toBoolean: cannot convert '{}' to boolean", srcStr));
        }
    }
}

}

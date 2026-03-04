#include "EvalUnaryExpr.h"

#include "columns/AllowedKinds.h"
#include "columns/UnaryPredicates.h"
#include "columns/ColumnOperatorDispatcher.h"
#include "columns/ColumnOperators.h"
#include "columns/ColumnVector.h"
#include "columns/ColumnOptVector.h"
#include "metadata/SupportedType.h"

#include "Panic.h"
#include "PipelineException.h"

#include <spdlog/fmt/fmt.h>

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
            UnaryPredicates::exec<Not>(result, arg);
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

// ---------------------------------------------------------------
// Conversion operators (string -> typed value)
// ---------------------------------------------------------------

template <>
void EvalUnaryExpr::eval<OP_TO_INTEGER>(Column* res, const Column* operand) {
    using StringCol = ColumnVector<std::string>;
    using IntCol = ColumnOptVector<types::Int64::Primitive>;
    const auto* src = static_cast<const StringCol*>(operand);
    auto* dst = static_cast<IntCol*>(res);
    dst->resize(src->size());
    for (size_t i = 0; i < src->size(); i++) {
        const auto& val = (*src)[i];
        try {
            (*dst)[i] = std::stoll(val);
        } catch (...) {
            throw PipelineException(
                fmt::format("toInteger: cannot convert '{}' to integer", val));
        }
    }
}

template <>
void EvalUnaryExpr::eval<OP_TO_FLOAT>(Column* res, const Column* operand) {
    using StringCol = ColumnVector<std::string>;
    using DblCol = ColumnOptVector<types::Double::Primitive>;
    const auto* src = static_cast<const StringCol*>(operand);
    auto* dst = static_cast<DblCol*>(res);
    dst->resize(src->size());
    for (size_t i = 0; i < src->size(); i++) {
        const auto& val = (*src)[i];
        try {
            (*dst)[i] = std::stod(val);
        } catch (...) {
            throw PipelineException(
                fmt::format("toFloat: cannot convert '{}' to float", val));
        }
    }
}

template <>
void EvalUnaryExpr::eval<OP_TO_BOOLEAN>(Column* res, const Column* operand) {
    using StringCol = ColumnVector<std::string>;
    using BoolCol = ColumnOptVector<types::Bool::Primitive>;
    const auto* src = static_cast<const StringCol*>(operand);
    auto* dst = static_cast<BoolCol*>(res);
    dst->resize(src->size());
    std::string lower;
    for (size_t i = 0; i < src->size(); i++) {
        const auto& val = (*src)[i];
        lower.clear();
        lower.reserve(val.size());
        for (char c : val) {
            lower += static_cast<char>(
                std::tolower(static_cast<unsigned char>(c)));
        }
        if (lower == "true") {
            (*dst)[i] = CustomBool(true);
        } else if (lower == "false") {
            (*dst)[i] = CustomBool(false);
        } else {
            throw PipelineException(
                fmt::format("toBoolean: cannot convert '{}' to boolean", val));
        }
    }
}

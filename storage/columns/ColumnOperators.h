#pragma once

#include <type_traits>

#include "BinaryOperators.h"
#include "BinaryPredicates.h"
#include "UnaryPredicates.h"

#include "ColumnCombinations.h"
#include "TypeUtils.h"
#include "columns/Functions.h"
#include "columns/KindTypes.h"

#include "views/GraphView.h"

namespace db {

/// Generic binary operators, returning a non-Bool-like
struct BinaryOperators {
    template <typename Op, typename ColW, typename ColT, typename ColU>
        requires is_result_column<Op, ColT, ColU, ColW>
    static void exec(ColW* res, const ColT* lhs, const ColU* rhs, Op op = {}) {
        static_assert(std::is_trivially_copyable_v<Op>, "NOTE: Op passed by value");

        BinaryOpExecutor<Op>::apply(res, lhs, rhs, op);
    }
};

/// Binary predicates, returning a Bool-like
struct BinaryPredicates {
    template <typename Op, typename ColW, typename ColT, typename ColU>
        requires is_result_column<Op, ColT, ColU, ColW>
    static void exec(ColW* res, const ColT* lhs, const ColU* rhs) {
        BinaryOpExecutor<Op>::apply(res, lhs, rhs);
    }
};

struct ColumnFunctions {
    template <typename Op, typename ColT>
    static void exec(ColumnVector<ListView>* res,
                     const ColT* arg,
                     const GraphView view,
                     QueryListBuffer* listBuffer) {
        using DecayColT = TypeUtils::decay_col_t<ColT>;
        using InternalT = InnerTypeHelper<DecayColT>::type;

        FunctionExecutor<Op, ListView, InternalT>::apply(res, arg, view, listBuffer);
    }

    template <typename Op, typename ColT>
    static void exec(ColumnVector<std::string_view>* res,
                     const ColT* arg,
                     const GraphView view,
                     StringBuffer* stringBuffer) {
        using DecayColT = TypeUtils::decay_col_t<ColT>;
        using InternalT = InnerTypeHelper<DecayColT>::type;

        FunctionExecutor<Op, std::string_view, InternalT>::apply(res, arg, view, stringBuffer);
    }

    template <typename Op, typename ColW, typename ColT>
    static void exec(ColW* res, const ColT* arg) {
        using DecayColW = TypeUtils::decay_col_t<ColW>;
        using InternalW = InnerTypeHelper<DecayColW>::type;

        using DecayColT = TypeUtils::decay_col_t<ColT>;
        using InternalT = InnerTypeHelper<DecayColT>::type;

        FunctionExecutor<Op, InternalW, InternalT>::apply(res, arg);
    }

    /// Binary function (e.g. cosine_similarity, euclidean_distance)
    template <typename Op, typename ColW, typename ColT, typename ColU>
    static void exec(ColW* res, const ColT* lhs, const ColU* rhs) {
        using DecayColT = TypeUtils::decay_col_t<ColT>;
        using InternalT = InnerTypeHelper<DecayColT>::type;

        using DecayColU = TypeUtils::decay_col_t<ColU>;
        using InternalU = InnerTypeHelper<DecayColU>::type;

        using DecayColW = TypeUtils::decay_col_t<ColW>;
        using InternalRes = InnerTypeHelper<DecayColW>::type;

        BinaryFunctionExecutor<Op, InternalRes, InternalT, InternalU>::apply(res, lhs, rhs);
    }
};

}

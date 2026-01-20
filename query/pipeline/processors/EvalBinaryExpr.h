#pragma once

#include "columns/ColumnOperator.h"

namespace db {

class Column;

class EvalBinaryExpr {
public:
    template <ColumnOperator Op>
    static void opEqual(Column* res, const Column* lhs, const Column* rhs);

    template <ColumnOperator Op>
    static void opCompare(Column* res, const Column* lhs, const Column* rhs);

    template <ColumnOperator Op>
    static void opCompareEqual(Column* res, const Column* lhs, const Column* rhs);

    template <ColumnOperator Op>
    static void opBoolean(Column* res, const Column* lhs, const Column* rhs);

    template <ColumnOperator Op>
    static void opArithmetic(Column* res, const Column* lhs, const Column* rhs);
};

}

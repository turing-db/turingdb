#pragma once

#include "columns/ColumnOperator.h"

namespace db {

class Column;

class EvalBinaryExpr {
public:
    template <ColumnOperator Op>
    static void eval(Column* res, const Column* lhs, const Column* rhs);
};

}

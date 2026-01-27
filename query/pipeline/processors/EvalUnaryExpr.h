#pragma once

#include "columns/ColumnOperator.h"

namespace db {

class Column;

class EvalUnaryExpr {
public:
    template <ColumnOperator Op>
    static void eval(Column* res, const Column* operand);
};

}

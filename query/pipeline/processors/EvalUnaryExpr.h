#pragma once

#include "columns/ColumnOperator.h"

namespace db {

class Column;

/**
 * @brief Indirection to pass a Column* to @ref ColumnSingleDispatcher with the
 * appropriate functor.
 * @detail Maps @ref ColumnOperator enum to UnaryOperator/Predicate functors, and
 * assumes the result column type is the same as the argument type. Constrains the types which are
 * to be passed to the functor using @ref AllowedKinds
 */
class EvalUnaryExpr {
public:
    template <ColumnOperator Op>
    static void eval(Column* res, const Column* operand);
};

}

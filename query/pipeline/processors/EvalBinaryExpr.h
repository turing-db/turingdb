#pragma once

#include "columns/ColumnOperator.h"

namespace db {

class Column;

/**
 * @brief Indirection to pass two Column*s to @ref ColumnDoubleDispatcher with the
 * appropriate functor.
 * @detail Maps @ref ColumnOperator enum to BinaryOperator/Predicate functors, and
 * determines the correct result column based on the operands and operator via @ref
 * ColumnCombinations. Constrains the types which are to be passed to the functor using
 * @ref AllowedKinds
 */
class EvalBinaryExpr {
public:
    template <ColumnOperator Op>
    static void eval(Column* res, const Column* lhs, const Column* rhs);
};

}

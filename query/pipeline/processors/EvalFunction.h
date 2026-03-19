#pragma once

#include "columns/ColumnOperator.h"
#include "views/GraphView.h"

namespace db {

class Column;

/**
 * @brief Indirection to pass two Column*s to @ref ColumnSingleDispatcher with the
 * appropriate functor to evaluate a function with its argument.
 */
class EvalFunction {
public:
    /// Generic 1-argument function evaluation
    template <ColumnOperator Op>
    static void eval(Column* res, const Column* arg);

    /// Specialisation for functions requiring a GraphView (e.g. labels())
    template <ColumnOperator Op>
    static void eval(Column* res, const Column* arg, GraphView view);

    /// Binary function evaluation (e.g. cosine_similarity, euclidean_distance)
    template <ColumnOperator Op>
    static void eval(Column* res, const Column* lhs, const Column* rhs);
};

}

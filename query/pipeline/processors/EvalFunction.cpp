#include "EvalFunction.h"
#include "columns/ColumnOperator.h"

using namespace db;

#include "columns/AllowedKinds.h"
#include "columns/ColumnCombinations.h"
#include "columns/ColumnOperatorDispatcher.h"
#include "columns/ColumnOperators.h"
#include "columns/Functions.h"

#include "Panic.h"

namespace {

template <ColumnOperator Op>
struct UnaryEval {
    Column* _res {nullptr};
    GraphView _view;

    template <typename T>
    void operator()(const T* arg) {
        bioassert(_res && arg, "Null operands to function.");

        if constexpr (Op == OP_FUNC_LABELS) {
            bioassert(_view.isValid(),
                      "Attempted to evaluate labels() with invalid GraphView.");

            using ResultType = FunctionColumnResult<LabelsFunction, T>::ResultColumnType;

            auto* result = dynamic_cast<ResultType*>(_res);
            bioassert(result, "Invalid cast to result column for labels().");

            ColumnFunctions::exec<LabelsFunction>(result, arg, _view);
        } else if constexpr (Op == OP_FUNC_EDGE_TYPES) {
            bioassert(_view.isValid(),
                      "Attempted to evaluate edgeTypes() with invalid GraphView.");

            using ResultType = FunctionColumnResult<EdgeTypesFunction, T>::ResultColumnType;

            auto* result = dynamic_cast<ResultType*>(_res);
            bioassert(result, "Invalid cast to result column for edgeTypes().");

            ColumnFunctions::exec<EdgeTypesFunction>(result, arg, _view);
        } else if constexpr (Op == OP_TO_INTEGER) {
            using Functor = ConversionFunctorFor<toIntegerFunction, ConversionArgument<T>>::Type;
            using ResultType = FunctionColumnResult<Functor, T>::ResultColumnType;

            auto* result = dynamic_cast<ResultType*>(_res);
            bioassert(result, "Invalid cast to result column for toInteger().");

            ColumnFunctions::exec<Functor>(result, arg);
        } else if constexpr (Op == OP_TO_FLOAT) {
            using Functor = ConversionFunctorFor<toFloatFunction, ConversionArgument<T>>::Type;
            using ResultType = FunctionColumnResult<Functor, T>::ResultColumnType;

            auto* result = dynamic_cast<ResultType*>(_res);
            bioassert(result, "Invalid cast to result column for toFloat().");

            ColumnFunctions::exec<Functor>(result, arg);
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

}

template <ColumnOperator Op>
void EvalFunction::eval(Column* res, const Column* arg, GraphView view) {
    using Types = TypeRestrictions<Op>;
    UnaryEval<Op> fn {res, view};
    using Dispatcher = ColumnSingleDispatcher<typename Types::Allowed,
                                              UnaryEval<Op>,
                                              typename Types::Excluded>;
    Dispatcher::dispatch(arg, fn);
}

template <ColumnOperator Op>
void EvalFunction::eval(Column* res, const Column* arg) {
    using Types = TypeRestrictions<Op>;
    UnaryEval<Op> fn {res, {}};
    using Dispatcher = ColumnSingleDispatcher<typename Types::Allowed,
                                              UnaryEval<Op>,
                                              typename Types::Excluded>;
    Dispatcher::dispatch(arg, fn);
}

template void EvalFunction::eval<OP_FUNC_LABELS>(Column* res, const Column* arg, GraphView view);
template void EvalFunction::eval<OP_FUNC_EDGE_TYPES>(Column* res, const Column* arg, GraphView view);

template void EvalFunction::eval<OP_TO_INTEGER>(Column* res, const Column* arg);
template void EvalFunction::eval<OP_TO_FLOAT>(Column* res, const Column* arg);
template void EvalFunction::eval<OP_TO_BOOLEAN>(Column* res, const Column* arg);

namespace {

template <ColumnOperator Op>
struct BinaryEval {
    Column* _res {nullptr};

    template <typename T, typename U>
    void operator()(const T* lhs, const U* rhs) {
        bioassert(_res && lhs && rhs, "Null operands to binary function.");

        if constexpr (Op == OP_FUNC_COSINE_SIMILARITY) {
            using ResultType = typename BinaryFunctionColumnResult<CosineSimilarityFunction, T, U>::ResultColumnType;
            auto* result = dynamic_cast<ResultType*>(_res);
            bioassert(result, "Invalid cast for cosine_similarity result.");
            ColumnFunctions::exec<CosineSimilarity>(result, lhs, rhs);
        } else if constexpr (Op == OP_FUNC_EUCLIDEAN_DISTANCE) {
            using ResultType = typename BinaryFunctionColumnResult<EuclideanDistanceFunction, T, U>::ResultColumnType;
            auto* result = dynamic_cast<ResultType*>(_res);
            bioassert(result, "Invalid cast for euclidean_distance result.");
            ColumnFunctions::exec<EuclideanDistance>(result, lhs, rhs);
        } else {
            COMPILE_ERROR("Invalid binary function.");
        }
    }
};

}

template <ColumnOperator Op>
void EvalFunction::eval(Column* res, const Column* lhs, const Column* rhs) {
    using Pairs = PairRestrictions<Op>;
    BinaryEval<Op> fn {res};
    ColumnDoubleDispatcher<typename Pairs::Allowed,
                           typename Pairs::AllowedMixed,
                           BinaryEval<Op>,
                           typename Pairs::Excluded>::dispatch(lhs, rhs, fn);
}

template void EvalFunction::eval<OP_FUNC_COSINE_SIMILARITY>(Column*, const Column*, const Column*);
template void EvalFunction::eval<OP_FUNC_EUCLIDEAN_DISTANCE>(Column*, const Column*, const Column*);

#pragma once

#include <type_traits>

#include "BinaryOperators.h"
#include "BinaryPredicates.h"
#include "MaskOperators.h"
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

        using DecayColT = TypeUtils::decay_col_t<ColT>;
        using DecayColU = TypeUtils::decay_col_t<ColU>;
        using DecayColW = TypeUtils::decay_col_t<ColW>;

        using InternalT = InnerTypeHelper<DecayColT>::type;
        using InternalU = InnerTypeHelper<DecayColU>::type;
        using InternalRes = InnerTypeHelper<DecayColW>::type;

        BinaryOpExecutor<Op, InternalRes, InternalT, InternalU>::apply(res, lhs, rhs, op);
    }
};

/// Binary predicates, returning a Bool-like
struct BinaryPredicates {
    template <typename Op, typename ColT, typename ColU>
    static void exec(ColumnMask* res, const ColT* lhs, const ColU* rhs) {
        using DecayColT = TypeUtils::decay_col_t<ColT>;
        using DecayColU = TypeUtils::decay_col_t<ColU>;

        using InternalT = InnerTypeHelper<DecayColT>::type;
        using InternalU = InnerTypeHelper<DecayColU>::type;

        BinaryPredicateExecutor<Op, InternalT, InternalU>::apply(res, lhs, rhs);
    }

    template <typename Op, typename ColT, typename ColU>
    static void exec(ColumnOptMask* res, const ColT* lhs, const ColU* rhs) {
        using DecayColT = TypeUtils::decay_col_t<ColT>;
        using DecayColU = TypeUtils::decay_col_t<ColU>;

        using InternalT = InnerTypeHelper<DecayColT>::type;
        using InternalU = InnerTypeHelper<DecayColU>::type;

        BinaryPredicateExecutor<Op, InternalT, InternalU>::apply(res, lhs, rhs);
    }

    /// Specialisations when filtering IDs by literals, e.g. n = 1
    template <typename Op, typename ColT, typename ColU>
    static void exec(ColumnVector<CustomBool>* res, const ColT* lhs, const ColU* rhs) {
        using DecayColT = TypeUtils::decay_col_t<ColT>;
        using DecayColU = TypeUtils::decay_col_t<ColU>;

        using InternalT = InnerTypeHelper<DecayColT>::type;
        using InternalU = InnerTypeHelper<DecayColU>::type;

        BinaryPredicateExecutor<Op, InternalT, InternalU>::apply(res, lhs, rhs);
    }

    /// Const x Const predicates e.g. 1 = 1
    // FIXME: Do we need opt version for null-literals ?
    template <typename Op, typename ColT, typename ColU>
    static void exec(ColumnConst<CustomBool>* res, const ColT* lhs, const ColU* rhs) {
        using DecayColT = TypeUtils::decay_col_t<ColT>;
        using DecayColU = TypeUtils::decay_col_t<ColU>;

        using InternalT = InnerTypeHelper<DecayColT>::type;
        using InternalU = InnerTypeHelper<DecayColU>::type;

        BinaryPredicateExecutor<Op, InternalT, InternalU>::apply(res, lhs, rhs);
    }
};

/// Unary predicates, returning a Bool-like
struct UnaryPredicates {
    template <typename Op, typename ColT>
    static void exec(ColumnMask* res, const ColT* arg) {
        using DecayColT = TypeUtils::decay_col_t<ColT>;
        using InternalT = InnerTypeHelper<DecayColT>::type;

        UnaryPredicateExecutor<Op, InternalT>::apply(res, arg);
    }

    // Below specialisation is needed for expressions like NOT TRUE, NOT FALSE
    // Both Bool and opt<Bool> are needed as there is no way to limit to
    // ColumnConst<{Bool}> whilst also having ColumnVector<{Bool, optional<Bool>}>
    template <typename Op, typename T>
        requires(std::is_same_v<T, types::Bool::Primitive>)
             || (std::is_same_v<T, std::optional<types::Bool::Primitive>>)
    static void exec(ColumnConst<T>* res, const ColumnConst<T>* arg) {
        UnaryPredicateExecutor<Op, T>::apply(res, arg);
    }

    // Below specialisation needed for type checker, but is never used, see above for why.
    // The optional<Bool> case is handled by @ref exec(ColumnOptMask*, ...) because
    // ColOptMask is a weak alias for ColumnVector<opt<Bool>>
    template <typename Op, typename T>
        requires(std::is_same_v<T, types::Bool::Primitive>)
    static void exec(ColumnVector<T>* res, const ColumnVector<T>* arg) {
        UnaryPredicateExecutor<Op, T>::apply(res, arg);
    }

    template <typename Op, typename ColT>
    static void exec(ColumnOptMask* res, const ColT* arg) {
        using DecayColT = TypeUtils::decay_col_t<ColT>;

        using InternalT = InnerTypeHelper<DecayColT>::type;

        UnaryPredicateExecutor<Op, InternalT>::apply(res, arg);
    }
};

struct MaskOperators {
    /// Binary operation on masks
    template <typename Op>
    static void exec(ColumnMask* res, const ColumnMask* lhs, const ColumnMask* rhs) {
        MaskOpExecutor<Op>::apply(res, lhs, rhs);
    }

    /// Unary operation on masks
    template <typename Op>
    static void exec(ColumnMask* res, const ColumnMask* arg) {
        MaskOpExecutor<Op>::apply(res, arg);
    }

    /// Applying masks to vectors
    template <typename Op, typename T>
        requires std::is_same_v<Op, Apply>
    static void exec(ColumnVector<T>* res, const ColumnVector<T>* src, const ColumnMask* mask) {
        MaskApplicator::apply(res, src, mask);
    }

    template <typename Op, typename T>
        requires std::is_same_v<Op, Apply>
    static void exec(ColumnVector<T>* res, const ColumnMask* mask, const ColumnVector<T>* src) {
        MaskApplicator::apply(res, src, mask);
    }
};

struct ColumnFunctions {
    /// labels() function; result type is string
    template <typename Op, typename ColT>
    static void exec(ColumnVector<std::string>* res, const ColT* arg, const GraphView view) {
        using DecayColT = TypeUtils::decay_col_t<ColT>;
        using InternalT = InnerTypeHelper<DecayColT>::type;

        FunctionExecutor<Op, std::string, InternalT>::apply(res, arg, view);
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

/// Generic Column projection and manipulation
struct ColumnOperators {
    template <typename T>
    static void copyChunk(typename ColumnVector<T>::ConstIterator srcStart,
                          typename ColumnVector<T>::ConstIterator srcEnd,
                          ColumnVector<T>* dst) {
        const size_t count = std::distance(srcStart, srcEnd);
        dst->resize(count);
        std::copy(srcStart, srcEnd, dst->begin());
    }

    template <typename T>
    static void copyTransformedChunk(const ColumnVector<size_t>* transform,
                                     const ColumnVector<T>* src,
                                     ColumnVector<T>* dst) {
        const auto& srcd = src->getRaw();
        const auto& transformd = transform->getRaw();
        const size_t count = transform->size();
        dst->resize(count);

        auto& dstd = dst->getRaw();
        for (size_t i = 0; i < count; i++) {
            dstd[i] = srcd[transformd[i]];
        }
    }
};

}

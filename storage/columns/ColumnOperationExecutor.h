#pragma once

#include "AllowedPairs.h"
#include "ColumnVector.h"
#include "ColumnConst.h"
#include "ColumnSet.h"
#include "ColumnMask.h"
#include "PairColumnKind.h"

#include "FatalException.h"
#include "columns/ContainerKind.h"

namespace db {

#define CASE_DOUBLE_DISPATCH(LHS, RHS)                                                   \
    case PairColumnKind::code(ContainerKind::code<LHS>(), ContainerKind::code<RHS>()): { \
        if constexpr (!ExcludedContainers::template contains<LHS>()                      \
                      && !ExcludedContainers::template contains<RHS>()) {                \
            dispatchInternal<LHS, RHS>(lhs, rhs, fn);                                    \
        } else {                                                                         \
            throw FatalException("Unsupported operation");                               \
        }                                                                                \
    } break;

#define CASE_SINGLE_DISPATCH(Operand)                                      \
    case ContainerKind::code<Operand>(): {                                 \
        if constexpr (!ExcludedContainers::template contains<Operand>()) { \
            dispatchInternal<Operand>(operand, fn);                        \
        } else {                                                           \
            throw FatalException("Unsupported operation");                 \
        }                                                                  \
    } break;

template <TupleExact AllowedInternalTypes,
          class Functor,
          class ExcludedContainers = ExcludedContainers<>>
class ColumnSingleDispatcher {

    using Fn = void (*)(const Column*, Functor&);

    static void unsupported(const Column*, Functor&) {
        throw FatalException("Unsupported operation");
    }

public:
    static void dispatch(const Column* operand, Functor& fn) {
        const ContainerKind::Code containerKind = ColumnKind::extractContainerKind(operand->getKind());

        using Dummy = size_t;

        switch (containerKind) {
            CASE_SINGLE_DISPATCH(ColumnVector<Dummy>)
            CASE_SINGLE_DISPATCH(ColumnConst<Dummy>)
            CASE_SINGLE_DISPATCH(ColumnSet<Dummy>)
            CASE_SINGLE_DISPATCH(ColumnMask)

            default: {
                throw FatalException("Unknown container kind");
            }
        }
    }

private:
    template <class OperandRaw>
    static void dispatchInternal(const Column* operand, Functor& fn) {
        using Container = OuterTypeHelper<OperandRaw>::type;

        if constexpr (std::is_same_v<Container, std::false_type>) {
            fn(static_cast<const OperandRaw*>(operand));
        } else {
            const auto i = ColumnKind::extractInternalKind(operand->getKind());

            static constexpr auto table = makeTable<Container::template type>(AllowedInternalTypes {});
            table[i](operand, fn);
        }
    }

    static constexpr std::size_t N = InternalKind::Types::count();

    template <template <typename> class Container, typename T>
    static void invoke(const Column* operand, Functor& fn) {
        fn(static_cast<const Container<T>*>(operand));
    }

    template <template <typename> class Container, typename T>
    static consteval void installFuncs(std::array<Fn, N + 1>& t) {
        constexpr auto i = InternalKind::code<T>();

        if constexpr (ColumnSupportsInternal<Container, T>) {
            t[i] = &invoke<Container, T>;
        }
    }

    template <template <typename> class Container, typename... Types>
    static consteval auto makeTable(std::tuple<Types...>) {
        std::array<Fn, N + 1> t {};

        for (std::size_t i = 0; i <= N; ++i) {
            t[i] = &unsupported;
        }

        (installFuncs<Container, Types>(t), ...);
        return t;
    }
};

template <KindPairListExact AllowedPairs, class AllowedMixed, class Functor, class ExcludedContainers = ExcludedContainers<>>
class ColumnDoubleDispatcher {
    using Fn = void (*)(const Column*, const Column*, Functor&);

    static void unsupported(const Column*, const Column*, Functor&) {
        throw FatalException("Unsupported operation");
    }

public:
    static void dispatch(const Column* lhs, const Column* rhs, Functor& fn) {
        const ContainerKind::Code lkind = ColumnKind::extractContainerKind(lhs->getKind());
        const ContainerKind::Code rkind = ColumnKind::extractContainerKind(rhs->getKind());
        const PairColumnKind::Code pairKind = PairColumnKind::code(lkind, rkind);

        using Dummy = size_t;

        switch (pairKind) {
            // Vector vs X
            CASE_DOUBLE_DISPATCH(ColumnVector<Dummy>, ColumnVector<Dummy>)
            CASE_DOUBLE_DISPATCH(ColumnVector<Dummy>, ColumnConst<Dummy>)
            CASE_DOUBLE_DISPATCH(ColumnVector<Dummy>, ColumnSet<Dummy>)
            CASE_DOUBLE_DISPATCH(ColumnVector<Dummy>, ColumnMask)

            // Const vs X
            CASE_DOUBLE_DISPATCH(ColumnConst<Dummy>, ColumnVector<Dummy>)
            CASE_DOUBLE_DISPATCH(ColumnConst<Dummy>, ColumnConst<Dummy>)
            CASE_DOUBLE_DISPATCH(ColumnConst<Dummy>, ColumnSet<Dummy>)
            CASE_DOUBLE_DISPATCH(ColumnConst<Dummy>, ColumnMask)

            // Set vs X
            CASE_DOUBLE_DISPATCH(ColumnSet<Dummy>, ColumnVector<Dummy>)
            CASE_DOUBLE_DISPATCH(ColumnSet<Dummy>, ColumnConst<Dummy>)
            CASE_DOUBLE_DISPATCH(ColumnSet<Dummy>, ColumnSet<Dummy>)
            CASE_DOUBLE_DISPATCH(ColumnSet<Dummy>, ColumnMask)

            // Mask vs X
            CASE_DOUBLE_DISPATCH(ColumnMask, ColumnVector<Dummy>)
            CASE_DOUBLE_DISPATCH(ColumnMask, ColumnConst<Dummy>)
            CASE_DOUBLE_DISPATCH(ColumnMask, ColumnSet<Dummy>)
            CASE_DOUBLE_DISPATCH(ColumnMask, ColumnMask)

            default: {
                throw FatalException("Unknown pair column kind");
            }
        }
    }

private:
    template <class LhsRaw, class RhsRaw>
    static void dispatchInternal(const Column* lhs, const Column* rhs, Functor& fn) {
        using LhsContainer = OuterTypeHelper<LhsRaw>::type;
        using RhsContainer = OuterTypeHelper<RhsRaw>::type;

        if constexpr (std::is_same_v<LhsContainer, std::false_type>
                      && std::is_same_v<RhsContainer, std::false_type>) {
            fn(static_cast<const LhsRaw*>(lhs), static_cast<const RhsRaw*>(rhs));
        } else if constexpr (std::is_same_v<LhsContainer, std::false_type>) {
            dispatchInternalR<LhsRaw, RhsContainer::template type>(lhs, rhs, fn);
        } else if constexpr (std::is_same_v<RhsContainer, std::false_type>) {
            dispatchInternalL<LhsContainer::template type, RhsRaw>(lhs, rhs, fn);
        } else {
            dispatchInternalLR<LhsContainer::template type, RhsContainer::template type>(lhs, rhs, fn);
        }
    }

    template <class LhsContainer, template <typename> class RhsContainer>
    static void dispatchInternalR(const Column* lhs, const Column* rhs, Functor& fn) {
        const auto ri = ColumnKind::extractInternalKind(rhs->getKind());
        static constexpr auto table = makeTableR<RhsContainer>();
        table[ri](lhs, rhs, fn);
    }

    template <template <typename> class LhsContainer, class RhsContainer>
    static void dispatchInternalL(const Column* lhs, const Column* rhs, Functor& fn) {
        const auto li = ColumnKind::extractInternalKind(lhs->getKind());
        static constexpr auto table = makeTableL<LhsContainer>();
        table[li](lhs, rhs, fn);
    }

    template <template <typename> class LhsContainer, template <typename> class RhsContainer>
    static void dispatchInternalLR(const Column* lhs, const Column* rhs, Functor& fn) {
        const auto li = ColumnKind::extractInternalKind(lhs->getKind());
        const auto ri = ColumnKind::extractInternalKind(rhs->getKind());

        static constexpr auto table = makeTableLR<LhsContainer, RhsContainer>(AllowedPairs {});
        table[li][ri](lhs, rhs, fn);
    }

    template <template <typename> class LhsContainer, typename RhsRaw, typename LK>
    static void invokeL(const Column* lhs, const Column* rhs, Functor& fn) {
        auto* l = static_cast<const LhsContainer<LK>*>(lhs);
        auto* r = static_cast<const RhsRaw*>(rhs);
        fn(l, r);
    }

    template <typename LhsRaw, template <typename> class RhsContainer, typename RK>
    static void invokeR(const Column* lhs, const Column* rhs, Functor& fn) {
        auto* l = static_cast<const LhsRaw*>(lhs);
        auto* r = static_cast<const RhsContainer<RK>*>(rhs);
        fn(l, r);
    }

    static constexpr std::size_t N = InternalKind::Types::count();

    template <template <typename> class LhsContainer, template <typename> class RhsContainer, typename LK, typename RK>
    static void invokeLR(const Column* lhs, const Column* rhs, Functor& fn) {
        auto* l = static_cast<const LhsContainer<LK>*>(lhs);
        auto* r = static_cast<const RhsContainer<RK>*>(rhs);
        fn(l, r);
    }

    template <template <typename> class LhsContainer, typename Mixed>
    static consteval void installMixedL(std::array<Fn, N + 1>& t) {
        using K = typename Mixed::Kind;
        constexpr auto ki = InternalKind::code<K>();

        if constexpr (ColumnSupportsInternal<LhsContainer, K>) {
            t[ki] = &invokeL<LhsContainer, typename Mixed::NonTemplatedType, K>;
        }
    }

    template <template <typename> class RhsContainer, typename Mixed>
    static consteval void installMixedR(std::array<Fn, N + 1>& t) {
        using K = typename Mixed::Kind;
        constexpr auto ki = InternalKind::code<K>();

        if constexpr (ColumnSupportsInternal<RhsContainer, K>) {
            t[ki] = &invokeR<typename Mixed::NonTemplatedType, RhsContainer, K>;
        }
    }

    template <template <typename> class LhsContainer, template <typename> class RhsContainer, typename LK, typename RK>
    static consteval void installLRPair(std::array<std::array<Fn, N + 1>, N + 1>& t) {
        constexpr auto li = InternalKind::code<LK>();
        constexpr auto ri = InternalKind::code<RK>();

        if constexpr (ColumnSupportsInternal<LhsContainer, LK> && ColumnSupportsInternal<RhsContainer, RK>) {
            t[li][ri] = &invokeLR<LhsContainer, RhsContainer, LK, RK>;
        }

        if constexpr (ColumnSupportsInternal<LhsContainer, RK> && ColumnSupportsInternal<RhsContainer, LK>) {
            t[ri][li] = &invokeLR<LhsContainer, RhsContainer, RK, LK>;
        }
    }

    template <template <typename> class LhsContainer>
    static consteval auto makeTableL() {
        return makeTableLImpl<LhsContainer>(AllowedMixed {});
    }

    template <template <typename> class LhsContainer, typename... Mixed>
    static consteval auto makeTableLImpl(AllowedMixedList<Mixed...>) {
        std::array<Fn, N + 1> t {};

        for (std::size_t i = 0; i <= N; ++i) {
            t[i] = &unsupported;
        }

        (installMixedL<LhsContainer, Mixed>(t), ...);
        return t;
    }

    template <template <typename> class RhsContainer>
    static consteval auto makeTableR() {
        return makeTableRImpl<RhsContainer>(AllowedMixed {});
    }

    template <template <typename> class RhsContainer, typename... Mixed>
    static consteval auto makeTableRImpl(AllowedMixedList<Mixed...>) {
        std::array<Fn, N + 1> t {};

        for (std::size_t i = 0; i <= N; ++i) {
            t[i] = &unsupported;
        }

        (installMixedR<RhsContainer, Mixed>(t), ...);
        return t;
    }

    template <template <typename> class LhsContainer, template <typename> class RhsContainer, typename... Pairs>
    static consteval auto makeTableLR(KindPairList<Pairs...>) {
        std::array<std::array<Fn, N + 1>, N + 1> t {};

        for (std::size_t i = 0; i <= N; ++i) {
            for (std::size_t j = 0; j <= N; ++j) {
                t[i][j] = &unsupported;
            }
        }

        (installLRPair<LhsContainer, RhsContainer, typename Pairs::Lhs, typename Pairs::Rhs>(t), ...);
        return t;
    }
};

}

#pragma once

#include <algorithm>
#include <concepts>
#include <functional>
#include <optional>
#include <type_traits>

#include "ColumnConst.h"
#include "ColumnMask.h"
#include "ColumnVector.h"
#include "ColumnOptMask.h"
#include "columns/ColumnSet.h"

#include "metadata/PropertyType.h"
#include "metadata/PropertyNull.h"

#include "BioAssert.h"

namespace db {

template <typename T, typename U>
concept Stringy = (
    (std::same_as<T, std::string_view> && std::same_as<std::string, U>) ||
    (std::same_as<std::string_view, U> && std::same_as<T, std::string>)
);

template <typename T>
struct is_optional : std::false_type {};

template <typename U>
struct is_optional<std::optional<U>> : std::true_type {};

template <typename T>
inline constexpr bool is_optional_v = is_optional<std::remove_cvref_t<T>>::value;

template <typename T>
struct unwrap_optional {
    using underlying_type = T;
};

template <typename U>
struct unwrap_optional<std::optional<U>> {
    using underlying_type = U;
};

template <typename T>
using unwrap_optional_t = typename unwrap_optional<T>::underlying_type;

// Types that may be compared, but one or both may be wrapped in optional
template <typename T, typename U>
concept OptionallyComparable =
    (Stringy<unwrap_optional_t<T>, unwrap_optional_t<U>>
     || std::totally_ordered_with<unwrap_optional_t<T>, unwrap_optional_t<U>>);

// Types that are semantically equivalent, but one or both may be wrapped in optional
template <typename T, typename U>
concept OptionallyEquivalent =
    (Stringy<unwrap_optional_t<T>, unwrap_optional_t<U>>
     || std::same_as<unwrap_optional_t<T>, unwrap_optional_t<U>>);

template <typename T>
concept BooleanOpt = std::same_as<unwrap_optional_t<T>, types::Bool::Primitive>
                  || std::same_as<ColumnMask::Bool_t, T>;

/**
 * @brief This macro instantiates the boilerplate for various combinations of operands
 * that are needed to evaluate predicates in filters via @ref ExprProgram. The
 * instantiation for each operator is identical, so is macro'd instead of explicitly
 * instantiated.
 * @detail These functions allow for filters using optionals as null values.
 * @param functionName The name of the function to instantiate
 * @param operatorFunction The name of the function to be applied to input operands
 */
#define INSTANTIATE_PROPERTY_PREDICATES(functionName, operatorFunction)                  \
    /* Case for producing a mask based on two vector inputs, e.g. n.name = m.name */     \
    template <typename T, typename U>                                                    \
        requires OptionallyComparable<T, U>                                              \
    static void functionName(ColumnOptMask* mask,                                        \
                             const ColumnVector<T>* lhs,                                 \
                             const ColumnVector<U>* rhs) {                               \
        bioassert(lhs->size() == rhs->size(), "Columns must have matching dimensions");  \
        mask->resize(lhs->size());                                                       \
                                                                                         \
        auto& maskd = mask->getRaw();                                                    \
        const auto& lhsd = lhs->getRaw();                                                \
        const auto& rhsd = rhs->getRaw();                                                \
        const auto size = lhs->size();                                                   \
                                                                                         \
        for (size_t i = 0; i < size; i++) {                                              \
            maskd[i] = operatorFunction(lhsd[i], rhsd[i]);                               \
        }                                                                                \
    }                                                                                    \
                                                                                         \
    /* Case for producing a mask based on vector and constant input, e.g. n.age = 10 */  \
    template <typename T, typename U>                                                    \
        requires OptionallyComparable<T, U>                                              \
    static void functionName(ColumnOptMask* mask,                                        \
                             const ColumnVector<T>* lhs,                                 \
                             const ColumnConst<U>* rhs) {                                \
        const auto size = lhs->size();                                                   \
                                                                                         \
        mask->resize(size);                                                              \
        auto& maskd = mask->getRaw();                                                    \
        const auto& lhsd = lhs->getRaw();                                                \
        const auto& val = rhs->getRaw();                                                 \
                                                                                         \
        for (size_t i = 0; i < size; i++) {                                              \
            maskd[i] = operatorFunction(lhsd[i], val);                                   \
        }                                                                                \
    }                                                                                    \
                                                                                         \
    /* Case for producing a mask based on constant and vector input, e.g. 10 = n.age */  \
    template <typename T, typename U>                                                    \
        requires OptionallyComparable<T, U>                                              \
    static void functionName(ColumnOptMask* mask,                                        \
                             const ColumnConst<T>* lhs,                                  \
                             const ColumnVector<U>* rhs) {                               \
        const auto size = rhs->size();                                                   \
                                                                                         \
        mask->resize(size);                                                              \
        auto& maskd = mask->getRaw();                                                    \
        const auto& val = lhs->getRaw();                                                 \
        const auto& rhsd = rhs->getRaw();                                                \
                                                                                         \
        for (size_t i = 0; i < size; i++) {                                              \
            maskd[i] = operatorFunction(val, rhsd[i]);                                   \
        }                                                                                \
    }                                                                                    \
                                                                                         \
    /* Case for producing a mask based on two costant inputs, e.g. 10 = 12 */            \
    template <typename T, typename U>                                                    \
        requires OptionallyComparable<T, U>                                              \
    static void functionName(ColumnOptMask* mask,                                        \
                             const ColumnConst<T>* lhs,                                  \
                             const ColumnConst<U>* rhs) {                                \
        mask->resize(1);                                                                 \
        auto& maskd = mask->getRaw();                                                    \
        maskd.front() = operatorFunction(lhs->getRaw(), rhs->getRaw());                  \
    }

class ColumnOperators {
public:
    // Boolean column operations

    /**
     * @brief Fills a mask corresponding to 'lhs && rhs'
     *
     * @param mask The mask to fill
     * @param lhs Left hand side mask
     * @param rhs Right hand side mask
     */
    static void andOp(ColumnMask* mask,
                      const ColumnMask* lhs,
                      const ColumnMask* rhs) {
        bioassert(lhs->size() == rhs->size(), "Columns must have matching dimensions");
        const auto size = lhs->size();

        mask->resize(size);
        auto& maskd = mask->getRaw();
        const auto& lhsd = lhs->getRaw();
        const auto& rhsd = rhs->getRaw();

        for (size_t i = 0; i < size; i++) {
            maskd[i] = lhsd[i] && rhsd[i];
        }
    }

    /**
     * @brief Fills a mask corresponding to 'lhs || rhs'
     *
     * @param mask The mask to fill
     * @param lhs Left hand side mask
     * @param rhs Right hand side mask
     */
    static void orOp(ColumnMask* mask,
                     const ColumnMask* lhs,
                     const ColumnMask* rhs) {
        bioassert(lhs->size() == rhs->size(), "Columns must have matching dimensions");
        const auto size = lhs->size();

        mask->resize(size);
        auto& maskd = mask->getRaw();
        const auto& lhsd = lhs->getRaw();
        const auto& rhsd = rhs->getRaw();

        for (size_t i = 0; i < size; i++) {
            maskd[i] = lhsd[i] || rhsd[i];
        }
    }

    // Optional-based property predicate column operations
    INSTANTIATE_PROPERTY_PREDICATES(equal, optionalEq)
    INSTANTIATE_PROPERTY_PREDICATES(notEqual, optionalNotEq)
    INSTANTIATE_PROPERTY_PREDICATES(greaterThan, optionalGT)
    INSTANTIATE_PROPERTY_PREDICATES(lessThan, optionalLT)
    INSTANTIATE_PROPERTY_PREDICATES(greaterThanOrEqual, optionalGTE)
    INSTANTIATE_PROPERTY_PREDICATES(lessThanOrEqual, optionalLTE)

    // Implementation for IS NULL
    template <typename T>
        requires is_optional_v<T>
    static void equal(ColumnOptMask* mask,
                             const ColumnVector<T>* lhs,
                             const ColumnConst<PropertyNull>*) {
        const auto size = lhs->size();

        mask->resize(size);
        auto& maskd = mask->getRaw();
        const auto& lhsd = lhs->getRaw();

        for (size_t i = 0; i < size; i++) {
            maskd[i] = lhsd[i] == std::nullopt;
        }
    }

    // Implementation for IS NOT NULL
    template <typename T>
        requires is_optional_v<T>
    static void notEqual(ColumnOptMask* mask,
                             const ColumnVector<T>* lhs,
                             const ColumnConst<PropertyNull>*) {
        const auto size = lhs->size();

        mask->resize(size);
        auto& maskd = mask->getRaw();
        const auto& lhsd = lhs->getRaw();

        for (size_t i = 0; i < size; i++) {
            maskd[i] = lhsd[i] != std::nullopt;
        }
    }

    // 3-valued logic column operations

    /**
     * @brief Fills a mask corresponding to 'lhs && rhs' in 3-valued logic
     * @detail std::nullopt is used as the "unknown" value
     *
     * @param mask The mask to fill
     * @param lhs Left hand side 3-valued column
     * @param rhs Right hand side 3-valued column
     */
    template <BooleanOpt T, BooleanOpt U>
    static void andOp(ColumnOptMask* mask,
                      const ColumnVector<T>* lhs,
                      const ColumnVector<U>* rhs) {
        bioassert(lhs->size() == rhs->size(), "Columns must have matching dimensions");
        const auto size = lhs->size();

        mask->resize(size);
        auto& maskd = mask->getRaw();
        const auto& lhsd = lhs->getRaw();
        const auto& rhsd = rhs->getRaw();

        for (size_t i = 0; i < size; i++) {
            const auto& l = lhsd[i];
            const auto& r = rhsd[i];

            maskd[i] = optionalAnd(l, r);
        }
    }

    static void andOp(ColumnOptMask* mask,
                      const ColumnOptMask* lhs,
                      const ColumnMask* rhs) {
        bioassert(lhs->size() == rhs->size(), "Columns must have matching dimensions");
        const auto size = lhs->size();

        mask->resize(size);
        auto& maskd = mask->getRaw();
        const auto& lhsd = lhs->getRaw();
        const auto& rhsd = rhs->getRaw();

        for (size_t i = 0; i < size; i++) {
            const auto& l = lhsd[i];
            const auto& r = rhsd[i];

            maskd[i] = optionalAnd(l, r);
        }
    }

    static void andOp(ColumnOptMask* mask,
                      const ColumnMask* lhs,
                      const ColumnOptMask* rhs) {
        bioassert(lhs->size() == rhs->size(), "Columns must have matching dimensions");
        const auto size = lhs->size();

        mask->resize(size);
        auto& maskd = mask->getRaw();
        const auto& lhsd = lhs->getRaw();
        const auto& rhsd = rhs->getRaw();

        for (size_t i = 0; i < size; i++) {
            const auto& l = lhsd[i];
            const auto& r = rhsd[i];

            maskd[i] = optionalAnd(l, r);
        }
    }

    /**
     * @brief Fills a mask corresponding to 'lhs || rhs' in 3-valued logic
     * @detail std::nullopt is used as the "unknown" value
     *
     * @param mask The mask to fill
     * @param lhs Left hand side 3-valued column
     * @param rhs Right hand side 3-valued column
     */
    template <BooleanOpt T, BooleanOpt U>
    static void orOp(ColumnOptMask* mask,
                     const ColumnVector<T>* lhs,
                     const ColumnVector<U>* rhs) {
        bioassert(lhs->size() == rhs->size(), "Columns must have matching dimensions");
        const size_t size = lhs->size();

        mask->resize(size);
        auto& maskd = mask->getRaw();
        const auto& lhsd = lhs->getRaw();
        const auto& rhsd = rhs->getRaw();

        for (size_t i = 0; i < size; i++) {
            maskd[i] = optionalOr(lhsd[i], rhsd[i]);
        }
    }

    /**
     * @brief Fills a mask corresponding to 'NOT input' in 3-valued logic
     * @detail std::nullopt is used as the "unknown" value
     *
     * @param mask The mask to fill
     * @param input Input 3-valued column
     */
    template <BooleanOpt T>
    static void notOp(ColumnOptMask* mask,
                      const ColumnVector<T>* input) {
        const size_t size = input->size();
        mask->resize(size);

        auto& maskd = mask->getRaw();
        const auto& ind = input->getRaw();

        for (size_t i = 0; i < size; i++) {
            maskd[i] = optionalNot(ind[i]);
        }
    }

    /**
     * @brief Fills a mask corresponding to mask[i] = lhs[i] \in rhs
     *
     * @param mask The mask to fill
     * @param lhs Vector of possible candidates
     * @param rhs Lookup set
     */
    template <typename T, typename U>
        requires OptionallyEquivalent<T, U> && (!is_optional_v<U>)
    static void inOp(ColumnMask* mask,
                     const ColumnVector<T>* lhs,
                     const ColumnSet<U>* rhs) {
        const auto size = lhs->size();

        mask->resize(size);
        auto& maskd = mask->getRaw();
        const auto& lhsd = lhs->getRaw();

        for (size_t i = 0; i < size; i++) {
            const T& val = lhsd[i];

            if constexpr (is_optional_v<T>) {
                if (!val.has_value()) {
                    maskd[i] = false;
                    continue;
                }
            }

            // @ref val is either engaged optional or plain value

            auto&& v = unwrap(val);
            maskd[i] = rhs->contains(v);
        }
    }

    // Projection column operations

    static void projectOp(ColumnMask* mask,
                          const ColumnVector<size_t>* lhs,
                          const ColumnMask* rhs) {
        bioassert(lhs->size() == rhs->size(), "Columns must have matching dimensions");
        const auto size = lhs->size();

        auto* maskd = mask->data();
        const auto* lhsd = lhs->data();
        const auto* rhsd = rhs->data();

        for (size_t i = 0; i < size; i++) {
            maskd[lhsd[i]]._value = rhsd[i];
        }
    }

    static void projectOp(ColumnMask* mask,
                          const ColumnMask* lhs,
                          const ColumnVector<size_t>* rhs) {
        bioassert(rhs->size() == lhs->size(), "Columns must have matching dimensions");
        const auto size = rhs->size();

        auto* maskd = mask->data();
        const auto* rhsd = rhs->data();
        const auto* lhsd = lhs->data();
        for (size_t i = 0; i < size; i++) {
            maskd[rhsd[i]]._value = lhsd[i];
        }
    }

    // Materialise/transform column operations

    template <typename T>
    static void copyChunk(ColumnVector<T>::ConstIterator srcStart,
                          ColumnVector<T>::ConstIterator srcEnd,
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

    // Mask application column operations

    template <typename T>
    static void applyMask(const ColumnVector<T>* src, 
                          const ColumnMask* mask,
                          ColumnVector<T>* dest) {
        bioassert(src->size() == mask->size(), "src and mask must have same size");
        const size_t size = src->size();

        dest->clear();
        dest->reserve(size);

        const auto* srcd = src->data();
        const auto* maskd = mask->data();
        for (size_t i = 0; i < size; i++) {
            if (maskd[i]) {
                dest->push_back(srcd[i]);
            }
        }
    }

private:
    /**
     * @brief Partial function which returns the underlying value of an  optional, and
     * is otherwise the identity function. Undefined for nullopt input.
     * @warn Assumes the optional is engaged, does not check for engagement.
     */
    template <typename T>
    static constexpr decltype(auto) unwrap(T&& t) {
        if constexpr (is_optional_v<T>) {
            return *std::forward<T>(t);
        } else {
            return std::forward<T>(t);
        }
    }

    // Implementations of basic operations in Kleene/3-valued logic

    // The following Boolean operators have unique semantics for 3-way logic (i.e.
    // short-circuiting) so are defined explicitly rather than generically
    template <BooleanOpt T, BooleanOpt U>
    static std::optional<bool> optionalOr(const T& a, const U& b) {
        if (a == CustomBool {true} || b == CustomBool {true}) {
            return true;
        }
        if (a == CustomBool {false} && b == CustomBool {false}) {
            return false;
        }
        return std::nullopt;
    }

    template <BooleanOpt T, BooleanOpt U>
    static std::optional<bool> optionalAnd(const T& a, const U& b) {
        if (a == CustomBool {true} && b == CustomBool {true}) {
            return true;
        }
        if (a == CustomBool {false} || b == CustomBool {false}) {
            return false;
        }
        return std::nullopt;
    }

    template <BooleanOpt T>
    static std::optional<bool> optionalNot(const T& a) {
        if (a == CustomBool {true}) {
            return false;
        }
        if (a == CustomBool{false}) {
            return true;
        }
        return std::nullopt;
    }

    /**
     * @brief Generic function to apply a predicate to two possibly-optional operands,
     * where either operand being nullopt results in the final result being nullopt, and the
     * result of applying the operator otherwise.
     */
    template <typename Pred, typename T, typename U>
        requires OptionallyComparable<T, U>
              && std::predicate<Pred, unwrap_optional_t<T>, unwrap_optional_t<U>>
    inline static std::optional<bool> optionalPredicate(const T& a, const U& b) {
        if constexpr (is_optional_v<T>) {
            if (!a.has_value()) {
                return std::nullopt;
            }
        }

        if constexpr (is_optional_v<U>) {
            if (!b.has_value()) {
                return std::nullopt;
            }
        }

        // a and b are both either engaged optionals or values, so safe to unwrap

        auto&& av = unwrap(a);
        auto&& bv = unwrap(b);

        return Pred {}(av, bv);
    }

    template <typename T, typename U>
        requires OptionallyComparable<T, U>
    inline static std::optional<bool> optionalEq(const T& a, const U& b) {
        return optionalPredicate<std::equal_to<>>(a, b);
    }

    template <typename T, typename U>
        requires OptionallyComparable<T, U>
    inline static std::optional<bool> optionalNotEq(const T& a, const U& b) {
        return optionalPredicate<std::not_equal_to<>>(a, b);
    }

    template <typename T, typename U>
        requires OptionallyComparable<T, U>
    inline static std::optional<bool> optionalGT(const T& a, const U& b) {
        return optionalPredicate<std::greater<>>(a, b);
    }

    template <typename T, typename U>
        requires OptionallyComparable<T, U>
    inline static std::optional<bool> optionalLT(const T& a, const U& b) {
        return optionalPredicate<std::less<>>(a, b);
    }

    template <typename T, typename U>
        requires OptionallyComparable<T, U>
    inline static std::optional<bool> optionalGTE(const T& a, const U& b) {
        return optionalPredicate<std::greater_equal<>>(a, b);
    }

    template <typename T, typename U>
        requires OptionallyComparable<T, U>
    inline static std::optional<bool> optionalLTE(const T& a, const U& b) {
        return optionalPredicate<std::less_equal<>>(a, b);
    }

};

}

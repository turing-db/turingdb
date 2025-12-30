#pragma once

#include <concepts>
#include <optional>
#include <type_traits>

#include "ColumnConst.h"
#include "ColumnMask.h"
#include "ColumnVector.h"
#include "ColumnOptMask.h"
#include "columns/ColumnSet.h"

#include "BioAssert.h"
#include "metadata/PropertyType.h"

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
inline constexpr bool is_optional_v = is_optional<T>::value;

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

template <typename T, typename U>
concept OptionallyComparable =
    (Stringy<unwrap_optional_t<T>, unwrap_optional_t<U>>
     || std::same_as<unwrap_optional_t<T>, unwrap_optional_t<U>>);

template <typename T>
concept BooleanOpt = std::same_as<unwrap_optional_t<T>, types::Bool::Primitive>;

class ColumnOperators {
public:
    /**
     * @brief Fills a mask corresponding to 'lhs == rhs'
     *
     * @param mask The mask to fill
     * @param lhs Right hand side column
     * @param lhs Light hand side column
     */
    template <typename T, typename U>
        requires(Stringy<T, U> || std::same_as<T, U>)
    static void equal(ColumnMask* mask,
                      const ColumnVector<T>* lhs,
                      const ColumnVector<U>* rhs) {
        bioassert(lhs->size() == rhs->size(), "Columns must have matching dimensions");
        mask->resize(lhs->size());
        auto& maskd = mask->getRaw();
        const auto& lhsd = lhs->getRaw();
        const auto& rhsd = rhs->getRaw();
        const auto size = lhs->size();

        for (size_t i = 0; i < size; i++) {
            maskd[i]._value = lhsd[i] == rhsd[i];
        }
    }

    template <typename T, typename U>
        requires OptionallyComparable<T, U>
    static void equal(ColumnOptMask* mask,
                      const ColumnVector<T>* lhs,
                      const ColumnVector<U>* rhs) {
        bioassert(lhs->size() == rhs->size(), "Columns must have matching dimensions");
        mask->resize(lhs->size());

        auto& maskd = mask->getRaw();
        const auto& lhsd = lhs->getRaw();
        const auto& rhsd = rhs->getRaw();
        const auto size = lhs->size();

        for (size_t i = 0; i < size; i++) {
            maskd[i] = optionalEq(lhsd[i], rhsd[i]);
        }
    }

    /**
     * @brief Fills a mask corresponding to 'lhs == rhs'
     *
     * @param mask The mask to fill
     * @param lhs Left hand side column
     * @param rhs Constant value to compare against
     */
    template <typename T, typename U>
        requires(Stringy<T, U> || std::same_as<T, U>)
    static void equal(ColumnMask* mask,
                      const ColumnVector<T>* lhs,
                      const ColumnConst<U>* rhs) {
        mask->resize(lhs->size());
        auto& maskd = mask->getRaw();
        const auto& lhsd = lhs->getRaw();
        const auto& rhsd = rhs->getRaw();
        const auto size = lhs->size();

        for (size_t i = 0; i < size; i++) {
            maskd[i]._value = lhsd[i] == rhsd;
        }
    }

    template <typename T, typename U>
        requires OptionallyComparable<T, U>
    static void equal(ColumnOptMask* mask,
                      const ColumnVector<T>* lhs,
                      const ColumnConst<U>* rhs) {
        mask->resize(lhs->size());
        auto& maskd = mask->getRaw();
        const auto& lhsd = lhs->getRaw();
        const auto& val = rhs->getRaw();
        const auto size = lhs->size();

        for (size_t i = 0; i < size; i++) {
            maskd[i] = optionalEq(lhsd[i], val);
        }
    }

    /**
     * @brief Fills a mask corresponding to 'lhs == rhs'
     *
     * @param mask The mask to fill
     * @param lhs Constant value to compare against
     * @param rhs Right hand side column
     */
    template <typename T, typename U>
        requires(Stringy<T, U> || std::same_as<T, U>)
    static void equal(ColumnMask* mask,
                      const ColumnConst<T>* lhs,
                      const ColumnVector<U>* rhs) {
        mask->resize(rhs->size());
        auto& maskd = mask->getRaw();
        const auto& rhsd = rhs->getRaw();
        const T& val = lhs->getRaw();
        const auto size = rhs->size();

        for (size_t i = 0; i < size; i++) {
            maskd[i] = val == rhsd[i];
        }
    }

    template <typename T, typename U>
        requires OptionallyComparable<T, U>
    static void equal(ColumnOptMask* mask,
                      const ColumnConst<T>* lhs,
                      const ColumnVector<U>* rhs) {
        mask->resize(lhs->size());
        auto& maskd = mask->getRaw();
        const auto& val = lhs->getRaw();
        const auto& rhsd = rhs->getRaw();
        const auto size = lhs->size();

        for (size_t i = 0; i < size; i++) {
            maskd[i] = optionalEq(val, rhsd[i]);
        }
    }

    template <typename T, typename U>
        requires OptionallyComparable<T, U>
    static void equal(ColumnOptMask* mask,
                      const ColumnConst<T>* lhs,
                      const ColumnConst<U>* rhs) {
        mask->resize(1);
        auto& maskd = mask->getRaw();
        maskd.front() = optionalEq(lhs->getRaw(), rhs->getRaw());
    }

    /**
     * @brief Fills a mask corresponding to 'lhs == rhs'
     *
     * @param mask The mask to fill
     * @param lhs Constant value
     * @param rhs Constant value
     */
    template <typename T, typename U>
        requires(Stringy<T, U> || std::same_as<T, U>)
    static void equal(ColumnMask* mask,
                      const ColumnConst<T>* lhs,
                      const ColumnConst<U>* rhs) {
        mask->resize(1);
        auto& maskd = mask->getRaw();
        maskd[0] = (lhs->getRaw() == rhs->getRaw());
    }

    template <typename T, typename U>
        requires OptionallyComparable<T, U>
    static void greaterThan(ColumnOptMask* mask,
                            const ColumnVector<T>* lhs,
                            const ColumnVector<U>* rhs) {
        bioassert(lhs->size() == rhs->size(), "Columns must have matching dimensions");
        mask->resize(lhs->size());

        auto& maskd = mask->getRaw();
        const auto& lhsd = lhs->getRaw();
        const auto& rhsd = rhs->getRaw();
        const auto size = lhs->size();

        for (size_t i = 0; i < size; i++) {
            maskd[i] = optionalGT(lhsd[i], rhsd[i]);
        }
    }

    template <typename T, typename U>
        requires OptionallyComparable<T, U>
    static void greaterThan(ColumnOptMask* mask,
                      const ColumnVector<T>* lhs,
                      const ColumnConst<U>* rhs) {
        mask->resize(lhs->size());
        auto& maskd = mask->getRaw();
        const auto& lhsd = lhs->getRaw();
        const auto& val = rhs->getRaw();
        const auto size = lhs->size();

        for (size_t i = 0; i < size; i++) {
            maskd[i] = optionalGT(lhsd[i], val);
        }
    }

    template <typename T, typename U>
        requires OptionallyComparable<T, U>
    static void greaterThan(ColumnOptMask* mask,
                      const ColumnConst<T>* lhs,
                      const ColumnVector<U>* rhs) {
        mask->resize(lhs->size());
        auto& maskd = mask->getRaw();
        const auto& val = lhs->getRaw();
        const auto& rhsd = rhs->getRaw();
        const auto size = lhs->size();

        for (size_t i = 0; i < size; i++) {
            maskd[i] = optionalGT(val, rhsd[i]);
        }
    }

    template <typename T, typename U>
        requires OptionallyComparable<T, U>
    static void greaterThan(ColumnOptMask* mask,
                      const ColumnConst<T>* lhs,
                      const ColumnConst<U>* rhs) {
        mask->resize(1);
        auto& maskd = mask->getRaw();
        maskd.front() = optionalGT(lhs->getRaw(), rhs->getRaw());
    }

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

        mask->resize(lhs->size());
        auto& maskd = mask->getRaw();
        const auto& lhsd = lhs->getRaw();
        const auto& rhsd = rhs->getRaw();
        const auto size = rhs->size();

        for (size_t i = 0; i < size; i++) {
            maskd[i] = lhsd[i] && rhsd[i];
        }
    }

    /**
     * @brief Fills a mask corresponding to 'lhs && rhs', but where the result is
     * std::nullopt if either operand is std::nullopt.
     *
     * @param mask The mask to fill
     * @param lhs Left hand side Column(Opt)Vector
     * @param rhs Right hand side Column(Opt)Vector
     */
    template <BooleanOpt T, BooleanOpt U>
    static void andOp(ColumnOptMask* mask,
                      const ColumnVector<T>* lhs,
                      const ColumnVector<U>* rhs) {
        bioassert(lhs->size() == rhs->size(), "Columns must have matching dimensions");

        mask->resize(lhs->size());
        auto& maskd = mask->getRaw();
        const auto& lhsd = lhs->getRaw();
        const auto& rhsd = rhs->getRaw();
        const auto size = rhs->size();

        for (size_t i = 0; i < size; i++) {
            const auto& l = lhsd[i];
            const auto& r = rhsd[i];

            maskd[i] = optionalAnd(l, r);
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

        mask->resize(lhs->size());
        auto& maskd = mask->getRaw();
        const auto& lhsd = lhs->getRaw();
        const auto& rhsd = rhs->getRaw();
        const auto size = rhs->size();

        for (size_t i = 0; i < size; i++) {
            maskd[i] = lhsd[i] || rhsd[i];
        }
    }

    /**
     * @brief Fills a mask corresponding to 'lhs || rhs'
     *
     * @param mask The mask to fill
     * @param lhs Left hand side Boolean column
     * @param rhs Right hand side Boolean column
     */
    template <BooleanOpt T, BooleanOpt U>
    static void orOp(ColumnOptMask* mask,
                     const ColumnVector<T>* lhs,
                     const ColumnVector<U>* rhs) {
        bioassert(lhs->size() == rhs->size(), "Columns must have matching dimensions");

        mask->resize(lhs->size());
        auto& maskd = mask->getRaw();
        const auto& lhsd = lhs->getRaw();
        const auto& rhsd = rhs->getRaw();
        const size_t size = rhs->size();

        for (size_t i = 0; i < size; i++) {
            maskd[i] = optionalOr(lhsd[i], rhsd[i]);
        }
    }

    /**
     * @brief Fills a mask corresponding to 'NOT input'
     *
     * @param mask The mask to fill
     * @param input Operand column of Boolean negation
     */
    template <BooleanOpt T>
    static void notOp(ColumnMask* mask,
                      const ColumnVector<T>* input) {
        const size_t size = input->size();
        mask->resize(size);

        auto& maskd = mask->getRaw();
        const auto& ind = input->getRaw();

        for (size_t i = 0; i < size; i++) {
            if constexpr (is_optional_v<T>) {
                maskd[i] = ind[i].has_value() && !*ind[i];
            } else {
                maskd[i] = !ind[i];
            }
        }
    }

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
        requires OptionallyComparable<T, U> && (!is_optional_v<U>)
    static void inOp(ColumnMask* mask,
                     const ColumnVector<T>* lhs,
                     const ColumnSet<U>* rhs) {
        mask->resize(lhs->size());
        auto& maskd = mask->getRaw();
        const auto& lhsd = lhs->getRaw();
        const auto size = lhs->size();

        for (size_t i = 0; i < size; i++) {
            const T& val = lhsd[i];

            if constexpr (is_optional_v<T>) {
                if (!val.has_value()) {
                    maskd[i] = false;
                } else {
                    maskd[i] = rhs->contains(val);
                }
            } else {
                maskd[i] = rhs->contains(val);
            }
        }
    }

    static void projectOp(ColumnMask* mask,
                          const ColumnVector<size_t>* lhs,
                          const ColumnMask* rhs) {
        bioassert(lhs->size() == rhs->size(), "Columns must have matching dimensions");

        auto* maskd = mask->data();
        const auto* lhsd = lhs->data();
        const auto* rhsd = rhs->data();
        const auto size = lhs->size();
        for (size_t i = 0; i < size; i++) {
            maskd[lhsd[i]]._value = rhsd[i];
        }
    }

    static void projectOp(ColumnMask* mask,
                          const ColumnMask* lhs,
                          const ColumnVector<size_t>* rhs) {
        bioassert(rhs->size() == lhs->size(), "Columns must have matching dimensions");

        auto* maskd = mask->data();
        const auto* rhsd = rhs->data();
        const auto* lhsd = lhs->data();
        const auto size = rhs->size();
        for (size_t i = 0; i < size; i++) {
            maskd[rhsd[i]]._value = lhsd[i];
        }
    }

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
        const auto* srcd = src->data();
        const auto* transformd = transform->data();
        const size_t count = transform->size();
        dst->resize(count);

        auto* dstd = dst->data();
        for (size_t i = 0; i < count; i++) {
            dstd[i] = srcd[transformd[i]];
        }
    }

    template <typename T>
    static void applyMask(const ColumnVector<T>* src, 
                          const ColumnMask* mask,
                          ColumnVector<T>* dest) {
        bioassert(src->size() == mask->size(), "src and mask must have same size");

        dest->clear();
        dest->reserve(mask->size());

        const auto* srcd = src->data();
        const auto* maskd = mask->data();
        const size_t size = mask->size();
        for (size_t i = 0; i < size; i++) {
            if (maskd[i]) {
                dest->push_back(srcd[i]);
            }
        }
    }

private:
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

    template <typename T, typename U>
        requires OptionallyComparable<T, U>
    static std::optional<bool> optionalEq(const T& a, const U& b) {
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

        // From herein, a and b are either engaged optionals or values

        if constexpr (is_optional_v<T> && is_optional_v<U>) {
            return *a == *b;
        } else if constexpr (is_optional_v<T>) {
            return *a == b;
        } else if constexpr (is_optional_v<U>) {
            return a == *b;
        } else {
            return a == b;
        }
    }

    template <typename T, typename U>
        requires OptionallyComparable<T, U>
    static std::optional<bool> optionalGT(const T& a, const U& b) {
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

        // From herein, a and b are either engaged optionals or values

        if constexpr (is_optional_v<T> && is_optional_v<U>) {
            return *a > *b;
        } else if constexpr (is_optional_v<T>) {
            return *a > b;
        } else if constexpr (is_optional_v<U>) {
            return a > *b;
        } else {
            return a > b;
        }
    }

};

}

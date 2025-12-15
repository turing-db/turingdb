#pragma once

#include <concepts>
#include <optional>
#include <type_traits>

#include "ColumnConst.h"
#include "ColumnMask.h"
#include "ColumnVector.h"
#include "columns/ColumnSet.h"

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
        requires OptionallyComparable<T, U>
    static void equal(ColumnMask& mask,
                      const ColumnVector<T>& lhs,
                      const ColumnVector<U>& rhs) {
        bioassert(lhs.size() == rhs.size(),
                     "Columns must have matching dimensions");
        mask.resize(lhs.size());
        auto& maskd = mask.getRaw();
        const auto& lhsd = lhs.getRaw();
        const auto& rhsd = rhs.getRaw();
        const auto size = lhs.size();

        for (size_t i = 0; i < size; i++) {
            if constexpr (is_optional_v<T>) {
                if (!lhsd[i]) {
                    maskd[i]._value = false;
                    continue;
                }
            }
            if constexpr (is_optional_v<U>) {
                if (!rhsd[i]) {
                    maskd[i]._value = false;
                    continue;
                }
            }
            maskd[i]._value = lhsd[i] == rhsd[i];
        }
    }

    /**
     * @brief Fills a mask corresponding to 'lhs == rhs'
     *
     * @param mask The mask to fill
     * @param lhs Right hand side column
     * @param lhs Light hand side column
     */
    template <typename T, typename U>
        requires OptionallyComparable<T, U>
    static void equal(ColumnMask& mask,
                      const ColumnVector<T>& lhs,
                      const ColumnConst<U>& rhs) {
        mask.resize(lhs.size());
        auto& maskd = mask.getRaw();
        const auto& lhsd = lhs.getRaw();
        const auto& rhsd = rhs.getRaw();
        const auto size = lhs.size();

        for (size_t i = 0; i < size; i++) {
            if constexpr (is_optional_v<T>) {
                if (!lhsd[i]) {
                    maskd[i]._value = false;
                    continue;
                }
            }
            maskd[i]._value = lhsd[i] == rhsd;
        }
    }

    /**
     * @brief Fills a mask corresponding to 'lhs == rhs'
     *
     * @param mask The mask to fill
     * @param lhs Right hand side column
     * @param lhs Light hand side column
     */
    template <typename T, typename U>
        requires OptionallyComparable<T, U>
    static void equal(ColumnMask& mask,
                      const ColumnConst<T>& lhs,
                      const ColumnVector<U>& rhs) {
        mask.resize(rhs.size());
        auto& maskd = mask.getRaw();
        const auto& rhsd = rhs.getRaw();
        const T& val = lhs.getRaw();
        const auto size = rhs.size();

        for (size_t i = 0; i < size; i++) {
            if constexpr (is_optional_v<U>) {
                if (!rhsd[i]) {
                    maskd[i] = false;
                    continue;
                }
            }
            maskd[i] = val == rhsd[i];
        }
    }

    /**
     * @brief Fills a mask corresponding to 'lhs == rhs'
     *
     * @param mask The mask to fill
     * @param lhs Right hand side column
     * @param lhs Light hand side column
     */
    template <typename T, typename U>
        requires(Stringy<T, U> || std::same_as<T, U>)
    static void equal(ColumnMask* mask, const ColumnConst<T>* lhs,
                      const ColumnConst<U>* rhs) {
        mask->resize(1);
        auto* maskd = mask->data();
        maskd[0] = (lhs->getRaw() == rhs->getRaw());
    }

    /**
     * @brief Fills a mask corresponding to 'lhs && rhs'
     *
     * @param mask The mask to fill
     * @param lhs Right hand side column
     * @param lhs Light hand side column
     */
    static void andOp(ColumnMask* mask,
                      const ColumnMask* lhs,
                      const ColumnMask* rhs) {
        bioassert(lhs->size() == rhs->size(),
                     "Columns must have matching dimensions");
        mask->resize(lhs->size());
        auto* maskd = mask->data();
        const auto* lhsd = lhs->data();
        const auto* rhsd = rhs->data();
        const auto size = rhs->size();
        for (size_t i = 0; i < size; i++) {
            maskd[i]._value = lhsd[i]._value && rhsd[i]._value;
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
        bioassert(lhs->size() == rhs->size(),
                     "Columns must have matching dimensions");
        mask->resize(lhs->size());
        auto* maskd = mask->data();
        const auto* lhsd = lhs->data();
        const auto* rhsd = rhs->data();
        const auto size = rhs->size();
        for (size_t i = 0; i < size; i++) {
            maskd[i]._value = lhsd[i]._value || rhsd[i]._value;
        }
    }

    /**
     * @brief Fills a mask corresponding to 'lhs || rhs'
     *
     * @param mask The mask to fill
     * @param lhs Left hand side Boolean column
     * @param rhs Right hand side Boolean column
     */
    template <typename BoolT>
        requires requires(BoolT a, BoolT b) { a || b; }
    static void orOp(ColumnMask* mask,
                     const ColumnVector<BoolT>* lhs,
                     const ColumnVector<BoolT>* rhs) {
        bioassert(lhs->size() == rhs->size(),
                     "Columns must have matching dimensions");
        mask->resize(lhs->size());
        auto* maskd = mask->data();
        const auto* lhsd = lhs->data();
        const auto* rhsd = rhs->data();
        const size_t size = rhs->size();
        for (size_t i = 0; i < size; i++) {
            maskd[i]._value = lhsd[i] || rhsd[i];
        }
    }

    /**
     * @brief Fills a mask corresponding to 'NOT input'
     *
     * @param mask The mask to fill
     * @param input Operand of Boolean negation
     */
     // TODO: Check with optional
    template <typename BoolT>
        requires requires(BoolT x) { !x; }
    static void notOp(ColumnMask* mask,
                      const ColumnVector<BoolT>* input) {
        auto& maskd = mask->getRaw();
        const auto& ind = input->getRaw();
        const size_t size = input->size();
        mask->resize(size);

        for (size_t i = 0; i < size; i++) {
            maskd[i]._value = !ind[i];
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
    static void inOp(ColumnMask& mask,
                     const ColumnVector<T>& lhs,
                     const ColumnSet<U>& rhs) {
        mask.resize(lhs.size());
        auto& maskd = mask.getRaw();
        const auto size = lhs.size();

        for (size_t i = 0; i < size; i++) {
            if constexpr (is_optional_v<T>) {
                if (!lhs[i]) {
                    maskd[i] = false;
                } else {
                    maskd[i] = rhs.contains(*lhs[i]);
                }
            } else {
                maskd[i] = rhs.contains(lhs[i]);
            }
        }
    }

    static void projectOp(ColumnMask* mask,
                          const ColumnVector<size_t>* lhs,
                          const ColumnMask* rhs) {
        bioassert(lhs->size() == rhs->size(),
                     "Columns must have matching dimensions");
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
        bioassert(rhs->size() == lhs->size(),
                     "Columns must have matching dimensions");
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
};
}

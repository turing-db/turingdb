#pragma once

#include "BioAssert.h"
#include "ColumnConst.h"
#include "ColumnMask.h"
#include "ColumnVector.h"
#include "columns/ColumnSet.h"

namespace db {

template <typename T, typename U>
concept Stringy = (
    (std::same_as<T, std::string_view> && std::same_as<std::string, U>) ||
    (std::same_as<std::string_view, U> && std::same_as<T, std::string>)
);

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
    requires (Stringy<T,U> || std::same_as<T,U>)
    static void equal(ColumnMask& mask,
                      const ColumnVector<T>& lhs,
                      const ColumnVector<U>& rhs) {
        msgbioassert(lhs.size() == rhs.size(),
                     "Columns must have matching dimensions");
        mask.resize(lhs.size());
        auto* maskd = mask.data();
        const auto* lhsd = lhs.data();
        const auto* rhsd = rhs.data();
        const auto size = lhs.size();
        for (size_t i = 0; i < size; i++) {
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
    requires (Stringy<T,U> || std::same_as<T,U>)
    static void equal(ColumnMask& mask,
                      const ColumnVector<T>& lhs,
                      const ColumnConst<U>& rhs) {
        mask.resize(lhs.size());
        auto* maskd = mask.data();
        const auto* lhsd = lhs.data();
        const auto& rhsd = rhs.getRaw();
        const auto size = lhs.size();
        for (size_t i = 0; i < size; i++) {
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
    requires (Stringy<T,U> || std::same_as<T,U>)
    static void equal(ColumnMask& mask,
                      const ColumnConst<T>& lhs,
                      const ColumnVector<U>& rhs) {
        mask.resize(rhs.size());
        auto* maskd = mask.data();
        const auto& lhsd = lhs.getRaw();
        const auto* rhsd = rhs.data();
        const auto size = rhs.size();
        for (size_t i = 0; i < size; i++) {
            maskd[i]._value = lhsd == rhsd[i];
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
    requires (Stringy<T,U> || std::same_as<T,U>)
    static void equal(ColumnMask& mask,
                      const ColumnConst<T>& lhs,
                      const ColumnConst<U>& rhs) {
        mask.resize(1);
        mask[0] = lhs.getRaw() == rhs.getRaw();
    }

    /**
     * @brief Fills a mask corresponding to 'lhs && rhs'
     *
     * @param mask The mask to fill
     * @param lhs Right hand side column
     * @param lhs Light hand side column
     */
    static void andOp(ColumnMask& mask,
                       const ColumnMask& lhs,
                       const ColumnMask& rhs) {
        msgbioassert(lhs.size() == rhs.size(),
                     "Columns must have matching dimensions");
        mask.resize(lhs.size());
        auto* maskd = mask.data();
        const auto* lhsd = lhs.data();
        const auto* rhsd = rhs.data();
        const auto size = rhs.size();
        for (size_t i = 0; i < size; i++) {
            maskd[i]._value = lhsd[i]._value && rhsd[i]._value;
        }
    }

    /**
     * @brief Fills a mask corresponding to 'lhs || rhs'
     *
     * @param mask The mask to fill
     * @param lhs Right hand side column
     * @param lhs Light hand side column
     */
    static void orOp(ColumnMask& mask,
                      const ColumnMask& lhs,
                      const ColumnMask& rhs) {
        msgbioassert(lhs.size() == rhs.size(),
                     "Columns must have matching dimensions");
        mask.resize(lhs.size());
        auto* maskd = mask.data();
        const auto* lhsd = lhs.data();
        const auto* rhsd = rhs.data();
        const auto size = rhs.size();
        for (size_t i = 0; i < size; i++) {
            maskd[i]._value = lhsd[i]._value || rhsd[i]._value;
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
    requires (Stringy<T,U> || std::same_as<T,U>)
    static void inOp(ColumnMask& mask,
                     const ColumnVector<T>& lhs,
                     const ColumnSet<U>& rhs) {
        mask.resize(lhs.size());
        auto* maskd = mask.data();
        const auto size = lhs.size();
        for (size_t i = 0; i < size; i++) {
            maskd[i] = rhs.contains(lhs[i]);
        }
    }

    static void projectOp(ColumnMask& mask,
                          const ColumnVector<size_t>& lhs,
                          const ColumnMask& rhs) {
        msgbioassert(lhs.size() == rhs.size(),
                     "Columns must have matching dimensions");
        auto* maskd = mask.data();
        const auto* lhsd = lhs.data();
        const auto* rhsd = rhs.data();
        const auto size = lhs.size();
        for (size_t i = 0; i < size; i++) {
            maskd[lhsd[i]]._value = rhsd[i];
        }
    }

    static void projectOp(ColumnMask& mask,
                          const ColumnMask& lhs,
                          const ColumnVector<size_t>& rhs) {
        msgbioassert(rhs.size() == lhs.size(),
                     "Columns must have matching dimensions");
        auto* maskd = mask.data();
        const auto* rhsd = rhs.data();
        const auto* lhsd = lhs.data();
        const auto size = rhs.size();
        for (size_t i = 0; i < size; i++) {
            maskd[rhsd[i]]._value = lhsd[i];
        }
    }

    template <typename T>
    static void copyChunk(ColumnVector<T>::ConstIterator srcStart,
                          ColumnVector<T>::ConstIterator srcEnd,
                          ColumnVector<T>& dst) {
        const size_t count = std::distance(srcStart, srcEnd);
        dst.resize(count);
        std::copy(srcStart, srcEnd, dst.begin());
    }

    template <typename T>
    static void copyTransformedChunk(const ColumnVector<size_t>& transform,
                                     const ColumnVector<T>& src,
                                     ColumnVector<T>& dst) {
        const size_t count = transform.size();
        dst.resize(count);

        for (size_t i = 0; i < count; i++) {
            dst[i] = src[transform[i]];
        }
    }
};

}

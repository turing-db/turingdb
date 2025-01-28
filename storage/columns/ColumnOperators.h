#pragma once

#include "BioAssert.h"
#include "ColumnConst.h"
#include "ColumnMask.h"
#include "ColumnVector.h"

namespace db {

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
     * @brief Fills a mask corresponding to 'lhs == rhs' and the corresponding indices
     *
     * @param mask The mask to fill
     * @param indices Stores the index of all the 'true' values of the mask
     * @param lhs Right hand side column
     * @param lhs Light hand side column
     */
    template <typename T, typename U>
    static void equal(ColumnMask& mask,
                      ColumnVector<size_t>& indices,
                      const ColumnVector<T>& lhs,
                      const ColumnVector<U>& rhs) {
        msgbioassert(lhs.size() == rhs.size(),
                     "Columns must have matching dimensions");
        mask.resize(lhs.size());
        indices.clear();
        auto* maskd = mask.data();
        const auto* lhsd = lhs.data();
        const auto* rhsd = rhs.data();
        const auto size = lhs.size();
        for (size_t i = 0; i < size; i++) {
            const bool v = lhsd[i] == rhsd[i];
            maskd[i]._value = v;
            if (v) {
                indices.push_back(i);
            }
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
     * @brief Fills a mask corresponding to 'lhs == rhs' and the corresponding indices
     *
     * @param mask The mask to fill
     * @param indices Stores the index of all the 'true' values of the mask
     * @param lhs Right hand side column
     * @param lhs Light hand side column
     */
    template <typename T, typename U>
    static void equal(ColumnMask& mask,
                      ColumnVector<size_t>& indices,
                      const ColumnVector<T>& lhs,
                      const ColumnConst<U>& rhs) {
        mask.resize(lhs.size());
        indices.clear();
        auto* maskd = mask.data();
        const auto* lhsd = lhs.data();
        const auto& rhsd = rhs.getRaw();
        const auto size = lhs.size();
        for (size_t i = 0; i < size; i++) {
            const bool v = lhsd[i] == rhsd;
            maskd[i]._value = v;
            if (v) {
                indices.push_back(i);
            }
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
     * @brief Fills a mask corresponding to 'lhs == rhs' and the corresponding indices
     *
     * @param mask The mask to fill
     * @param indices Stores the index of all the 'true' values of the mask
     * @param lhs Right hand side column
     * @param lhs Light hand side column
     */
    template <typename T, typename U>
    static void equal(ColumnMask& mask,
                      ColumnVector<size_t>& indices,
                      const ColumnConst<T>& lhs,
                      const ColumnVector<U>& rhs) {
        mask.resize(rhs.size());
        indices.clear();
        auto* maskd = mask.data();
        const auto& lhsd = lhs.getRaw();
        const auto* rhsd = rhs.data();
        const auto size = rhs.size();
        for (size_t i = 0; i < size; i++) {
            const bool v = lhsd == rhsd[i];
            maskd[i]._value = v;
            if (v) {
                indices.push_back(i);
            }
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
    static void equal(ColumnMask& mask,
                      const ColumnConst<T>& lhs,
                      const ColumnConst<U>& rhs) {
        mask.resize(1);
        mask[0] = lhs.getRaw() == rhs.getRaw();
    }

    /**
     * @brief Fills a mask corresponding to 'lhs == rhs' and the corresponding indices
     *
     * @param mask The mask to fill
     * @param indices Stores the index of all the 'true' values of the mask
     * @param lhs Right hand side column
     * @param lhs Light hand side column
     */
    template <typename T, typename U>
    static void equal(ColumnMask& mask,
                      ColumnVector<size_t>& indices,
                      const ColumnConst<T>& lhs,
                      const ColumnConst<U>& rhs) {
        mask.resize(1);
        indices.clear();
        const bool v = lhs.getRaw() == rhs.getRaw();
        mask[0] = v;
        if (v) {
            indices.push_back(0);
        }
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
     * @brief Fills a mask corresponding to 'lhs && rhs' and the corresponding indices
     *
     * @param mask The mask to fill
     * @param indices Stores the index of all the 'true' values of the mask
     * @param lhs Right hand side column
     * @param lhs Light hand side column
     */
    static void andOp(ColumnMask& mask,
                       ColumnVector<size_t>& indices,
                       const ColumnMask& lhs,
                       const ColumnMask& rhs) {
        msgbioassert(lhs.size() == rhs.size(),
                     "Columns must have matching dimensions");
        mask.resize(lhs.size());
        indices.clear();
        auto* maskd = mask.data();
        const auto* lhsd = lhs.data();
        const auto* rhsd = rhs.data();
        const auto size = rhs.size();
        for (size_t i = 0; i < size; i++) {
            const bool v = lhsd[i]._value && rhsd[i]._value;
            maskd[i]._value = v;
            if (v) {
                indices.push_back(i);
            }
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
     * @brief Fills a mask corresponding to 'lhs || rhs' and the corresponding indices
     *
     * @param mask The mask to fill
     * @param indices Stores the index of all the 'true' values of the mask
     * @param lhs Right hand side column
     * @param lhs Light hand side column
     */
    static void orOp(ColumnMask& mask,
                      ColumnVector<size_t>& indices,
                      const ColumnMask& lhs,
                      const ColumnMask& rhs) {
        msgbioassert(lhs.size() == rhs.size(),
                     "Columns must have matching dimensions");
        mask.resize(lhs.size());
        indices.clear();
        auto* maskd = mask.data();
        const auto* lhsd = lhs.data();
        const auto* rhsd = rhs.data();
        const auto size = rhs.size();
        for (size_t i = 0; i < size; i++) {
            const bool v = lhsd[i]._value || rhsd[i]._value;
            maskd[i]._value = v;
            if (v) {
                indices.push_back(i);
            }
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

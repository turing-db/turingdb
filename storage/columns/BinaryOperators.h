#pragma once

#include <cmath>
#include <concepts>
#include <functional>
#include <optional>
#include <string_view>
#include <type_traits>

#include "ColumnVector.h"
#include "ValueText.h"
#include "ColumnConst.h"
#include "TypeUtils.h"
#include "buffers/StringBuffer.h"
#include "list/ListBuffer.h"
#include "list/ListUtils.h"

#include "BioAssert.h"
#include "TuringException.h"

namespace db {

namespace {

struct SafeDivides;
struct SafeModulo;
struct Power;

/**
 * @brief Generic function to apply a generic invokable to two possibly-optional
 * operands, where either operand being nullopt results in the final result being
 * nullopt, and the result of applying the invokable otherwise.
 */
template <typename Func, typename T, typename U>
    requires OptionallyInvokable<Func, T, U>
inline auto optionalGeneric(T&& a,
                            U&& b) -> TypeUtils::optional_invoke_result<Func, T, U> {
    if constexpr (TypeUtils::is_optional_v<T>) {
        if (!a.has_value()) {
            return std::nullopt;
        }
    }

    if constexpr (TypeUtils::is_optional_v<U>) {
        if (!b.has_value()) {
            return std::nullopt;
        }
    }

    // a and b are both either engaged optionals or values, so safe to unwrap

    auto&& av = TypeUtils::unwrap(a);
    auto&& bv = TypeUtils::unwrap(b);

    return Func {}(av, bv);
}

/**
 * @brief Wrapper of overloads of @ref apply functions for different combinations of
 * operands shapes and outputs for executing operators.
 * @detail The role of this struct is to define once the logic for each possible
 * combination of operand and result columns. It is not concerned with the internal types
 * of its arguments.
 */
template <typename Op, typename Res, typename T, typename U>
struct BinaryOpExecutor {
    static void apply(ColumnVector<Res>* res,
                      const ColumnVector<T>* lhs,
                      const ColumnVector<U>* rhs,
                      Op op = {}) {
        bioassert(lhs->size() == rhs->size(), "Misshapen ColumnVectors.");
        const size_t size = lhs->size();
        res->resize(size);

        const auto& lhsd = lhs->getRaw();
        const auto& rhsd = rhs->getRaw();
        auto& resd = res->getRaw();

        for (size_t i = 0; i < size; i++) {
            resd[i] = op(lhsd[i], rhsd[i]);
        }
    }

    static void apply(ColumnVector<Res>* res,
                      const ColumnVector<T>* lhs,
                      const ColumnConst<U>* rhs,
                      Op op = {}) {
       const size_t size = lhs->size();

       res->resize(size);
       auto& resd = res->getRaw();
       const auto& lhsd = lhs->getRaw();
       const auto& val = rhs->getRaw();

       for (size_t i = 0; i < size; i++) {
           resd[i] = op(lhsd[i], val);
       }
    }

    static void apply(ColumnVector<Res>* res,
                      const ColumnConst<T>* lhs,
                      const ColumnVector<U>* rhs,
                      Op op = {}) {
       const size_t size = rhs->size();

       res->resize(size);
       auto& resd = res->getRaw();
       const auto& val = lhs->getRaw();
       const auto& rhsd = rhs->getRaw();

       for (size_t i = 0; i < size; i++) {
           resd[i] = op(val, rhsd[i]);
       }
    }

    static void apply(ColumnConst<Res>* res,
                      const ColumnConst<T>* lhs,
                      const ColumnConst<U>* rhs,
                      Op op = {}) {
        const Res& result = op(lhs->getRaw(), rhs->getRaw());
        res->set(result);
    }

    static_assert(std::is_trivially_copyable_v<Op>, "NOTE: Op passed by value in apply");
};

/**
 * @brief Widen an unsigned integer operand to the signed integer the query language has.
 * Cypher has one integer type and it is signed: a tally is carried unsigned because it
 * can never be negative, but an expression over it can be, and computing in the
 * unsigned type would wrap that result around zero (12 - 18 as 2^64 - 6).
 */
template <typename T>
inline auto asSignedInteger(T&& value) {
    using Decayed = std::decay_t<T>;

    if constexpr (std::is_same_v<Decayed, uint64_t>) {
        return static_cast<int64_t>(value);
    } else if constexpr (std::is_same_v<Decayed, std::optional<uint64_t>>) {
        if (!value.has_value()) {
            return std::optional<int64_t> {};
        }

        return std::optional<int64_t> {static_cast<int64_t>(*value)};
    } else {
        return std::forward<T>(value);
    }
}

/**
 * @brief The number a type-erased cell holds, whatever numeric type it is tagged as.
 * Absent when the cell holds no number - a string, a nested list or a null - so the
 * arithmetic over it answers null the way it does over an absent operand.
 */
inline std::optional<double> cellNumber(const ListElementView cell) {
    switch (cell.getTag()) {
        case ListBufferTypeTag::Int:
            return static_cast<double>(cell.getAs<types::Int64::Primitive>());
        break;

        case ListBufferTypeTag::UInt:
            return static_cast<double>(cell.getAs<types::UInt64::Primitive>());
        break;

        case ListBufferTypeTag::Double:
            return cell.getAs<types::Double::Primitive>();
        break;

        default:
            return std::nullopt;
        break;
    }
}

/**
 * @brief An operand of an arithmetic operation one side of which is a type-erased cell,
 * read as the double every side of such an operation computes in: mixed tags name no
 * single integer type, which is why a reduction over cells lands on a double too.
 */
template <typename T>
inline std::optional<double> cellOperand(const T& value) {
    if constexpr (std::is_same_v<std::decay_t<T>, ListElementView>) {
        return cellNumber(value);
    } else if constexpr (TypeUtils::is_optional_v<T>) {
        if (!value.has_value()) {
            return std::nullopt;
        }

        return cellOperand(*value);
    } else {
        return static_cast<double>(value);
    }
}

template <typename T>
concept TaggedCell = std::same_as<TypeUtils::unwrap_optional_t<T>, ListElementView>;

// The operators that compute a number out of two, as opposed to the index, which reads a
// cell as the list it may hold rather than as a number
template <typename F>
concept ComputesNumbers = std::is_same_v<F, std::plus<>>
                       || std::is_same_v<F, std::minus<>>
                       || std::is_same_v<F, std::multiplies<>>
                       || std::is_same_v<F, SafeDivides>
                       || std::is_same_v<F, SafeModulo>
                       || std::is_same_v<F, Power>;

template <typename F, typename T, typename U>
concept ComputesOverTaggedCell = ComputesNumbers<F> && (TaggedCell<T> || TaggedCell<U>);

// Every other pair: the operands are computed in a type the column names. Spelled as a
// concept of its own so the two operators below order against each other - a negation
// written twice is two constraints, one written once is one
template <typename F, typename T, typename U>
concept ComputesOverColumnType = !ComputesOverTaggedCell<F, T, U>;

// The concatenation of two lists, as opposed to the two strings the same operator joins.
// Either side may be nullable, a stored list being read out of a nullable column.
template <typename A, typename B>
concept ConcatenatesLists =
    std::same_as<TypeUtils::unwrap_optional_t<std::decay_t<A>>, ListView>
    && std::same_as<TypeUtils::unwrap_optional_t<std::decay_t<B>>, ListView>;

// A concatenation answers null where an operand is null, or where a type-erased cell holds
// no text of its own. Two plain values always answer the text they make.
template <typename A, typename B>
concept ConcatenatesNullableText = TypeUtils::is_optional_v<std::decay_t<A>>
                                || TypeUtils::is_optional_v<std::decay_t<B>>
                                || TaggedCell<A>
                                || TaggedCell<B>;

/**
 * @brief Thin wrapper over a provided functor @param F to dispatch optional logic
 * accordingly
 */
template <typename F, bool NarrowsUnsigned = true>
struct BinaryOp {
    template <typename T>
    static inline decltype(auto) operand(T&& value) {
        if constexpr (NarrowsUnsigned) {
            return asSignedInteger(std::forward<T>(value));
        } else {
            return std::forward<T>(value);
        }
    }

    template <typename T, typename U>
        requires (TypeUtils::is_optional_v<T> || TypeUtils::is_optional_v<U>)
              && ComputesOverColumnType<F, T, U>
    inline decltype(auto) operator()(T&& a, U&& b) const {
        return optionalGeneric<F>(operand(std::forward<T>(a)), operand(std::forward<U>(b)));
    }

    template <typename T, typename U>
        requires ComputesOverColumnType<F, T, U>
    inline decltype(auto) operator()(T&& a, U&& b) const {
        return F {}(operand(std::forward<T>(a)), operand(std::forward<U>(b)));
    }

    // A cell carries its type per row rather than in the column's, so the operands are
    // read through the tag each row holds instead of computed in a type the column names
    template <typename T, typename U>
        requires ComputesOverTaggedCell<F, T, U>
    inline std::optional<double> operator()(T&& a, U&& b) const {
        const std::optional<double> lhs = cellOperand(a);
        const std::optional<double> rhs = cellOperand(b);

        if (!lhs.has_value() || !rhs.has_value()) {
            return std::nullopt;
        }

        return F {}(*lhs, *rhs);
    }
};

struct SafeDivides {
    template <typename T, typename U>
    inline auto operator()(T&& a, U&& b) const {
        if (b == 0) {
            throw TuringException("Attempted to divide by zero.");
        }
        return std::divides<> {}(std::forward<T>(a), std::forward<U>(b));
    }
};

struct SafeModulo {
    template <typename T, typename U>
    inline auto operator()(T&& a, U&& b) const {
        using DecayT = std::decay_t<T>;
        using DecayU = std::decay_t<U>;

        if (b == 0) {
            throw TuringException("Attempted modulo by zero.");
        }

        if constexpr (std::is_integral_v<DecayT> && std::is_integral_v<DecayU>) {
            return std::modulus<> {}(std::forward<T>(a), std::forward<U>(b));
        } else {
            return std::fmod(static_cast<double>(a), static_cast<double>(b));
        }
    }
};

struct Power {
    template <typename T, typename U>
    inline double operator()(T&& a, U&& b) const {
        return std::pow(static_cast<double>(a), static_cast<double>(b));
    }
};

struct Concatenate {
    StringBuffer* _stringBuffer {nullptr};
    QueryListBuffer* _listBuffer {nullptr};

    // Two values join as the text each is written with: a string as its own characters, a
    // number as Cypher prints it, a type-erased cell as whatever its tag says it holds. A
    // cell holding no text - a null, a nested list - makes the concatenation null, as a
    // null operand does.
    template <typename A, typename B>
        requires (!ConcatenatesLists<A, B>)
    inline auto operator()(const A& a, const B& b) const {
        ValueTextScratch leftScratch;
        ValueTextScratch rightScratch;

        if constexpr (ConcatenatesNullableText<A, B>) {
            std::string_view left;
            std::string_view right;

            if (!concatenatedText(a, left, leftScratch) || !concatenatedText(b, right, rightScratch)) {
                return std::optional<std::string_view> {};
            }

            return std::optional<std::string_view> {_stringBuffer->concatenate(left, right)};
        } else {
            return _stringBuffer->concatenate(plainText(a, leftScratch), plainText(b, rightScratch));
        }
    }

    inline ListView operator()(ListView a, ListView b) const {
        return _listBuffer->concatenate(a, b);
    }

    // The text one side of such a concatenation contributes, false where it contributes
    // none. @param scratch holds it where the value has to be written out.
    template <typename T>
    static bool concatenatedText(const T& value, std::string_view& text, ValueTextScratch& scratch) {
        if constexpr (TypeUtils::is_optional_v<T>) {
            if (!value.has_value()) {
                return false;
            }

            return concatenatedText(*value, text, scratch);
        } else if constexpr (std::is_same_v<std::decay_t<T>, ListElementView>) {
            return cellText(value, text, scratch);
        } else {
            text = plainText(value, scratch);
            return true;
        }
    }

    // The text a value always contributes: its own characters, or the ones Cypher writes
    // the number with
    template <typename T>
    static std::string_view plainText(const T& value, ValueTextScratch& scratch) {
        if constexpr (std::is_arithmetic_v<std::decay_t<T>>) {
            return valueTextInto(value, scratch);
        } else {
            return value;
        }
    }

    static bool cellText(const ListElementView cell, std::string_view& text, ValueTextScratch& scratch) {
        switch (cell.getTag()) {
            case ListBufferTypeTag::String:
                text = cell.getAs<types::String::Primitive>();
                return true;
            break;

            case ListBufferTypeTag::Int:
                text = valueTextInto(cell.getAs<types::Int64::Primitive>(), scratch);
                return true;
            break;

            case ListBufferTypeTag::UInt:
                text = valueTextInto(cell.getAs<types::UInt64::Primitive>(), scratch);
                return true;
            break;

            case ListBufferTypeTag::Double:
                text = valueTextInto(cell.getAs<types::Double::Primitive>(), scratch);
                return true;
            break;

            case ListBufferTypeTag::Bool:
                text = valueTextInto(cell.getAs<types::Bool::Primitive>(), scratch);
                return true;
            break;

            default:
                return false;
            break;
        }
    }

    template <typename A, typename B>
        requires ConcatenatesLists<A, B>
              && (TypeUtils::is_optional_v<A> || TypeUtils::is_optional_v<B>)
    inline std::optional<ListView> operator()(const A& a, const B& b) const {
        if constexpr (TypeUtils::is_optional_v<A>) {
            if (!a.has_value()) {
                return std::nullopt;
            }
        }

        if constexpr (TypeUtils::is_optional_v<B>) {
            if (!b.has_value()) {
                return std::nullopt;
            }
        }

        return _listBuffer->concatenate(TypeUtils::unwrap(a), TypeUtils::unwrap(b));
    }
};

struct ListIndexImpl {
    inline std::optional<ListElementView> operator()(ListView list, int64_t index) const {
        const int64_t size = list.size();
        const int64_t offset = index < 0 ? size + index : index;

        if (offset < 0 || offset >= size) {
            return std::nullopt;
        }

        return list.elements()[static_cast<size_t>(offset)];
    }

    inline std::optional<ListElementView> operator()(ListElementView cell, int64_t index) const {
        if (cell.getTag() != ListBufferTypeTag::ListView) {
            return std::nullopt;
        }

        return operator()(cell.getAs<ListView>(), index);
    }

    inline std::optional<ListElementView> operator()(ListView list, uint64_t index) const {
        if (index >= list.size()) {
            return std::nullopt;
        }

        return list.elements()[static_cast<size_t>(index)];
    }

    inline std::optional<ListElementView> operator()(ListElementView cell, uint64_t index) const {
        if (cell.getTag() != ListBufferTypeTag::ListView) {
            return std::nullopt;
        }

        return operator()(cell.getAs<ListView>(), index);
    }

    inline std::optional<ListElementView> operator()(ListView /*unused*/, PropertyNull /*unused*/) const {
        return std::nullopt;
    }

    inline std::optional<ListElementView> operator()(ListElementView /*unused*/, PropertyNull /*unused*/) const {
        return std::nullopt;
    }

    // Unused but defined to satisfy dispatcher
    std::optional<ListElementView> operator()(int64_t /*unused*/, ListView /*unused*/) const {
        throw TuringException("Index operands the wrong way round.");
    }
    std::optional<ListElementView> operator()(int64_t /*unused*/, ListElementView /*unused*/) const {
        throw TuringException("Index operands the wrong way round.");
    }
    std::optional<ListElementView> operator()(PropertyNull /*unused*/, ListView /*unused*/) const {
        throw TuringException("Index operands the wrong way round.");
    }
    std::optional<ListElementView> operator()(PropertyNull /*unused*/, ListElementView /*unused*/) const {
        throw TuringException("Index operands the wrong way round.");
    }
    std::optional<ListElementView> operator()(uint64_t /*unused*/, ListView /*unused*/) const {
        throw TuringException("Index operands the wrong way round.");
    }
    std::optional<ListElementView> operator()(uint64_t /*unused*/, ListElementView /*unused*/) const {
        throw TuringException("Index operands the wrong way round.");
    }
};

/**
 * @brief The indexed element read as the type its list names, rather than as the tagged
 * cell @ref ListIndexImpl hands back. A cell carrying another tag - the tagged null a
 * nullable column puts in a list - reads as the absent value.
 */
template <typename T>
struct ValueListIndexImpl {
    template <typename Base, typename Index>
        requires std::is_invocable_v<ListIndexImpl, Base, Index>
    inline std::optional<T> operator()(const Base& base, const Index& index) const {
        const std::optional<ListElementView> cell = ListIndexImpl {}(base, index);

        if (!cell.has_value() || cell->getTag() != TypeToListBufferTag<T>::Tag) {
            return std::nullopt;
        }

        return cell->getAs<T>();
    }
};

}

using Add = BinaryOp<std::plus<>>;
using Sub = BinaryOp<std::minus<>>;
using Mul = BinaryOp<std::multiplies<>>;
using Div = BinaryOp<SafeDivides>;
using Mod = BinaryOp<SafeModulo>;
using Pow = BinaryOp<Power>;
using Concat = Concatenate;
using ListIndex = BinaryOp<ListIndexImpl, /*NarrowsUnsigned=*/false>;

template <typename T>
using ValueListIndex = BinaryOp<ValueListIndexImpl<T>, /*NarrowsUnsigned=*/false>;

}


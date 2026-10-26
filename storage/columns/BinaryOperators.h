#pragma once

#include <cmath>
#include <concepts>
#include <functional>
#include <limits>
#include <optional>
#include <span>
#include <string_view>
#include <type_traits>

#include "ColumnVector.h"
#include "ValueText.h"
#include "ColumnConst.h"
#include "TypeUtils.h"
#include "buffers/StringBuffer.h"
#include "list/ListBuffer.h"
#include "CellUtils.h"
#include "metadata/DateTime.h"

#include "BioAssert.h"
#include "TuringException.h"

namespace db {

namespace {

struct Subtract;
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
inline auto arithmeticOperand(T&& value) {
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
inline std::optional<double> cellNumber(const CellView cell) {
    switch (cell.getTag()) {
        case CellTag::Int:
            return static_cast<double>(cell.getAs<types::Int64::Primitive>());
        break;

        case CellTag::UInt:
            return static_cast<double>(cell.getAs<types::UInt64::Primitive>());
        break;

        case CellTag::Double:
            return cell.getAs<types::Double::Primitive>();
        break;

        case CellTag::Bool:
        case CellTag::String:
        case CellTag::Embedding:
        case CellTag::ListView:
        case CellTag::Null:
        case CellTag::NodeID:
        case CellTag::EdgeID:
        case CellTag::DateTime:
        case CellTag::MapView:
        case CellTag::Duration:
        case CellTag::INVALID:
            return std::nullopt;
        break;
    }

    return std::nullopt;
}

/**
 * @brief An operand of an arithmetic operation one side of which is a type-erased cell,
 * read as a double: the type two operands compute in unless both hold integers.
 */
template <typename T>
inline std::optional<double> cellOperand(const T& value) {
    if constexpr (std::is_same_v<std::decay_t<T>, CellView>) {
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

/// Whether a type-erased cell is tagged as one of the integers.
inline bool cellHoldsAnInteger(const CellView cell) {
    const CellTag tag = cell.getTag();

    return tag == CellTag::Int || tag == CellTag::UInt;
}

/// The integer a cell holds, read where @ref cellHoldsAnInteger answered for it.
inline int64_t cellInteger(const CellView cell) {
    if (cell.getTag() == CellTag::UInt) {
        return static_cast<int64_t>(cell.getAs<types::UInt64::Primitive>());
    }

    return cell.getAs<types::Int64::Primitive>();
}

/**
 * @brief Whether such an operand holds an integer: the tags an integer is stored under,
 * and the integer columns, which a non-cell operand answers at compile time.
 */
template <typename T>
inline bool holdsAnInteger(const T& value) {
    using Decayed = std::decay_t<T>;

    if constexpr (std::is_same_v<Decayed, CellView>) {
        return cellHoldsAnInteger(value);
    } else if constexpr (TypeUtils::is_optional_v<T>) {
        return value.has_value() && holdsAnInteger(*value);
    } else {
        return std::is_integral_v<Decayed> && !std::is_same_v<Decayed, bool>;
    }
}

/// Whether such an operand holds a string: a cell tagged as one, or a string value.
template <typename T>
inline bool holdsAString(const T& value) {
    using Decayed = std::decay_t<T>;

    if constexpr (std::is_same_v<Decayed, CellView>) {
        return value.getTag() == CellTag::String;
    } else if constexpr (TypeUtils::is_optional_v<T>) {
        return value.has_value() && holdsAString(*value);
    } else {
        return std::is_same_v<Decayed, types::String::Primitive>
            || std::is_same_v<Decayed, types::String::OwningPrimitive>;
    }
}

/// Whether such an operand holds a list: a cell tagged as one, or a list value.
template <typename T>
inline bool holdsAList(const T& value) {
    using Decayed = std::decay_t<T>;

    if constexpr (std::is_same_v<Decayed, CellView>) {
        return value.getTag() == CellTag::ListView;
    } else if constexpr (TypeUtils::is_optional_v<T>) {
        return value.has_value() && holdsAList(*value);
    } else {
        return std::is_same_v<Decayed, ListView>;
    }
}

/// Whether such an operand is null: an absent value, the null literal, or a cell tagged as one.
template <typename T>
inline bool holdsNull(const T& value) {
    using Decayed = std::decay_t<T>;

    if constexpr (std::is_same_v<Decayed, CellView>) {
        return value.getTag() == CellTag::Null;
    } else if constexpr (TypeUtils::is_optional_v<T>) {
        return !value.has_value() || holdsNull(*value);
    } else {
        return std::is_same_v<Decayed, PropertyNull>;
    }
}

// The list an operand of a '+' that joins a list stands for: the list a cell holds, or the
// one-element list of any other value, a null among them. The list buffer keeps a string
// as a view, so a string owning its characters is copied into @param stringBuffer first.
template <typename T>
inline ListView joinedListOperand(QueryListBuffer* listBuffer, StringBuffer* stringBuffer, const T& value) {
    using Decayed = std::decay_t<T>;

    QueryListBuffer::ListItemVariant item {PropertyNull {}};

    if constexpr (std::is_same_v<Decayed, ListView>) {
        return value;
    } else if constexpr (std::is_same_v<Decayed, CellView>) {
        if (value.getTag() == CellTag::ListView) {
            return value.template getAs<ListView>();
        }

        const auto read = []<typename E>(const CellView cell) -> QueryListBuffer::ListItemVariant {
            return cell.getAs<E>();
        };

        item = CellTagDispatcher {value.getTag()}.execute(read, value);
    } else if constexpr (TypeUtils::is_optional_v<Decayed>) {
        if (value.has_value()) {
            return joinedListOperand(listBuffer, stringBuffer, *value);
        }
    } else if constexpr (std::is_same_v<Decayed, types::String::OwningPrimitive>) {
        item = stringBuffer->insert(value);
    } else {
        item = arithmeticOperand(value);
    }

    return listBuffer->insert(std::span<const QueryListBuffer::ListItemVariant> {&item, 1});
}

/// The string such an operand holds, read where @ref holdsAString answered for it.
template <typename T>
inline std::string_view operandString(const T& value) {
    using Decayed = std::decay_t<T>;

    if constexpr (std::is_same_v<Decayed, CellView>) {
        return value.template getAs<types::String::Primitive>();
    } else if constexpr (TypeUtils::is_optional_v<T>) {
        return operandString(*value);
    } else {
        return value;
    }
}

/// The list such an operand holds, read where @ref holdsAList answered for it.
template <typename T>
inline ListView operandList(const T& value) {
    using Decayed = std::decay_t<T>;

    if constexpr (std::is_same_v<Decayed, CellView>) {
        return value.template getAs<ListView>();
    } else if constexpr (TypeUtils::is_optional_v<T>) {
        return operandList(*value);
    } else {
        return value;
    }
}

template <typename T>
inline CellView stagedCell(QueryListBuffer* listBuffer, T value) {
    const QueryListBuffer::ListItemVariant element {value};

    return listBuffer->insert(std::span<const QueryListBuffer::ListItemVariant> {&element, 1}).front();
}

template <typename T>
concept Temporal = std::same_as<TypeUtils::unwrap_optional_t<std::decay_t<T>>, DateTime>
                || std::same_as<TypeUtils::unwrap_optional_t<std::decay_t<T>>, Duration>;

/// Whether such an operand holds a number, an instant or a duration.
template <typename T>
inline bool holdsAnArithmeticValue(const T& value) {
    using Decayed = std::decay_t<T>;

    if constexpr (std::is_same_v<Decayed, CellView>) {
        const CellTag tag = value.getTag();

        return tag == CellTag::Int
            || tag == CellTag::UInt
            || tag == CellTag::Double
            || tag == CellTag::DateTime
            || tag == CellTag::Duration;
    } else if constexpr (TypeUtils::is_optional_v<T>) {
        return value.has_value() && holdsAnArithmeticValue(*value);
    } else {
        return std::is_arithmetic_v<Decayed> || Temporal<Decayed>;
    }
}

// Calls @param visit with the value an operand holds, read as its own type: a cell as the
// type its tag names, a column value as itself. An operand holding nothing arithmetic reads
// answers null, as an absent operand does.
template <typename T, typename Visit>
inline std::optional<CellView> visitArithmeticOperand(const T& value, Visit&& visit) {
    if constexpr (std::is_same_v<std::decay_t<T>, CellView>) {
        auto visitor = [&visit]<typename N>(CellView view) -> std::optional<CellView> {
            if constexpr (std::is_same_v<N, types::UInt64::Primitive>) {
                return visit(static_cast<int64_t>(view.getAs<N>()));
            } else if constexpr (std::is_same_v<N, types::Int64::Primitive>
                              || std::is_same_v<N, types::Double::Primitive>
                              || std::is_same_v<N, types::DateTime::Primitive>
                              || std::is_same_v<N, types::Duration::Primitive>) {
                return visit(view.getAs<N>());
            } else {
                return std::nullopt;
            }
        };

       return CellTagDispatcher{value.getTag()}.execute(visitor, value);

    } else if constexpr (TypeUtils::is_optional_v<T>) {
        return value.has_value() ? visitArithmeticOperand(*value, visit) : std::nullopt;
    } else if constexpr (std::is_same_v<std::decay_t<T>, PropertyNull>
                      || std::is_same_v<std::decay_t<T>, types::Bool::Primitive>) {
        return std::nullopt;
    } else {
        return visit(arithmeticOperand(value));
    }
}

template <typename T>
concept TaggedCell = std::same_as<TypeUtils::unwrap_optional_t<T>, CellView>;

// The operators whose result carries the type of its operands, as opposed to the index,
// which reads a cell as the list it may hold rather than as a number
template <typename F>
concept ComputesNumbers = std::is_same_v<F, std::plus<>>
                       || std::is_same_v<F, Subtract>
                       || std::is_same_v<F, std::multiplies<>>
                       || std::is_same_v<F, SafeDivides>
                       || std::is_same_v<F, SafeModulo>;

// '^' answers a double whatever it is given, so its result carries no tag of its own
template <typename F>
concept ComputesReals = std::is_same_v<F, Power>;

template <typename F, typename T, typename U>
concept ComputesOverTaggedCell = ComputesNumbers<F> && (TaggedCell<T> || TaggedCell<U>);

template <typename T>
concept ListTyped = std::same_as<TypeUtils::unwrap_optional_t<T>, ListView>;

// A '+' of a list and a cell answers a list, whatever the cell holds
template <typename F, typename T, typename U>
concept AddsAListToATaggedCell = ComputesOverTaggedCell<F, T, U>
                              && std::is_same_v<F, std::plus<>>
                              && (ListTyped<T> || ListTyped<U>);

template <typename F, typename T, typename U>
concept ComputesRealOverTaggedCell = ComputesReals<F> && (TaggedCell<T> || TaggedCell<U>);

// Every other pair: the operands are computed in a type the column names. Spelled as a
// concept of its own so the two operators below order against each other - a negation
// written twice is two constraints, one written once is one
template <typename F, typename T, typename U>
concept ComputesOverColumnType = !ComputesOverTaggedCell<F, T, U>
                              && !ComputesRealOverTaggedCell<F, T, U>;

// The concatenation of two lists, as opposed to the two strings the same operator joins.
// Either side may be nullable, a stored list being read out of a nullable column.
template <typename A, typename B>
concept ConcatenatesLists = ListTyped<A> && ListTyped<B>;

template <typename A, typename B>
concept ConcatenatesACell = TaggedCell<A> || TaggedCell<B>;

// What '||' over a type-erased cell answers: a cell where both sides are cells, since only
// their rows say whether they hold strings or lists, and otherwise what the other side is
template <typename A, typename B>
using CellConcatenation = std::conditional_t<TaggedCell<A> && TaggedCell<B>,
                                             CellView,
                                             std::conditional_t<ListTyped<A> || ListTyped<B>,
                                                                ListView,
                                                                std::string_view>>;

// The text a type-erased cell joins a string with under '+', false where its tag names a
// value that has none
inline bool cellText(const CellView cell, std::string_view& text, ValueTextScratch& scratch) {
    switch (cell.getTag()) {
        case CellTag::String:
            text = cell.getAs<types::String::Primitive>();
            return true;
        break;

        case CellTag::Int:
            text = valueTextInto(cell.getAs<types::Int64::Primitive>(), scratch);
            return true;
        break;

        case CellTag::UInt:
            text = valueTextInto(cell.getAs<types::UInt64::Primitive>(), scratch);
            return true;
        break;

        case CellTag::Double:
            text = valueTextInto(cell.getAs<types::Double::Primitive>(), scratch);
            return true;
        break;

        case CellTag::Bool:
            text = valueTextInto(cell.getAs<types::Bool::Primitive>(), scratch);
            return true;
        break;

        case CellTag::DateTime:
            text = valueTextInto(cell.getAs<types::DateTime::Primitive>(), scratch);
            return true;
        break;

        case CellTag::Duration:
            text = valueTextInto(cell.getAs<types::Duration::Primitive>(), scratch);
            return true;
        break;

        default:
            return false;
        break;
    }
}

// The text one side of a '+' joins a string with: a string's own characters, or the ones
// Cypher writes a number, a boolean or a temporal with. False where the side contributes
// none. Where the text has to be written out, @param scratch holds it.
template <typename T>
bool joinedSideText(const T& value, std::string_view& text, ValueTextScratch& scratch) {
    using Decayed = std::decay_t<T>;

    if constexpr (TypeUtils::is_optional_v<T>) {
        if (!value.has_value()) {
            return false;
        }

        return joinedSideText(*value, text, scratch);
    } else if constexpr (std::is_same_v<Decayed, CellView>) {
        return cellText(value, text, scratch);
    } else if constexpr (std::is_same_v<Decayed, types::String::Primitive>
                      || std::is_same_v<Decayed, types::String::OwningPrimitive>) {
        text = value;
        return true;
    } else if constexpr (std::is_arithmetic_v<Decayed>
                      || std::is_same_v<Decayed, types::Bool::Primitive>
                      || Temporal<Decayed>) {
        text = valueTextInto(value, scratch);
        return true;
    } else {
        return false;
    }
}

template <typename T, typename U>
std::optional<std::string_view> joinedText(StringBuffer* stringBuffer, const T& a, const U& b) {
    ValueTextScratch leftScratch;
    ValueTextScratch rightScratch;
    std::string_view left;
    std::string_view right;

    if (!joinedSideText(a, left, leftScratch) || !joinedSideText(b, right, rightScratch)) {
        return std::nullopt;
    }

    return stringBuffer->concatenate(left, right);
}

/**
 * @brief Thin wrapper over a provided functor @param F to dispatch optional logic
 * accordingly
 */
template <typename F, bool NarrowsUnsigned = true>
struct BinaryOp {
    QueryListBuffer* _listBuffer {nullptr};
    StringBuffer* _stringBuffer {nullptr};

    template <typename T>
    static inline decltype(auto) operand(T&& value) {
        if constexpr (NarrowsUnsigned) {
            return arithmeticOperand(std::forward<T>(value));
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
    // read through the tag each row holds instead of computed in a type the column names,
    // and the result is staged as a cell of its own. Two integers answer an integer there
    // as they do anywhere else, which is what lets the result be written to an integer
    // property. A '+' joins a list where either side holds one, and text where either
    // holds a string, before it computes anything.
    template <typename T, typename U>
        requires ComputesOverTaggedCell<F, T, U>
    inline std::optional<CellView> operator()(T&& a, U&& b) const {
        bioassert(_listBuffer, "Arithmetic over a type-erased cell has no list buffer to stage its result in");

        if constexpr (std::is_same_v<F, std::plus<>>) {
            if (holdsAList(a) || holdsAList(b)) {
                return stagedCell(_listBuffer, joinedList(a, b));
            } else if (holdsNull(a) || holdsNull(b)) {
                return std::nullopt;
            } else if (holdsAString(a) || holdsAString(b)) {
                bioassert(_stringBuffer, "A '+' over a string cell has no string buffer to join its text in");

                const std::optional<std::string_view> text = joinedText(_stringBuffer, a, b);
                if (!text.has_value()) {
                    throwAdditionTypeError();
                }

                return stagedCell(_listBuffer, *text);
            } else if (!holdsAnArithmeticValue(a) || !holdsAnArithmeticValue(b)) {
                throwAdditionTypeError();
            }
        }

        return visitArithmeticOperand(a, [this, &b](const auto lhs) {
            return visitArithmeticOperand(b, [this, lhs](const auto rhs) -> std::optional<CellView> {
                if constexpr (requires { F {}(lhs, rhs); }) {
                    return stagedCell(this->_listBuffer, F {}(lhs, rhs));
                } else {
                    throw TuringException("Operands are not valid and compatible types");
                }
            });
        });
    }

    // A list beside a cell joins the list the cell holds, or takes the cell as one more
    // element. Only an absent list makes the result null.
    template <typename T, typename U>
        requires AddsAListToATaggedCell<F, T, U>
    inline std::optional<ListView> operator()(T&& a, U&& b) const {
        bioassert(_listBuffer, "A '+' over a type-erased cell has no list buffer to join its list in");

        const bool joinsAnAbsentList = (ListTyped<T> && !holdsAList(a)) || (ListTyped<U> && !holdsAList(b));
        if (joinsAnAbsentList) {
            return std::nullopt;
        }

        return joinedList(a, b);
    }

    template <typename T, typename U>
        requires ComputesRealOverTaggedCell<F, T, U>
    inline std::optional<double> operator()(T&& a, U&& b) const {
        const std::optional<double> lhs = cellOperand(a);
        const std::optional<double> rhs = cellOperand(b);

        if (!lhs.has_value() || !rhs.has_value()) {
            return std::nullopt;
        }

        return F {}(*lhs, *rhs);
    }

    // The list '+' makes of two values one of which holds a list: the two lists joined, or
    // the other value appended or prepended as one element
    template <typename T, typename U>
    ListView joinedList(const T& a, const U& b) const {
        return _listBuffer->concatenate(joinedListOperand(_listBuffer, _stringBuffer, a),
                                        joinedListOperand(_listBuffer, _stringBuffer, b));
    }

    [[noreturn]] static void throwAdditionTypeError() {
        throw TuringException("Operands are not valid and compatible types for '+'");
    }
};

struct Subtract : std::minus<> {
    using std::minus<>::operator();

    // Unused but defined to satisfy dispatcher
    DateTime operator()(Duration duration, DateTime instant) const {
        throw TuringException("A duration minus a datetime is not defined.");
    }
};

struct SafeDivides {
    template <typename T, typename U>
        requires (!Temporal<U>) && requires(T&& a, U&& b) { std::divides<> {}(a, b); }
    inline auto operator()(T&& a, U&& b) const {
        using DecayT = std::decay_t<T>;
        using DecayU = std::decay_t<U>;

        if (b == 0) {
            throw TuringException("Attempted to divide by zero.");
        }

        if constexpr (std::is_integral_v<DecayT> && std::is_signed_v<DecayT>
                      && std::is_integral_v<DecayU> && std::is_signed_v<DecayU>) {
            const bool overflows = a == std::numeric_limits<DecayT>::min() && b == -1;
            if (overflows) {
                throw TuringException("Division overflow.");
            }
        }

        return std::divides<> {}(std::forward<T>(a), std::forward<U>(b));
    }

    // Unused but defined to satisfy dispatcher
    template <typename T>
    Duration operator()(T&& number, Duration duration) const {
        throw TuringException("A number divided by a duration is not defined.");
    }
};

struct SafeModulo {
    template <typename T, typename U>
        requires (!Temporal<T>) && std::is_arithmetic_v<std::decay_t<T>> && std::is_arithmetic_v<std::decay_t<U>>
    inline auto operator()(T&& a, U&& b) const {
        using DecayT = std::decay_t<T>;
        using DecayU = std::decay_t<U>;

        if (b == 0) {
            throw TuringException("Attempted modulo by zero.");
        }

        if constexpr (std::is_integral_v<DecayT> && std::is_integral_v<DecayU>) {
            if constexpr (std::is_signed_v<DecayT> && std::is_signed_v<DecayU>) {
                if (b == -1) {
                    return decltype(a % b) {0};
                }
            }

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
    // number or a boolean as Cypher prints it, which is how '+' joins one to a string
    template <typename A, typename B>
        requires (!ConcatenatesLists<A, B>) && (!ConcatenatesACell<A, B>)
    inline auto operator()(const A& a, const B& b) const {
        if constexpr (TypeUtils::is_optional_v<A> || TypeUtils::is_optional_v<B>) {
            return joinedText(_stringBuffer, a, b);
        } else {
            ValueTextScratch leftScratch;
            ValueTextScratch rightScratch;
            std::string_view left;
            std::string_view right;

            joinedSideText(a, left, leftScratch);
            joinedSideText(b, right, rightScratch);

            return _stringBuffer->concatenate(left, right);
        }
    }

    inline ListView operator()(ListView a, ListView b) const {
        return _listBuffer->concatenate(a, b);
    }

    // '||' converts nothing, so a type-erased cell joins as the value its row holds: a
    // string beside a string, a list beside a list. A row holding any other value is the
    // type error the two values are.
    template <typename A, typename B>
        requires ConcatenatesACell<A, B>
    inline std::optional<CellConcatenation<A, B>> operator()(const A& a, const B& b) const {
        using Result = CellConcatenation<A, B>;

        if (holdsNull(a) || holdsNull(b)) {
            return std::nullopt;
        }

        if constexpr (!std::is_same_v<Result, ListView>) {
            if (holdsAString(a) && holdsAString(b)) {
                const std::string_view text = _stringBuffer->concatenate(operandString(a), operandString(b));

                if constexpr (std::is_same_v<Result, CellView>) {
                    return stagedCell(_listBuffer, text);
                } else {
                    return text;
                }
            }
        }

        if constexpr (!std::is_same_v<Result, std::string_view>) {
            if (holdsAList(a) && holdsAList(b)) {
                const ListView list = _listBuffer->concatenate(operandList(a), operandList(b));

                if constexpr (std::is_same_v<Result, CellView>) {
                    return stagedCell(_listBuffer, list);
                } else {
                    return list;
                }
            }
        }

        throw TuringException("Operands are not valid and compatible types for '||'");
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
    inline std::optional<CellView> operator()(ListView list, int64_t index) const {
        const int64_t size = list.size();
        const int64_t offset = index < 0 ? size + index : index;

        if (offset < 0 || offset >= size) {
            return std::nullopt;
        }

        return list.elements()[static_cast<size_t>(offset)];
    }

    inline std::optional<CellView> operator()(CellView cell, int64_t index) const {
        if (cell.getTag() != CellTag::ListView) {
            return std::nullopt;
        }

        return operator()(cell.getAs<ListView>(), index);
    }

    inline std::optional<CellView> operator()(ListView list, uint64_t index) const {
        if (index >= list.size()) {
            return std::nullopt;
        }

        return list.elements()[static_cast<size_t>(index)];
    }

    inline std::optional<CellView> operator()(CellView cell, uint64_t index) const {
        if (cell.getTag() != CellTag::ListView) {
            return std::nullopt;
        }

        return operator()(cell.getAs<ListView>(), index);
    }

    inline std::optional<CellView> operator()(ListView /*unused*/, PropertyNull /*unused*/) const {
        return std::nullopt;
    }

    inline std::optional<CellView> operator()(CellView /*unused*/, PropertyNull /*unused*/) const {
        return std::nullopt;
    }

    // Unused but defined to satisfy dispatcher
    std::optional<CellView> operator()(int64_t /*unused*/, ListView /*unused*/) const {
        throw TuringException("Index operands the wrong way round.");
    }
    std::optional<CellView> operator()(int64_t /*unused*/, CellView /*unused*/) const {
        throw TuringException("Index operands the wrong way round.");
    }
    std::optional<CellView> operator()(PropertyNull /*unused*/, ListView /*unused*/) const {
        throw TuringException("Index operands the wrong way round.");
    }
    std::optional<CellView> operator()(PropertyNull /*unused*/, CellView /*unused*/) const {
        throw TuringException("Index operands the wrong way round.");
    }
    std::optional<CellView> operator()(uint64_t /*unused*/, ListView /*unused*/) const {
        throw TuringException("Index operands the wrong way round.");
    }
    std::optional<CellView> operator()(uint64_t /*unused*/, CellView /*unused*/) const {
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
        const std::optional<CellView> cell = ListIndexImpl {}(base, index);

        if (!cell.has_value() || cell->getTag() != TypeToCellTag<T>::Tag) {
            return std::nullopt;
        }

        return cell->getAs<T>();
    }
};

}

using Add = BinaryOp<std::plus<>>;
using Sub = BinaryOp<Subtract>;
using Mul = BinaryOp<std::multiplies<>>;
using Div = BinaryOp<SafeDivides>;
using Mod = BinaryOp<SafeModulo>;
using Pow = BinaryOp<Power>;
using Concat = Concatenate;
using ListIndex = BinaryOp<ListIndexImpl, /*NarrowsUnsigned=*/false>;

template <typename T>
using ValueListIndex = BinaryOp<ValueListIndexImpl<T>, /*NarrowsUnsigned=*/false>;

}


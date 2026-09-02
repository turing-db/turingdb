#pragma once

#include <algorithm>
#include <concepts>
#include <functional>
#include <type_traits>
#include <utility>

#include "ID.h"
#include "TypeUtils.h"
#include "ColumnMask.h"
#include "ColumnConst.h"
#include "list/ListElementOrder.h"
#include "metadata/PropertyType.h"

#include "TuringException.h"

namespace db {


namespace {

struct TuringEqual;
struct TuringNotEqual;

// Static storage duration sentinels to avoid reconstructing each iteration
static constexpr CustomBool sentinelFalse(false);
static constexpr CustomBool sentinelTrue(true);

template <typename T>
concept BooleanOpt = std::same_as<TypeUtils::unwrap_optional_t<T>, types::Bool::Primitive>
                  || std::same_as<ColumnMask::Bool_t, T>;

template <typename F>
concept TestsEquality =
    (std::is_same_v<F, std::equal_to<>> || std::is_same_v<F, std::not_equal_to<>>
     || std::is_same_v<F, TuringEqual> || std::is_same_v<F, TuringNotEqual>);

// The following Boolean operators have unique semantics for 3-way logic (i.e.
// short-circuiting) so are defined explicitly rather than generically
template <BooleanOpt T, BooleanOpt U>
inline std::optional<bool> optionalOr(const T& a, const U& b) {
    if (a == sentinelTrue || b == sentinelTrue) {
        return true;
    }
    if (a == sentinelFalse && b == sentinelFalse) {
        return false;
    }
    return std::nullopt;
}

template <BooleanOpt T, BooleanOpt U>
inline std::optional<bool> optionalAnd(const T& a, const U& b) {
    if (a == sentinelTrue && b == sentinelTrue) {
        return true;
    }
    if (a == sentinelFalse || b == sentinelFalse) {
        return false;
    }
    return std::nullopt;
}

/**
 * @brief Generic function to apply a generic predicate to two possibly-optional
 * operands, where either operand being nullopt results in the final result being
 * nullopt, and the result of applying the predicate otherwise.
 */
template <typename Pred, typename T, typename U>
    requires OptionalPredicate<Pred, T, U>
inline auto optionalPredicate(T&& a,
                              U&& b) -> TypeUtils::optional_invoke_result<Pred, T, U> {
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

    return Pred {}(av, bv);
}

/**
 * @brief Wrapper of overloads of @ref apply functions for different combinations of
 * operand shapes and outputs for executing predicates.
 * @detail The role of this aggregate is to define once the logic for each possible
 * combination of operand and result columns. It is not concerned with the internal types
 * of its arguments.
 */
template <typename Op, typename T, typename U>
struct BinaryPredicateExecutor {
    static void apply(ColumnMask* res,
                      const ColumnVector<T>* lhs,
                      const ColumnVector<U>* rhs) {
        bioassert(lhs->size() == rhs->size(), "Misshapen ColumnVectors.");
        const size_t size = lhs->size();
        res->resize(size);

        const auto& lhsd = lhs->getRaw();
        const auto& rhsd = rhs->getRaw();
        auto& resd = res->getRaw();

        auto op = Op {};
        for (size_t i = 0; i < size; i++) {
            resd[i] = op(lhsd[i], rhsd[i]);
        }
    }

    static void apply(ColumnOptMask* res,
                      const ColumnVector<T>* lhs,
                      const ColumnVector<U>* rhs) {
        bioassert(lhs->size() == rhs->size(), "Misshapen ColumnVectors.");
        const size_t size = lhs->size();
        res->resize(size);

        const auto& lhsd = lhs->getRaw();
        const auto& rhsd = rhs->getRaw();
        auto& resd = res->getRaw();

        auto op = Op {};
        for (size_t i = 0; i < size; i++) {
            resd[i] = op(lhsd[i], rhsd[i]);
        }
    }

    static void apply(ColumnMask* res,
                      const ColumnVector<T>* lhs,
                      const ColumnConst<U>* rhs) {
       const size_t size = lhs->size();

       res->resize(size);
       auto& resd = res->getRaw();
       const auto& lhsd = lhs->getRaw();
       const auto& val = rhs->getRaw();

       auto op = Op {};
       for (size_t i = 0; i < size; i++) {
           resd[i] = op(lhsd[i], val);
       }
    }

    static void apply(ColumnOptMask* res,
                      const ColumnVector<T>* lhs,
                      const ColumnConst<U>* rhs) {
        const size_t size = lhs->size();

        res->resize(size);
        auto& resd = res->getRaw();
        const auto& lhsd = lhs->getRaw();
        const auto& val = rhs->getRaw();

        auto op = Op {};
        for (size_t i = 0; i < size; i++) {
            resd[i] = op(lhsd[i], val);
        }
    }

    static void apply(ColumnMask* res,
                      const ColumnConst<T>* lhs,
                      const ColumnVector<U>* rhs) {
        const size_t size = rhs->size();

        res->resize(size);
        auto& resd = res->getRaw();
        const auto& val = lhs->getRaw();
        const auto& rhsd = rhs->getRaw();

        auto op = Op {};
        for (size_t i = 0; i < size; i++) {
            resd[i] = op(val, rhsd[i]);
        }
    }

    static void apply(ColumnOptMask* res,
                      const ColumnConst<T>* lhs,
                      const ColumnVector<U>* rhs) {
        const size_t size = rhs->size();

        res->resize(size);
        auto& resd = res->getRaw();
        const auto& val = lhs->getRaw();
        const auto& rhsd = rhs->getRaw();

        auto op = Op {};
        for (size_t i = 0; i < size; i++) {
            resd[i] = op(val, rhsd[i]);
        }
    }

    // Special case for e.g. WHERE 1 = 1
    static void apply(ColumnConst<CustomBool>* res,
                      const ColumnConst<T>* lhs,
                      const ColumnConst<U>* rhs) {
        auto op = Op {};
        const CustomBool val = CustomBool {op(lhs->getRaw(), rhs->getRaw())};
        res->set(val);
    }

    static void apply(ColumnMask* res,
                      const ColumnConst<T>* lhs,
                      const ColumnConst<U>* rhs) {
        res->resize(1);
        auto op = Op {};
        const auto& result = op(lhs->getRaw(), rhs->getRaw());
        res->front() = result;
    }

    static void apply(ColumnOptMask* res,
                      const ColumnConst<T>* lhs,
                      const ColumnConst<U>* rhs) {
        res->resize(1);
        auto op = Op {};
        const auto& result = op(lhs->getRaw(), rhs->getRaw());
        res->front() = result;
    }

    static void apply(ColumnMask* res,
                      const ColumnMask* lhs,
                      const ColumnMask* rhs) {
        bioassert(lhs->size() == rhs->size(), "Misshapen ColumnMasks.");
        const size_t size = lhs->size();

        res->resize(size);
        auto& resd = res->getRaw();
        const auto& lhsd = lhs->getRaw();
        const auto& rhsd = rhs->getRaw();

        auto op = Op {};
        for (size_t i = 0; i < size; i++) {
            resd[i] = op(lhsd[i], rhsd[i]);
       }
    }

    static void apply(ColumnOptMask* res,
                      const ColumnOptMask* lhs,
                      const ColumnMask* rhs) {
        bioassert(lhs->size() == rhs->size(), "Misshapen ColumnMasks.");
        const size_t size = lhs->size();

        res->resize(size);
        auto& resd = res->getRaw();
        const auto& lhsd = lhs->getRaw();
        const auto& rhsd = rhs->getRaw();

        auto op = Op {};
        for (size_t i = 0; i < size; i++) {
            resd[i] = op(lhsd[i], rhsd[i]);
       }
    }

    static void apply(ColumnOptMask* res,
                      const ColumnMask* lhs,
                      const ColumnOptMask* rhs) {
        bioassert(lhs->size() == rhs->size(), "Misshapen ColumnMasks.");
        const size_t size = lhs->size();

        res->resize(size);
        auto& resd = res->getRaw();
        const auto& lhsd = lhs->getRaw();
        const auto& rhsd = rhs->getRaw();

        auto op = Op {};
        for (size_t i = 0; i < size; i++) {
            resd[i] = op(lhsd[i], rhsd[i]);
       }
    }

    /// Specialisations when filtering IDs by literals, e.g. n = 1
    static void apply(ColumnVector<CustomBool>* res,
                      const ColumnVector<CustomBool>* lhs,
                      const ColumnMask* rhs) {
        bioassert(lhs->size() == rhs->size(), "Misshapen ColumnMasks.");

        const size_t size = lhs->size();

        res->resize(size);
        auto& resd = res->getRaw();
        const auto& lhsd = lhs->getRaw();
        const auto& rhsd = rhs->getRaw();

        auto op = Op {};
        for (size_t i = 0; i < size; i++) {
            resd[i] = op(lhsd[i], rhsd[i]);
       }
    }

    static void apply(ColumnVector<CustomBool>* res,
                      const ColumnMask* lhs,
                      const ColumnVector<CustomBool>* rhs) {
        bioassert(lhs->size() == rhs->size(), "Misshapen ColumnMasks.");

        const size_t size = lhs->size();

        res->resize(size);
        auto& resd = res->getRaw();
        const auto& lhsd = lhs->getRaw();
        const auto& rhsd = rhs->getRaw();

        auto op = Op {};
        for (size_t i = 0; i < size; i++) {
            resd[i] = op(lhsd[i], rhsd[i]);
       }
    }

    static void apply(ColumnMask* res,
                      const ColumnConst<CustomBool>* lhs,
                      const ColumnMask* rhs) {
        const size_t size = rhs->size();

        res->resize(size);
        auto& resd = res->getRaw();
        const CustomBool& val = lhs->getRaw();
        const auto& rhsd = rhs->getRaw();

        auto op = Op {};
        for (size_t i = 0; i < size; i++) {
            resd[i] = op(val, rhsd[i]);
        }
    }

    static void apply(ColumnMask* res,
                      const ColumnMask* lhs,
                      const ColumnConst<CustomBool>* rhs) {
        const size_t size = lhs->size();

        res->resize(size);
        auto& resd = res->getRaw();
        const auto& lhsd = lhs->getRaw();
        const CustomBool& val = rhs->getRaw();

        auto op = Op {};
        for (size_t i = 0; i < size; i++) {
            resd[i] = op(lhsd[i], val);
        }
    }

    static void apply(ColumnOptMask* res,
                      const ColumnMask* lhs,
                      const ColumnConst<U>* rhs) {
        const size_t size = lhs->size();

        res->resize(size);
        auto& resd = res->getRaw();
        const auto& lhsd = lhs->getRaw();
        const auto& val = rhs->getRaw();

        auto op = Op {};
        for (size_t i = 0; i < size; i++) {
            resd[i] = op(lhsd[i], val);
        }
    }

    static void apply(ColumnOptMask* res,
                      const ColumnConst<T>* lhs,
                      const ColumnMask* rhs) {
        const size_t size = rhs->size();

        res->resize(size);
        auto& resd = res->getRaw();
        const auto& val = lhs->getRaw();
        const auto& rhsd = rhs->getRaw();

        auto op = Op {};
        for (size_t i = 0; i < size; i++) {
            resd[i] = op(val, rhsd[i]);
        }
    }

};

/**
 * @brief Thin wrapper over a provided functor @param F to dispatch optional logic
 * accordingly.
 * @detail Short-circuiting Boolean operations are not handled as a generic predicate as
 * they requires special short-circuiting logic.
 */
template <typename F>
struct BinaryPredicate {
    // Handle optional cases
    template<typename T, typename U>
        requires (TypeUtils::is_optional_v<T> || TypeUtils::is_optional_v<U>)
    inline std::optional<CustomBool> operator()(T&& a, U&& b) {
        // Short-circuiting implementations for AND and OR
        if constexpr (std::is_same_v<F, std::logical_or<>>) {
            return optionalOr(std::forward<T>(a), std::forward<U>(b));
        } else if constexpr (std::is_same_v<F, std::logical_and<>>) {
            return optionalAnd(std::forward<T>(a), std::forward<U>(b));
        } else { // General implementation for >, <, <=, etc
            return optionalPredicate<F>(std::forward<T>(a), std::forward<U>(b));
        }
    }
    
    // Handle non-optional cases
    template <typename T, typename U>
    inline ColumnMask::Bool_t operator()(T&& a, U&& b) {
        return F{}(std::forward<T>(a), std::forward<U>(b));
    }
    
    // Specialisation for IS NOT NULL and IS NULL
    template <typename T>
    requires(TypeUtils::is_optional_v<T> && TestsEquality<F>)
    inline ColumnMask::Bool_t operator()(T&& a, const PropertyNull& null) {
        return F{}(std::forward<T>(a), null);
    }
};

struct TuringEqual {
    bool operator()(const types::Embedding::Primitive& a, const types::Embedding::Primitive& b) {
        const bool equal =
            (a.size() == b.size()) && std::equal(a.begin(), a.end(), b.begin());
        return equal;
    }

    /// Specialisations to allow for ID <-> int, e.g  (n) = 42, (e) = n.age, etc.
    template <TypedInternalID IDT>
    bool operator()(IDT n, types::Int64::Primitive i) {
        if (i < 0) {
            throw TuringException("Cannot compare ID with negative integer.");
        }
        const types::UInt64::Primitive id = i;
        return n.getValue() == id;
    }

    template <TypedInternalID IDT>
    bool operator()(types::Int64::Primitive i, IDT n) {
        if (i < 0) {
            throw TuringException("compare ID with negative integer.");
        }
        const types::UInt64::Primitive id = i;
        return n.getValue() == id;
    }

    // Generalist fallback for all other types
    template <typename T, typename U>
    bool operator()(const T& a, const U& b) {
        return std::equal_to<> {}(a, b);
    }
};

struct TuringNotEqual {
    template <typename T, typename U>
    bool operator()(T&& a, U&& b) {
        return !TuringEqual {}(std::forward<T>(a), std::forward<U>(b));
    }
};

struct TuringXor {
    bool operator()(bool a, bool b) {
        return a ^ b;
    } 

    bool operator()(CustomBool a, CustomBool b) {
        return a._boolean ^ b._boolean;
    }

    bool operator()(ColumnMask::Bool_t a, ColumnMask::Bool_t b) {
        return a._value ^ b._value; 
    }
};

// The characters a string predicate reads out of one operand. A type-erased cell holds
// them only when it is tagged as a string: one holding a number, a nested list or a null
// has none, so no string predicate can match it.
inline std::optional<types::String::Primitive> predicateText(const types::String::Primitive value) {
    return value;
}

inline std::optional<types::String::Primitive> predicateText(const types::String::OwningPrimitive& value) {
    return value;
}

inline std::optional<types::String::Primitive> predicateText(const ListElementView element) {
    if (element.getTag() != ListBufferTypeTag::String) {
        return std::nullopt;
    }

    return element.getAs<types::String::Primitive>();
}

/**
 * @brief Applies a string test @param F to the characters each operand holds, whichever of
 * the string column kinds - or type-erased cells - the operands are.
 */
template <typename F>
struct StringPredicate {
    template <typename T, typename U>
    bool operator()(const T& text, const U& pattern) const {
        const std::optional<types::String::Primitive> textView = predicateText(text);
        const std::optional<types::String::Primitive> patternView = predicateText(pattern);

        if (!textView || !patternView) {
            return false;
        }

        return F {}(*textView, *patternView);
    }
};

struct StringStartsWith {
    bool operator()(const types::String::Primitive text, const types::String::Primitive prefix) const {
        return text.starts_with(prefix);
    }
};

struct StringEndsWith {
    bool operator()(const types::String::Primitive text, const types::String::Primitive suffix) const {
        return text.ends_with(suffix);
    }
};

struct StringContains {
    bool operator()(const types::String::Primitive text, const types::String::Primitive pattern) const {
        return text.find(pattern) != types::String::Primitive::npos;
    }
};

}

using Eq = BinaryPredicate<TuringEqual>;
using Ne = BinaryPredicate<TuringNotEqual>;

using Gt = BinaryPredicate<std::greater<>>;
using Lt = BinaryPredicate<std::less<>>;

using Gte = BinaryPredicate<std::greater_equal<>>;
using Lte = BinaryPredicate<std::less_equal<>>;

using And = BinaryPredicate<std::logical_and<>>;
using Or = BinaryPredicate<std::logical_or<>>;
using Xor = BinaryPredicate<TuringXor>;

using StartsWith = BinaryPredicate<StringPredicate<StringStartsWith>>;
using EndsWith = BinaryPredicate<StringPredicate<StringEndsWith>>;
using Contains = BinaryPredicate<StringPredicate<StringContains>>;

}

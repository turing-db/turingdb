#pragma once

#include <math.h>
#include <stdint.h>

#include <compare>
#include <concepts>
#include <limits>
#include <utility>

namespace db {

template <typename T>
concept Number = std::same_as<T, int64_t> || std::same_as<T, uint64_t> || std::same_as<T, double>;

template <std::integral Integer>
inline constexpr double INTEGER_LOWER_BOUND = static_cast<double>(std::numeric_limits<Integer>::min());

template <std::integral Integer>
inline constexpr double INTEGER_UPPER_BOUND = static_cast<double>(std::numeric_limits<Integer>::max() / 2 + 1) * 2.0;

// Orders an integer against a double by their exact values. Converting the integer to
// double instead rounds it past 2^53, so 2^53 + 1 would compare equal to 2^53.0.
template <std::integral Integer>
inline std::partial_ordering integerOrderAgainstDouble(const Integer integer, const double value) {
    if (std::isnan(value)) {
        return std::partial_ordering::unordered;
    } else if (value < INTEGER_LOWER_BOUND<Integer>) {
        return std::partial_ordering::greater;
    } else if (value >= INTEGER_UPPER_BOUND<Integer>) {
        return std::partial_ordering::less;
    }

    const double truncated = std::trunc(value);
    const Integer whole = static_cast<Integer>(truncated);

    if (integer != whole) {
        return integer <=> whole;
    }

    return truncated <=> value;
}

// Orders two numbers by their exact values, whatever their types. A NaN is unordered
// against every number.
template <Number Lhs, Number Rhs>
inline std::partial_ordering numberOrder(const Lhs lhs, const Rhs rhs) {
    if constexpr (std::integral<Lhs> && std::integral<Rhs>) {
        if (std::cmp_less(lhs, rhs)) {
            return std::partial_ordering::less;
        } else if (std::cmp_less(rhs, lhs)) {
            return std::partial_ordering::greater;
        }

        return std::partial_ordering::equivalent;
    } else if constexpr (std::integral<Lhs>) {
        return integerOrderAgainstDouble(lhs, rhs);
    } else if constexpr (std::integral<Rhs>) {
        return 0 <=> integerOrderAgainstDouble(rhs, lhs);
    } else {
        return lhs <=> rhs;
    }
}

template <Number Lhs, Number Rhs>
inline bool numbersEqual(const Lhs lhs, const Rhs rhs) {
    return numberOrder(lhs, rhs) == std::partial_ordering::equivalent;
}

// Converts a number to the type of @param converted when that type holds its exact value,
// as the integer 32 holds 32.0. No integer holds 32.5, and no double holds 2^53 + 1.
template <Number Source, Number Target>
inline bool convertNumberExactly(const Source number, Target& converted) {
    if constexpr (std::integral<Source> && std::integral<Target>) {
        if (!std::in_range<Target>(number)) {
            return false;
        }

        converted = static_cast<Target>(number);
        return true;
    } else if constexpr (std::integral<Target>) {
        const bool inRange = number >= INTEGER_LOWER_BOUND<Target> && number < INTEGER_UPPER_BOUND<Target>;
        if (!inRange || std::trunc(number) != number) {
            return false;
        }

        converted = static_cast<Target>(number);
        return true;
    } else if constexpr (std::integral<Source>) {
        converted = static_cast<Target>(number);
        return numbersEqual(number, converted);
    } else {
        converted = number;
        return true;
    }
}

}

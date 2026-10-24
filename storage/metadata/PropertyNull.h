#pragma once

#include <optional>

namespace db {

/**
 * @brief Default-initialisable wrapper for std::nullopt_t, for use in @ref
 * ColumnConst<PropertyNull>, required for IS NULL or IS NOT NULL operations.
 */
struct PropertyNull {
    constexpr PropertyNull() noexcept = default;
    constexpr operator std::nullopt_t() const noexcept { return std::nullopt; }
};

// An optional is null where it holds nothing, or where what it holds is a null of its own:
// a type-erased cell carries its null in its tag
template <typename T>
constexpr inline bool operator==(const std::optional<T>& o, const PropertyNull& null) noexcept {
    return o == std::nullopt || *o == null;
}

template <typename T>
constexpr inline bool operator==(const PropertyNull& null, const std::optional<T>& o) noexcept {
    return o == null;
}

template <typename T>
constexpr inline bool operator!=(const PropertyNull& null, const std::optional<T>& o) noexcept {
    return !(o == null);
}

template <typename T>
constexpr inline bool operator!=(const std::optional<T>& o, const PropertyNull& null) noexcept {
    return !(o == null);
}

// Any non-optional is not equal to PropertyNull
template <typename T>
constexpr inline bool operator==(const PropertyNull&, const T&) noexcept {
    return false;
}

template <typename T>
constexpr inline bool operator==(const T&, const PropertyNull&) noexcept {
    return false;
}

}

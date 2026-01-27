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

template <typename T>
constexpr inline bool operator==(const PropertyNull&, const std::optional<T>&) noexcept {
    return false;
}

template <typename T>
constexpr inline bool operator==(const std::optional<T>&, const PropertyNull&) noexcept {
    return false;
}

template <typename T>
constexpr inline bool operator!=(const PropertyNull&, const std::optional<T>&) noexcept {
    return true;
}

template <typename T>
constexpr inline bool operator!=(const std::optional<T>&, const PropertyNull&) noexcept {
    return true;
}

template <typename T>
constexpr inline bool operator==(const PropertyNull&, const T&) noexcept {
    return false;
}

template <typename T>
constexpr inline bool operator==(const T&, const PropertyNull&) noexcept {
    return false;
}

}

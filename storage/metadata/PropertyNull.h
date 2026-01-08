#pragma once

#include <optional>

/**
 * @brief Default-initialisable wrapper for std::nullopt_t, for use in @ref
 * ColumnConst<PropertyNull>, required for IS NULL or IS NOT NULL operations.
 */
struct PropertyNull {
    consteval PropertyNull() noexcept = default;
    consteval operator std::nullopt_t() const noexcept { return std::nullopt; }
};

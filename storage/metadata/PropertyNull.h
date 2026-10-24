#pragma once

#include <optional>

namespace db {

/**
 * @brief Default-initialisable wrapper for std::nullopt_t, for use in @ref
 * ColumnConst<PropertyNull>, the column of the null literal.
 */
struct PropertyNull {
    constexpr PropertyNull() noexcept = default;
    constexpr operator std::nullopt_t() const noexcept { return std::nullopt; }
};

}

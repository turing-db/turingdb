#pragma once

#include <optional>

struct PropertyNull {
    static constexpr std::optional<int> _value {std::nullopt};
};

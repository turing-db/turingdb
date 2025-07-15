#pragma once

#include <cstdint>
#include <optional>
#include <string>
#include <variant>

namespace db {

class Literal {
public:
    using ValueType = std::variant<bool, int64_t, double, std::string, char, std::nullopt_t>;

    Literal() = default;

    explicit Literal(ValueType&& vt)
        : value(std::move(vt)) {}

private:
    ValueType value;
};

}

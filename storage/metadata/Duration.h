#pragma once

#include <stdint.h>

#include <compare>
#include <functional>
#include <string>

namespace db {

// A signed length of time, counted in microseconds.
class Duration {
public:
    Duration() = default;

    constexpr explicit Duration(int64_t microseconds)
        : _microseconds(microseconds)
    {
    }

    int64_t getMicroseconds() const { return _microseconds; }

    std::strong_ordering operator<=>(const Duration& other) const {
        return _microseconds <=> other._microseconds;
    }

    bool operator==(const Duration& other) const {
        return _microseconds == other._microseconds;
    }

    static void format(std::string& out, Duration value);

private:
    int64_t _microseconds;
};

}

template <>
struct std::hash<db::Duration> {
    size_t operator()(const db::Duration& value) const noexcept {
        return std::hash<int64_t> {}(value.getMicroseconds());
    }
};

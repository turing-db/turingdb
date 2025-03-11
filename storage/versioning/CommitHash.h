#pragma once

#include <spdlog/fmt/ostr.h>
#include <cstdint>
#include <limits>
#include <string>

namespace db {

class CommitHash {
public:
    constexpr CommitHash() = default;
    constexpr ~CommitHash() = default;

    constexpr CommitHash(const CommitHash&) = default;
    constexpr CommitHash(CommitHash&&) noexcept = default;
    constexpr CommitHash& operator=(const CommitHash&) = default;
    constexpr CommitHash& operator=(CommitHash&&) noexcept = default;

    constexpr explicit CommitHash(uint64_t v)
        : _value(v)
    {
    }

    constexpr CommitHash& operator=(uint64_t v) {
        _value = v;
        return *this;
    }

    [[nodiscard]] constexpr uint64_t get() const { return _value; }
    [[nodiscard]] constexpr explicit operator uint64_t() const { return _value; }

    [[nodiscard]] static CommitHash create();

    [[nodiscard]] static consteval CommitHash head() { return CommitHash {}; }

    [[nodiscard]] bool operator==(const CommitHash& other) const {
        return _value == other._value;
    }

    [[nodiscard]] bool operator!=(const CommitHash& other) const {
        return !(*this == other);
    }

    [[nodiscard]] bool operator<(const CommitHash& other) const {
        return _value < other._value;
    }

    [[nodiscard]] bool operator<=(const CommitHash& other) const {
        return _value <= other._value;
    }

    [[nodiscard]] bool operator>=(const CommitHash& other) const {
        return _value >= other._value;
    }

    [[nodiscard]] bool operator>(const CommitHash& other) const {
        return _value > other._value;
    }

private:
    uint64_t _value = std::numeric_limits<uint64_t>::max();
};

}

template <>
struct std::hash<db::CommitHash> {
    size_t operator()(const db::CommitHash& h) const {
        return h.get();
    }
};

namespace std {

inline string to_string(db::CommitHash h) {
    return to_string(h.get());
}

template <typename T>
ostream& operator<<(ostream& os, db::CommitHash h) {
    return os << h.get();
}

}

template <>
struct fmt::formatter<db::CommitHash> : ostream_formatter {};



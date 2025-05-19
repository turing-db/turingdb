#pragma once

#include <charconv>
#include <spdlog/fmt/ostr.h>
#include <cstdint>
#include <limits>
#include <string>

#include "BasicResult.h"

namespace db {

template <int = 0>
class TemplateCommitHash {
public:
    using ValueType = uint64_t;

    constexpr TemplateCommitHash() = default;
    constexpr ~TemplateCommitHash() = default;

    constexpr TemplateCommitHash(const TemplateCommitHash&) = default;
    constexpr TemplateCommitHash(TemplateCommitHash&&) noexcept = default;
    constexpr TemplateCommitHash& operator=(const TemplateCommitHash&) = default;
    constexpr TemplateCommitHash& operator=(TemplateCommitHash&&) noexcept = default;

    constexpr explicit TemplateCommitHash(ValueType v)
        : _value(v)
    {
    }

    constexpr TemplateCommitHash& operator=(ValueType v) {
        _value = v;
        return *this;
    }

    [[nodiscard]] constexpr ValueType get() const { return _value; }
    [[nodiscard]] constexpr explicit operator ValueType() const { return _value; }

    [[nodiscard]] static TemplateCommitHash create();

    [[nodiscard]] static consteval TemplateCommitHash head() { return TemplateCommitHash {}; }

    [[nodiscard]] bool operator==(const TemplateCommitHash& other) const {
        return _value == other._value;
    }

    [[nodiscard]] bool operator!=(const TemplateCommitHash& other) const {
        return !(*this == other);
    }

    [[nodiscard]] bool operator<(const TemplateCommitHash& other) const {
        return _value < other._value;
    }

    [[nodiscard]] bool operator<=(const TemplateCommitHash& other) const {
        return _value <= other._value;
    }

    [[nodiscard]] bool operator>=(const TemplateCommitHash& other) const {
        return _value >= other._value;
    }

    [[nodiscard]] bool operator>(const TemplateCommitHash& other) const {
        return _value > other._value;
    }

    [[nodiscard]] static BasicResult<TemplateCommitHash, std::string_view> fromString(std::string_view str) {
        TemplateCommitHash::ValueType hashValue = TemplateCommitHash::head().get();
        if (str == "head") {
            return TemplateCommitHash::head();
        }

        const char* begin = str.data();
        const char* end = str.data() + str.size();
        const auto res = std::from_chars(begin, end, hashValue, 16);

        if (res.ec == std::errc::result_out_of_range) {
            return BadResult<std::string_view>("Too large hash value");
        } else if (res.ec == std::errc::invalid_argument) {
            return BadResult<std::string_view>("Invalid hash value");
        } else if (res.ptr != end) {
            return BadResult<std::string_view>("String contains invalid characters");
        }

        return TemplateCommitHash(hashValue);
    }

private:
    ValueType _value = std::numeric_limits<ValueType>::max();
};

using CommitHash = TemplateCommitHash<0>;

}

template <int i>
struct std::hash<db::TemplateCommitHash<i>> {
    size_t operator()(const db::TemplateCommitHash<i>& h) const {
        return h.get();
    }
};

namespace std {

template <int i>
inline string to_string(db::TemplateCommitHash<i> h) {
    return to_string(h.get());
}

template <typename T, int i>
ostream& operator<<(ostream& os, db::TemplateCommitHash<i> h) {
    return os << h.get();
}

}

template <int i>
struct fmt::formatter<db::TemplateCommitHash<i>> : ostream_formatter {};

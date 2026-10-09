#pragma once

#include <array>
#include <charconv>
#include <stdint.h>
#include <limits>
#include <string>

#include <spdlog/fmt/ostr.h>

#include "BasicResult.h"

namespace db {

template <int = 0, int Radix = 16>
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
        const auto res = std::from_chars(begin, end, hashValue, Radix);

        if (res.ec == std::errc::result_out_of_range) {
            return BadResult<std::string_view>("Too large hash value");
        } else if (res.ec == std::errc::invalid_argument) {
            return BadResult<std::string_view>("Invalid hash value");
        } else if (res.ptr != end) {
            return BadResult<std::string_view>("String contains invalid characters");
        }

        return TemplateCommitHash(hashValue);
    }

    void appendString(std::string& out) const {
        if (*this == head()) {
            out += "head";
            return;
        }

        std::array<char, std::numeric_limits<ValueType>::digits> buffer;
        const auto res = std::to_chars(buffer.data(), buffer.data() + buffer.size(), _value, Radix);
        out.append(buffer.data(), res.ptr);
    }

    bool isValid() { return _value == 0; }

private:
    ValueType _value {std::numeric_limits<ValueType>::max()};
};

using CommitHash = TemplateCommitHash<0>;

}

template <int i, int radix>
struct std::hash<db::TemplateCommitHash<i, radix>> {
    size_t operator()(const db::TemplateCommitHash<i, radix>& h) const {
        return h.get();
    }
};

namespace db {

template <int i, int radix>
std::ostream& operator<<(std::ostream& os, TemplateCommitHash<i, radix> hash) {
    std::string text;
    hash.appendString(text);
    return os << text;
}

}

namespace std {

template <int i, int radix>
inline string to_string(db::TemplateCommitHash<i, radix> hash) {
    string text;
    hash.appendString(text);
    return text;
}

}

template <int i, int radix>
struct fmt::formatter<db::TemplateCommitHash<i, radix>> : fmt::formatter<std::string_view> {
    auto format(db::TemplateCommitHash<i, radix> hash, fmt::format_context& context) const {
        std::string text;
        hash.appendString(text);
        return fmt::formatter<std::string_view>::format(text, context);
    }
};

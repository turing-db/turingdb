#pragma once

#include <stdint.h>

#include <compare>
#include <functional>
#include <optional>
#include <string>
#include <string_view>

namespace db {

// A calendar field of an instant. Millisecond and Microsecond name the sub-second part
// truncated to that precision, so an instant .123456 of a second in has a Millisecond of
// 123 and a Microsecond of 123456.
enum class DateTimePart : uint8_t {
    Year,
    Month,
    Day,
    Hour,
    Minute,
    Second,
    Millisecond,
    Microsecond,
};

// An instant on the UTC timeline, counted in microseconds from the Unix epoch.
class DateTime {
public:
    DateTime() = default;

    constexpr explicit DateTime(int64_t microseconds)
        : _microseconds(microseconds)
    {
    }

    int64_t getMicroseconds() const { return _microseconds; }

    std::strong_ordering operator<=>(const DateTime& other) const {
        return _microseconds <=> other._microseconds;
    }

    bool operator==(const DateTime& other) const {
        return _microseconds == other._microseconds;
    }

    // An ISO-8601 date, or date and time, read as UTC. A trailing offset names which instant
    // the text stands for and is then spent: one count of microseconds cannot carry a zone,
    // so the value is the instant and reads back as UTC.
    static std::optional<DateTime> parse(std::string_view text);

    static DateTime now();

    // A count of seconds since the Unix epoch, as datetime(<integer>) reads one. False
    // where the count names an instant format cannot spell, which is also what keeps the
    // scaling to microseconds from overflowing.
    static bool fromEpochSeconds(int64_t seconds, DateTime& value);

    static int64_t component(DateTime value, DateTimePart part);

    static void format(std::string& out, DateTime value);

    // Whether format spells this instant as a value parse reads back. A year needs four
    // digits on both sides, and std::chrono leaves the calendar conversion unspecified
    // well before an int64 count of microseconds runs out, so an instant arriving from
    // outside - an import - is checked against this rather than rendered blind.
    static bool isRenderable(DateTime value);

private:
    // No initializer, as CustomBool's bool has none: a default member initializer makes the
    // default constructor non-trivial, and the datapart loader reads a trivial property's
    // values straight out of the mapped page through fs::TrivialPrimitive
    int64_t _microseconds;
};

}

template <>
struct std::hash<db::DateTime> {
    size_t operator()(const db::DateTime& value) const noexcept {
        return std::hash<int64_t> {}(value.getMicroseconds());
    }
};

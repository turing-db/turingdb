#pragma once

#include <stdint.h>

#include <compare>
#include <functional>
#include <string>

namespace db {

// A unit of a duration, read as the whole count of that unit it spans. A month is the
// average Gregorian month of 30.436875 days, so a year is 365.2425 days. The ...Of...
// parts are what is left of that count past the whole next larger unit, with the sign of
// the duration.
enum class DurationPart : uint8_t {
    Years,
    Quarters,
    Months,
    Weeks,
    Days,
    Hours,
    Minutes,
    Seconds,
    Milliseconds,
    Microseconds,
    QuartersOfYear,
    MonthsOfYear,
    MonthsOfQuarter,
    DaysOfWeek,
    MinutesOfHour,
    SecondsOfMinute,
    MillisecondsOfSecond,
    MicrosecondsOfSecond,
};

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

    static int64_t component(Duration value, DurationPart part);

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

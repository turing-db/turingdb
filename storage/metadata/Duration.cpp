#include "Duration.h"

#include <iterator>

#include <spdlog/fmt/fmt.h>

using namespace db;

namespace {

constexpr int64_t MICROSECONDS_PER_SECOND = 1000000;
constexpr int64_t SECONDS_PER_MINUTE = 60;
constexpr int64_t SECONDS_PER_HOUR = 3600;

constexpr int64_t MICROSECONDS_PER_MILLISECOND = 1000;
constexpr int64_t MICROSECONDS_PER_MINUTE = 60 * MICROSECONDS_PER_SECOND;
constexpr int64_t MICROSECONDS_PER_HOUR = 60 * MICROSECONDS_PER_MINUTE;
constexpr int64_t MICROSECONDS_PER_DAY = 24 * MICROSECONDS_PER_HOUR;
constexpr int64_t MICROSECONDS_PER_WEEK = 7 * MICROSECONDS_PER_DAY;
constexpr int64_t MICROSECONDS_PER_MONTH = 2629746 * MICROSECONDS_PER_SECOND;
constexpr int64_t MICROSECONDS_PER_QUARTER = 3 * MICROSECONDS_PER_MONTH;
constexpr int64_t MICROSECONDS_PER_YEAR = 12 * MICROSECONDS_PER_MONTH;

}

int64_t Duration::component(Duration value, DurationPart part) {
    const int64_t microseconds = value.getMicroseconds();

    switch (part) {
        case DurationPart::Years:
            return microseconds / MICROSECONDS_PER_YEAR;
        break;

        case DurationPart::Quarters:
            return microseconds / MICROSECONDS_PER_QUARTER;
        break;

        case DurationPart::Months:
            return microseconds / MICROSECONDS_PER_MONTH;
        break;

        case DurationPart::Weeks:
            return microseconds / MICROSECONDS_PER_WEEK;
        break;

        case DurationPart::Days:
            return microseconds / MICROSECONDS_PER_DAY;
        break;

        case DurationPart::Hours:
            return microseconds / MICROSECONDS_PER_HOUR;
        break;

        case DurationPart::Minutes:
            return microseconds / MICROSECONDS_PER_MINUTE;
        break;

        case DurationPart::Seconds:
            return microseconds / MICROSECONDS_PER_SECOND;
        break;

        case DurationPart::Milliseconds:
            return microseconds / MICROSECONDS_PER_MILLISECOND;
        break;

        case DurationPart::Microseconds:
            return microseconds;
        break;

        case DurationPart::QuartersOfYear:
            return component(value, DurationPart::Quarters) % 4;
        break;

        case DurationPart::MonthsOfYear:
            return component(value, DurationPart::Months) % 12;
        break;

        case DurationPart::MonthsOfQuarter:
            return component(value, DurationPart::Months) % 3;
        break;

        case DurationPart::DaysOfWeek:
            return component(value, DurationPart::Days) % 7;
        break;

        case DurationPart::MinutesOfHour:
            return component(value, DurationPart::Minutes) % 60;
        break;

        case DurationPart::SecondsOfMinute:
            return component(value, DurationPart::Seconds) % 60;
        break;

        case DurationPart::MillisecondsOfSecond:
            return component(value, DurationPart::Milliseconds) % 1000;
        break;

        case DurationPart::MicrosecondsOfSecond:
            return component(value, DurationPart::Microseconds) % 1000000;
        break;
    }

    return 0;
}

void Duration::format(std::string& out, Duration value) {
    const int64_t microseconds = value.getMicroseconds();
    const bool isNegative = microseconds < 0;

    // The magnitude of INT64_MIN has no int64, so it is taken unsigned
    const uint64_t magnitude = isNegative
                                 ? uint64_t {0} - static_cast<uint64_t>(microseconds)
                                 : static_cast<uint64_t>(microseconds);

    const uint64_t totalSeconds = magnitude / MICROSECONDS_PER_SECOND;
    const uint64_t fraction = magnitude % MICROSECONDS_PER_SECOND;
    const uint64_t hours = totalSeconds / SECONDS_PER_HOUR;
    const uint64_t minutes = (totalSeconds % SECONDS_PER_HOUR) / SECONDS_PER_MINUTE;
    const uint64_t seconds = totalSeconds % SECONDS_PER_MINUTE;

    const std::string_view sign = isNegative ? "-" : "";

    out += "PT";

    if (magnitude == 0) {
        out += "0S";
        return;
    }

    const auto output = std::back_inserter(out);

    if (hours != 0) {
        fmt::format_to(output, "{}{}H", sign, hours);
    }

    if (minutes != 0) {
        fmt::format_to(output, "{}{}M", sign, minutes);
    }

    if (fraction != 0) {
        fmt::format_to(output, "{}{}.{:06}", sign, seconds, fraction);
        out.erase(out.find_last_not_of('0') + 1);
        out += 'S';
    } else if (seconds != 0) {
        fmt::format_to(output, "{}{}S", sign, seconds);
    }
}

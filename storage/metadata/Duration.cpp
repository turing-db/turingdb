#include "Duration.h"

#include <iterator>

#include <spdlog/fmt/fmt.h>

using namespace db;

namespace {

constexpr uint64_t MICROSECONDS_PER_SECOND = 1000000;
constexpr uint64_t SECONDS_PER_MINUTE = 60;
constexpr uint64_t SECONDS_PER_HOUR = 3600;

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

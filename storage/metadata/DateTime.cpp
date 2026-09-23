#include "DateTime.h"

#include <charconv>
#include <chrono>

#include <spdlog/fmt/fmt.h>

using namespace db;

namespace {

constexpr int64_t MICROSECONDS_PER_SECOND = 1000000;
constexpr int64_t SECONDS_PER_MINUTE = 60;
constexpr int64_t SECONDS_PER_HOUR = 3600;
constexpr int64_t SECONDS_PER_DAY = 86400;

constexpr size_t FRACTION_DIGITS = 6;

bool isDigit(char c) {
    return c >= '0' && c <= '9';
}

// std::from_chars accepts a leading sign, and a field of an ISO-8601 timestamp is a fixed
// run of digits, so the digits are checked before the number is read.
bool readDigits(std::string_view text, size_t& offset, size_t count, int& value) {
    if (offset + count > text.size()) {
        return false;
    }

    const char* begin = text.data() + offset;
    const char* end = begin + count;

    for (const char* character = begin; character != end; character++) {
        if (!isDigit(*character)) {
            return false;
        }
    }

    const std::from_chars_result result = std::from_chars(begin, end, value);
    if (result.ec != std::errc {} || result.ptr != end) {
        return false;
    }

    offset += count;
    return true;
}

bool readSeparator(std::string_view text, size_t& offset, char separator) {
    if (offset >= text.size() || text[offset] != separator) {
        return false;
    }

    offset++;
    return true;
}

// The digits after the decimal point, scaled to microseconds. ISO-8601 puts no bound on how
// many a writer may spell, so a value finer than a microsecond is read and truncated rather
// than turned away.
bool readFraction(std::string_view text, size_t& offset, int64_t& microseconds) {
    size_t digit = 0;
    microseconds = 0;

    while (offset < text.size() && isDigit(text[offset])) {
        if (digit < FRACTION_DIGITS) {
            microseconds = microseconds * 10 + (text[offset] - '0');
        }

        digit++;
        offset++;
    }

    if (digit == 0) {
        return false;
    }

    for (size_t missing = digit; missing < FRACTION_DIGITS; missing++) {
        microseconds *= 10;
    }

    return true;
}

bool readOffset(std::string_view text, size_t& offset, int64_t& seconds) {
    seconds = 0;

    if (offset == text.size()) {
        return true;
    }

    const char sign = text[offset];
    if (sign == 'Z' || sign == 'z') {
        offset++;
        return offset == text.size();
    }

    if (sign != '+' && sign != '-') {
        return false;
    }

    offset++;

    int hour = 0;
    if (!readDigits(text, offset, 2, hour) || hour > 23) {
        return false;
    }

    int minute = 0;
    if (offset != text.size()) {
        readSeparator(text, offset, ':');

        if (!readDigits(text, offset, 2, minute) || minute > 59) {
            return false;
        }
    }

    seconds = hour * SECONDS_PER_HOUR + minute * SECONDS_PER_MINUTE;
    if (sign == '-') {
        seconds = -seconds;
    }

    return offset == text.size();
}

}

std::optional<DateTime> DateTime::parse(std::string_view text) {
    size_t offset = 0;

    int year = 0;
    int month = 0;
    int day = 0;

    const bool readDate = readDigits(text, offset, 4, year)
                       && readSeparator(text, offset, '-')
                       && readDigits(text, offset, 2, month)
                       && readSeparator(text, offset, '-')
                       && readDigits(text, offset, 2, day);

    if (!readDate) {
        return std::nullopt;
    }

    const std::chrono::year_month_day date {std::chrono::year {year},
                                            std::chrono::month {static_cast<unsigned>(month)},
                                            std::chrono::day {static_cast<unsigned>(day)}};

    if (!date.ok()) {
        return std::nullopt;
    }

    const int64_t epochDay = std::chrono::sys_days {date}.time_since_epoch().count();

    if (offset == text.size()) {
        return DateTime {epochDay * SECONDS_PER_DAY * MICROSECONDS_PER_SECOND};
    }

    const char dateTimeSeparator = text[offset];
    if (dateTimeSeparator != 'T' && dateTimeSeparator != 't' && dateTimeSeparator != ' ') {
        return std::nullopt;
    }

    offset++;

    int hour = 0;
    int minute = 0;

    const bool readTime = readDigits(text, offset, 2, hour)
                       && readSeparator(text, offset, ':')
                       && readDigits(text, offset, 2, minute);

    if (!readTime || hour > 23 || minute > 59) {
        return std::nullopt;
    }

    int second = 0;
    if (readSeparator(text, offset, ':')) {
        if (!readDigits(text, offset, 2, second) || second > 59) {
            return std::nullopt;
        }
    }

    int64_t fraction = 0;
    if (readSeparator(text, offset, '.') && !readFraction(text, offset, fraction)) {
        return std::nullopt;
    }

    int64_t offsetSeconds = 0;
    if (!readOffset(text, offset, offsetSeconds)) {
        return std::nullopt;
    }

    const int64_t seconds = epochDay * SECONDS_PER_DAY
                          + hour * SECONDS_PER_HOUR
                          + minute * SECONDS_PER_MINUTE
                          + second
                          - offsetSeconds;

    return DateTime {seconds * MICROSECONDS_PER_SECOND + fraction};
}

void DateTime::format(std::string& out, DateTime value) {
    const int64_t microseconds = value.getMicroseconds();

    // An instant before the epoch counts down, but its time of day counts up, so both
    // divisions floor rather than truncate towards zero
    int64_t seconds = microseconds / MICROSECONDS_PER_SECOND;
    int64_t fraction = microseconds % MICROSECONDS_PER_SECOND;
    if (fraction < 0) {
        fraction += MICROSECONDS_PER_SECOND;
        seconds--;
    }

    int64_t epochDay = seconds / SECONDS_PER_DAY;
    int64_t secondOfDay = seconds % SECONDS_PER_DAY;
    if (secondOfDay < 0) {
        secondOfDay += SECONDS_PER_DAY;
        epochDay--;
    }

    const std::chrono::year_month_day date {
        std::chrono::sys_days {std::chrono::days {epochDay}}};

    const int64_t hour = secondOfDay / SECONDS_PER_HOUR;
    const int64_t minute = (secondOfDay % SECONDS_PER_HOUR) / SECONDS_PER_MINUTE;
    const int64_t second = secondOfDay % SECONDS_PER_MINUTE;

    out += fmt::format("{:04}-{:02}-{:02}T{:02}:{:02}:{:02}",
                       static_cast<int>(date.year()),
                       static_cast<unsigned>(date.month()),
                       static_cast<unsigned>(date.day()),
                       hour,
                       minute,
                       second);

    if (fraction != 0) {
        out += fmt::format(".{:06}", fraction);
    }

    out += 'Z';
}

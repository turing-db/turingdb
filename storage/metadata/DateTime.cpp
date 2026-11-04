#include "DateTime.h"

#include <algorithm>
#include <array>
#include <charconv>
#include <chrono>
#include <limits>
#include <string_view>
#include <unordered_map>
#include <utility>

#include <spdlog/fmt/fmt.h>

#include "TuringException.h"
#include "map/MapView.h"

#include "BioAssert.h"

using namespace db;

namespace {

constexpr int64_t MICROSECONDS_PER_SECOND = 1000000;
constexpr int64_t SECONDS_PER_MINUTE = 60;
constexpr int64_t SECONDS_PER_HOUR = 3600;
constexpr int64_t SECONDS_PER_DAY = 86400;

constexpr int64_t MICROSECONDS_PER_MILLISECOND = 1000;

constexpr size_t FRACTION_DIGITS = 6;

// 0000-01-01T00:00:00Z and 9999-12-31T23:59:59.999999Z, the ends of the four-digit
// years format writes and parse reads
constexpr int64_t FIRST_RENDERABLE = -62167219200000000;
constexpr int64_t LAST_RENDERABLE = 253402300799999999;

struct DateTimeFields {
    int64_t _year {0};
    int64_t _month {0};
    int64_t _day {0};
    int64_t _hour {0};
    int64_t _minute {0};
    int64_t _second {0};
    int64_t _microsecond {0};
};

void decompose(DateTimeFields& fields, DateTime value) {
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

    fields._year = static_cast<int>(date.year());
    fields._month = static_cast<unsigned>(date.month());
    fields._day = static_cast<unsigned>(date.day());

    fields._hour = secondOfDay / SECONDS_PER_HOUR;
    fields._minute = (secondOfDay % SECONDS_PER_HOUR) / SECONDS_PER_MINUTE;
    fields._second = secondOfDay % SECONDS_PER_MINUTE;
    fields._microsecond = fraction;
}

int64_t compose(const DateTimeFields& fields) {
    const std::chrono::year_month_day date {std::chrono::year {static_cast<int>(fields._year)},
                                            std::chrono::month {static_cast<unsigned>(fields._month)},
                                            std::chrono::day {static_cast<unsigned>(fields._day)}};
    const int64_t epochDay = std::chrono::sys_days {date}.time_since_epoch().count();

    const int64_t seconds = epochDay * SECONDS_PER_DAY
                          + fields._hour * SECONDS_PER_HOUR
                          + fields._minute * SECONDS_PER_MINUTE
                          + fields._second;

    return seconds * MICROSECONDS_PER_SECOND + fields._microsecond;
}

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

enum class MapComponent : uint8_t {
    Year = 0,
    Month,
    Day,
    Hour,
    Minute,
    Second,
    Millisecond,
    Microsecond,
    Nanosecond,
    EpochSeconds,
    EpochMillis,
    Timezone,
    _SIZE,
};

constexpr size_t MAP_COMPONENT_COUNT = static_cast<size_t>(MapComponent::_SIZE);

constexpr std::array<std::string_view, MAP_COMPONENT_COUNT> MAP_COMPONENT_NAMES = {
    "year",
    "month",
    "day",
    "hour",
    "minute",
    "second",
    "millisecond",
    "microsecond",
    "nanosecond",
    "epochSeconds",
    "epochMillis",
    "timezone",
};

// Each unit of a calendar map needs the next larger one, so units are only left out from
// the least significant end
constexpr std::array<std::pair<MapComponent, MapComponent>, 8> LARGER_UNITS = {{
    {MapComponent::Month, MapComponent::Year},
    {MapComponent::Day, MapComponent::Month},
    {MapComponent::Hour, MapComponent::Day},
    {MapComponent::Minute, MapComponent::Hour},
    {MapComponent::Second, MapComponent::Minute},
    {MapComponent::Millisecond, MapComponent::Second},
    {MapComponent::Microsecond, MapComponent::Second},
    {MapComponent::Nanosecond, MapComponent::Second},
}};

constexpr int64_t NANOSECONDS_PER_MICROSECOND = 1000;

struct MapComponents {
    std::array<bool, MAP_COMPONENT_COUNT> _present {};
    std::array<bool, MAP_COMPONENT_COUNT> _null {};
    std::array<int64_t, MAP_COMPONENT_COUNT> _values {};
    std::string_view _timezone;

    bool has(MapComponent component) const { return _present[static_cast<size_t>(component)]; }
    bool isNull(MapComponent component) const { return _null[static_cast<size_t>(component)]; }
};

std::string_view componentName(MapComponent component) {
    return MAP_COMPONENT_NAMES[static_cast<size_t>(component)];
}

bool componentNamed(std::string_view name, MapComponent& component) {
    const auto it = std::ranges::find(MAP_COMPONENT_NAMES, name);
    if (it == MAP_COMPONENT_NAMES.end()) {
        return false;
    }

    component = static_cast<MapComponent>(std::distance(MAP_COMPONENT_NAMES.begin(), it));

    return true;
}

void readComponents(const MapView& map, MapComponents& components) {
    for (const MapEntryView entry : map) {
        const std::string_view key = entry.getKey();

        MapComponent component {MapComponent::Year};
        if (!componentNamed(key, component)) {
            throw TuringException(fmt::format("Unknown datetime component: {}", key));
        }

        const CellView value = entry.getValue();
        const CellTag tag = value.getTag();
        const size_t index = static_cast<size_t>(component);

        const bool hasAFirstValue = component != MapComponent::EpochSeconds
                                 && component != MapComponent::EpochMillis
                                 && component != MapComponent::Timezone;

        if (tag == CellTag::Null && hasAFirstValue) {
            components._null[index] = true;
        } else if (component == MapComponent::Timezone) {
            if (tag != CellTag::String) {
                throw TuringException("The datetime component timezone must be a string");
            }

            components._timezone = value.getAs<std::string_view>();
        } else if (tag == CellTag::Int) {
            components._values[index] = value.getAs<int64_t>();
        } else if (tag == CellTag::UInt) {
            const uint64_t count = value.getAs<uint64_t>();
            if (count > static_cast<uint64_t>(std::numeric_limits<int64_t>::max())) {
                throw TuringException(fmt::format("The datetime component {} is too large: {}", key, count));
            }

            components._values[index] = static_cast<int64_t>(count);
        } else {
            throw TuringException(fmt::format("The datetime component {} must be an integer", key));
        }

        components._present[index] = true;
    }
}

int64_t componentIn(const MapComponents& components, MapComponent component, int64_t first, int64_t last, int64_t absent) {
    if (!components.has(component) || components.isNull(component)) {
        return absent;
    }

    const int64_t value = components._values[static_cast<size_t>(component)];
    if (value < first || value > last) {
        throw TuringException(fmt::format("The datetime component {} must be between {} and {}, not {}",
                                          componentName(component),
                                          first,
                                          last,
                                          value));
    }

    return value;
}

int64_t timezoneOffset(std::string_view timezone) {
    size_t offset = 0;
    int64_t seconds = 0;

    if (timezone.empty() || !readOffset(timezone, offset, seconds)) {
        throw TuringException(fmt::format("The timezone '{}' is not an offset such as '+01:00' or 'Z'", timezone));
    }

    return seconds;
}

int64_t epochMicroseconds(const MapComponents& components) {
    const MapComponent epoch = components.has(MapComponent::EpochMillis) ? MapComponent::EpochMillis
                                                                         : MapComponent::EpochSeconds;

    for (size_t index = 0; index < MAP_COMPONENT_COUNT; index++) {
        const MapComponent component = static_cast<MapComponent>(index);
        const bool goesWithTheEpoch = component == epoch
                                   || component == MapComponent::Nanosecond
                                   || component == MapComponent::Timezone;

        if (components.has(component) && !goesWithTheEpoch) {
            throw TuringException(fmt::format("A datetime of an epoch takes no component but nanosecond and timezone, not {}",
                                              componentName(component)));
        }
    }

    if (epoch == MapComponent::EpochMillis) {
        const int64_t milliseconds = componentIn(components,
                                                 epoch,
                                                 FIRST_RENDERABLE / MICROSECONDS_PER_MILLISECOND,
                                                 LAST_RENDERABLE / MICROSECONDS_PER_MILLISECOND,
                                                 0);
        const int64_t nanoseconds = componentIn(components, MapComponent::Nanosecond, 0, 999999, 0);

        return milliseconds * MICROSECONDS_PER_MILLISECOND + nanoseconds / NANOSECONDS_PER_MICROSECOND;
    }

    const int64_t seconds = componentIn(components,
                                        epoch,
                                        DateTime::FIRST_EPOCH_SECOND,
                                        DateTime::LAST_EPOCH_SECOND,
                                        0);
    const int64_t nanoseconds = componentIn(components, MapComponent::Nanosecond, 0, 999999999, 0);

    return seconds * MICROSECONDS_PER_SECOND + nanoseconds / NANOSECONDS_PER_MICROSECOND;
}

int64_t calendarMicroseconds(const MapComponents& components) {
    const bool hasEpoch = components.has(MapComponent::EpochMillis) ||  components.has(MapComponent::EpochSeconds);
    bioassert(!hasEpoch, "A calendar datetime map must not contain an Epoch component");

    for (const auto& [unit, larger] : LARGER_UNITS) {
        if (components.has(unit) && !components.has(larger)) {
            throw TuringException(fmt::format("The datetime component {} needs the component {}",
                                              componentName(unit),
                                              componentName(larger)));
        }
    }

    if (!components.has(MapComponent::Year)) {
        throw TuringException("A datetime of a map needs the component year or an epoch");
    }

    const int64_t year = componentIn(components, MapComponent::Year, 0, 9999, 0);
    const int64_t month = componentIn(components, MapComponent::Month, 1, 12, 1);

    const std::chrono::year_month_day_last lastOfMonth {std::chrono::year {static_cast<int>(year)},
                                                        std::chrono::month_day_last {std::chrono::month {static_cast<unsigned>(month)}}};
    const int64_t lastDay = static_cast<unsigned>(lastOfMonth.day());
    const int64_t day = componentIn(components, MapComponent::Day, 1, lastDay, 1);

    const int64_t hour = componentIn(components, MapComponent::Hour, 0, 23, 0);
    const int64_t minute = componentIn(components, MapComponent::Minute, 0, 59, 0);
    const int64_t second = componentIn(components, MapComponent::Second, 0, 59, 0);

    const bool hasMilliseconds = components.has(MapComponent::Millisecond);
    const bool hasMicroseconds = components.has(MapComponent::Microsecond);

    const int64_t lastMicrosecond = hasMilliseconds ? 999 : 999999;
    const int64_t lastNanosecond = hasMicroseconds ? 999 : (hasMilliseconds ? 999999 : 999999999);

    const int64_t milliseconds = componentIn(components, MapComponent::Millisecond, 0, 999, 0);
    const int64_t microseconds = componentIn(components, MapComponent::Microsecond, 0, lastMicrosecond, 0);
    const int64_t nanoseconds = componentIn(components, MapComponent::Nanosecond, 0, lastNanosecond, 0);

    const DateTimeFields fields {
        ._year = year,
        ._month = month,
        ._day = day,
        ._hour = hour,
        ._minute = minute,
        ._second = second,
        ._microsecond = milliseconds * MICROSECONDS_PER_MILLISECOND + microseconds + nanoseconds / NANOSECONDS_PER_MICROSECOND,
    };

    return compose(fields);
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

    if (offset == text.size()) {
        const DateTimeFields midnight {
            ._year = year,
            ._month = month,
            ._day = day,
        };

        return DateTime {compose(midnight)};
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

    const DateTimeFields fields {
        ._year = year,
        ._month = month,
        ._day = day,
        ._hour = hour,
        ._minute = minute,
        ._second = second,
        ._microsecond = fraction,
    };

    return DateTime {compose(fields) - offsetSeconds * MICROSECONDS_PER_SECOND};
}

Duration DateTime::operator-(const DateTime& other) const {
    return Duration(_microseconds - other._microseconds);
}

DateTime DateTime::operator+(const Duration& duration) const {
    const int64_t microseconds = duration.getMicroseconds();

    const bool pastTheLastYear = microseconds > LAST_RENDERABLE - _microseconds;
    const bool beforeTheFirstYear = microseconds < FIRST_RENDERABLE - _microseconds;

    if (pastTheLastYear || beforeTheFirstYear) {
        throw TuringException("The instant of a datetime plus a duration is outside the years 0000 to 9999");
    }

    return DateTime(_microseconds + microseconds);
}

DateTime DateTime::operator-(const Duration& duration) const {
    const int64_t microseconds = duration.getMicroseconds();

    const bool pastTheLastYear = microseconds < _microseconds - LAST_RENDERABLE;
    const bool beforeTheFirstYear = microseconds > _microseconds - FIRST_RENDERABLE;

    if (pastTheLastYear || beforeTheFirstYear) {
        throw TuringException("The instant of a datetime minus a duration is outside the years 0000 to 9999");
    }

    return DateTime(_microseconds - microseconds);
}

DateTime db::operator+(const Duration& duration, const DateTime& instant) {
    return instant + duration;
}

bool DateTime::isRenderable(DateTime value) {
    const int64_t microseconds = value.getMicroseconds();

    return microseconds >= FIRST_RENDERABLE && microseconds <= LAST_RENDERABLE;
}

DateTime DateTime::now() {
    const std::chrono::system_clock::duration since
        = std::chrono::system_clock::now().time_since_epoch();

    return DateTime {std::chrono::duration_cast<std::chrono::microseconds>(since).count()};
}

bool DateTime::fromEpochSeconds(int64_t seconds, DateTime& value) {
    if (seconds < FIRST_EPOCH_SECOND || seconds > LAST_EPOCH_SECOND) {
        return false;
    }

    value = DateTime {seconds * MICROSECONDS_PER_SECOND};

    return true;
}

void DateTime::fromMap(const MapView& map, DateTime& value) {
    MapComponents components;
    readComponents(map, components);

    const bool readsAnEpoch = components.has(MapComponent::EpochSeconds) || components.has(MapComponent::EpochMillis);
    int64_t microseconds = 0;

    if (readsAnEpoch) {
        microseconds = epochMicroseconds(components);
    } else {
        microseconds = calendarMicroseconds(components);
    }

    if (components.has(MapComponent::Timezone)) {
        const int64_t offsetSeconds = timezoneOffset(components._timezone);
        if (!readsAnEpoch) {
            microseconds -= offsetSeconds * MICROSECONDS_PER_SECOND;
        }
    }

    if (!isRenderable(DateTime {microseconds})) {
        throw TuringException("The datetime of the map is outside the years 0000 to 9999");
    }

    value = DateTime {microseconds};
}

bool DateTime::partNamed(std::string_view name, DateTimePart& part) {
    static const std::unordered_map<std::string_view, DateTimePart> parts = {
        {"year",        DateTimePart::Year       },
        {"month",       DateTimePart::Month      },
        {"day",         DateTimePart::Day        },
        {"hour",        DateTimePart::Hour       },
        {"minute",      DateTimePart::Minute     },
        {"second",      DateTimePart::Second     },
        {"millisecond", DateTimePart::Millisecond},
        {"microsecond", DateTimePart::Microsecond},
    };

    const auto it = parts.find(name);
    if (it == parts.end()) {
        return false;
    }

    part = it->second;

    return true;
}

int64_t DateTime::component(DateTime value, DateTimePart part) {
    DateTimeFields fields;
    decompose(fields, value);

    switch (part) {
        case DateTimePart::Year:
            return fields._year;
        break;

        case DateTimePart::Month:
            return fields._month;
        break;

        case DateTimePart::Day:
            return fields._day;
        break;

        case DateTimePart::Hour:
            return fields._hour;
        break;

        case DateTimePart::Minute:
            return fields._minute;
        break;

        case DateTimePart::Second:
            return fields._second;
        break;

        case DateTimePart::Millisecond:
            return fields._microsecond / MICROSECONDS_PER_MILLISECOND;
        break;

        case DateTimePart::Microsecond:
            return fields._microsecond;
        break;
    }

    return 0;
}

void DateTime::format(std::string& out, DateTime value) {
    DateTimeFields fields;
    decompose(fields, value);

    out += fmt::format("{:04}-{:02}-{:02}T{:02}:{:02}:{:02}",
                       fields._year,
                       fields._month,
                       fields._day,
                       fields._hour,
                       fields._minute,
                       fields._second);

    if (fields._microsecond != 0) {
        out += fmt::format(".{:06}", fields._microsecond);
    }

    out += 'Z';
}

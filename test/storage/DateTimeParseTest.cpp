#include <gtest/gtest.h>

#include <optional>
#include <string>

#include "metadata/DateTime.h"

using namespace db;

namespace {

int64_t microsecondsOf(std::string_view text) {
    const std::optional<DateTime> parsed = DateTime::parse(text);
    EXPECT_TRUE(parsed.has_value()) << "did not parse: " << text;

    return parsed.has_value() ? parsed->getMicroseconds() : 0;
}

std::string formatted(int64_t microseconds) {
    std::string out;
    DateTime::format(out, DateTime {microseconds});

    return out;
}

constexpr int64_t microsecondsPerSecond = 1000000;
constexpr int64_t microsecondsPerDay = 86400 * microsecondsPerSecond;

}

TEST(DateTimeParseTest, readsADateAsMidnightUTC) {
    EXPECT_EQ(microsecondsOf("1970-01-01"), 0);
    EXPECT_EQ(microsecondsOf("1970-01-02"), microsecondsPerDay);
    EXPECT_EQ(microsecondsOf("2026-09-23"), 1790121600 * microsecondsPerSecond);
}

TEST(DateTimeParseTest, readsADateAndTime) {
    EXPECT_EQ(microsecondsOf("1970-01-01T00:00:01"), microsecondsPerSecond);
    EXPECT_EQ(microsecondsOf("1970-01-01T01:00:00"), 3600 * microsecondsPerSecond);
    EXPECT_EQ(microsecondsOf("2026-09-23T14:05:00Z"), 1790172300 * microsecondsPerSecond);
}

TEST(DateTimeParseTest, acceptsASpaceOrALowercaseTBetweenTheDateAndTheTime) {
    const int64_t expected = microsecondsOf("2026-09-23T14:05:00Z");

    EXPECT_EQ(microsecondsOf("2026-09-23t14:05:00Z"), expected);
    EXPECT_EQ(microsecondsOf("2026-09-23 14:05:00Z"), expected);
}

TEST(DateTimeParseTest, readsATimeWithNoSeconds) {
    EXPECT_EQ(microsecondsOf("2026-09-23T14:05Z"), 1790172300 * microsecondsPerSecond);
}

TEST(DateTimeParseTest, scalesTheFractionToMicroseconds) {
    EXPECT_EQ(microsecondsOf("1970-01-01T00:00:00.5"), 500000);
    EXPECT_EQ(microsecondsOf("1970-01-01T00:00:00.000001"), 1);
    EXPECT_EQ(microsecondsOf("1970-01-01T00:00:00.123456"), 123456);
}

// ISO-8601 puts no bound on the fraction's digits, so a nanosecond timestamp - what a
// Parquet or pandas export spells - is read rather than turned away
TEST(DateTimeParseTest, truncatesAFractionFinerThanAMicrosecond) {
    EXPECT_EQ(microsecondsOf("1970-01-01T00:00:00.123456789"), 123456);
}

TEST(DateTimeParseTest, spendsTheOffsetOnTheInstant) {
    const int64_t utc = microsecondsOf("2026-09-23T14:05:00Z");

    EXPECT_EQ(microsecondsOf("2026-09-23T16:05:00+02:00"), utc);
    EXPECT_EQ(microsecondsOf("2026-09-23T12:05:00-02:00"), utc);
    EXPECT_EQ(microsecondsOf("2026-09-23T16:05:00+0200"), utc);
    EXPECT_EQ(microsecondsOf("2026-09-23T16:05:00+02"), utc);
}

TEST(DateTimeParseTest, readsAnInstantBeforeTheEpoch) {
    EXPECT_EQ(microsecondsOf("1969-12-31T23:59:59Z"), -microsecondsPerSecond);
    EXPECT_EQ(microsecondsOf("1969-12-31T23:59:59.5Z"), -500000);
    EXPECT_EQ(microsecondsOf("1969-12-31"), -microsecondsPerDay);
}

TEST(DateTimeParseTest, answersNothingForTextThatNamesNoInstant) {
    EXPECT_FALSE(DateTime::parse("").has_value());
    EXPECT_FALSE(DateTime::parse("not a date").has_value());
    EXPECT_FALSE(DateTime::parse("2026-09-23T").has_value());
    EXPECT_FALSE(DateTime::parse("2026-09-23T14:05:00Zjunk").has_value());
    EXPECT_FALSE(DateTime::parse("2026-9-23").has_value());
    EXPECT_FALSE(DateTime::parse("2026-09-23T14:05:00.").has_value());
    EXPECT_FALSE(DateTime::parse("+2026-09-23").has_value());
}

TEST(DateTimeParseTest, answersNothingForADayNoMonthHas) {
    EXPECT_FALSE(DateTime::parse("2026-02-30").has_value());
    EXPECT_FALSE(DateTime::parse("2026-13-01").has_value());
    EXPECT_FALSE(DateTime::parse("2026-00-01").has_value());
    EXPECT_FALSE(DateTime::parse("2025-02-29").has_value());
    EXPECT_TRUE(DateTime::parse("2024-02-29").has_value());
}

TEST(DateTimeParseTest, answersNothingForATimeNoClockShows) {
    EXPECT_FALSE(DateTime::parse("2026-09-23T24:00:00").has_value());
    EXPECT_FALSE(DateTime::parse("2026-09-23T14:60:00").has_value());
    EXPECT_FALSE(DateTime::parse("2026-09-23T14:05:60").has_value());
    EXPECT_FALSE(DateTime::parse("2026-09-23T14:05:00+24:00").has_value());
}

TEST(DateTimeParseTest, rendersAsUTCWithTheFractionOnlyWhenThereIsOne) {
    EXPECT_EQ(formatted(0), "1970-01-01T00:00:00Z");
    EXPECT_EQ(formatted(1790172300 * microsecondsPerSecond), "2026-09-23T14:05:00Z");
    EXPECT_EQ(formatted(123456), "1970-01-01T00:00:00.123456Z");
    EXPECT_EQ(formatted(500000), "1970-01-01T00:00:00.500000Z");
}

// Both divisions floor, so the day an instant before the epoch falls in is the one before
// it rather than the one after, and its time of day counts up from that day's midnight
TEST(DateTimeParseTest, rendersAnInstantBeforeTheEpoch) {
    EXPECT_EQ(formatted(-microsecondsPerSecond), "1969-12-31T23:59:59Z");
    EXPECT_EQ(formatted(-500000), "1969-12-31T23:59:59.500000Z");
    EXPECT_EQ(formatted(-microsecondsPerDay), "1969-12-31T00:00:00Z");
}

TEST(DateTimeParseTest, roundTripsEverySpelling) {
    const std::string_view texts[] = {
        "1970-01-01T00:00:00Z",
        "2026-09-23T14:05:00Z",
        "2026-09-23T14:05:00.123456Z",
        "1969-12-31T23:59:59Z",
        "1900-01-01T00:00:00Z",
        "2262-04-11T23:47:16Z",
    };

    for (const std::string_view text : texts) {
        const std::optional<DateTime> parsed = DateTime::parse(text);
        ASSERT_TRUE(parsed.has_value()) << text;

        std::string rendered;
        DateTime::format(rendered, *parsed);

        EXPECT_EQ(rendered, text);
    }
}

TEST(DateTimeParseTest, ordersByTheInstant) {
    const DateTime earlier {microsecondsOf("2026-09-23T14:05:00Z")};
    const DateTime later {microsecondsOf("2026-09-23T14:05:01Z")};

    EXPECT_LT(earlier, later);
    EXPECT_GT(later, earlier);
    EXPECT_EQ(earlier, DateTime {microsecondsOf("2026-09-23T16:05:00+02:00")});
    EXPECT_NE(earlier, later);
}

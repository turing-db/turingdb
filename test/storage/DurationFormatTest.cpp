#include <gtest/gtest.h>

#include <limits>
#include <string>

#include "metadata/Duration.h"

using namespace db;

namespace {

std::string formatted(int64_t microseconds) {
    std::string out;
    Duration::format(out, Duration {microseconds});

    return out;
}

constexpr int64_t microsecondsPerSecond = 1000000;

}

TEST(DurationFormatTest, writesZeroAsZeroSeconds) {
    EXPECT_EQ(formatted(0), "PT0S");
}

TEST(DurationFormatTest, writesHoursMinutesAndSeconds) {
    EXPECT_EQ(formatted(90061 * microsecondsPerSecond), "PT25H1M1S");
    EXPECT_EQ(formatted(3600 * microsecondsPerSecond), "PT1H");
    EXPECT_EQ(formatted(60 * microsecondsPerSecond), "PT1M");
    EXPECT_EQ(formatted(3601 * microsecondsPerSecond), "PT1H1S");
}

TEST(DurationFormatTest, writesTheFractionWithoutTrailingZeros) {
    EXPECT_EQ(formatted(1500000), "PT1.5S");
    EXPECT_EQ(formatted(1), "PT0.000001S");
    EXPECT_EQ(formatted(120000), "PT0.12S");
    EXPECT_EQ(formatted(60 * microsecondsPerSecond + 250000), "PT1M0.25S");
}

TEST(DurationFormatTest, signsEachComponentOfANegativeDuration) {
    EXPECT_EQ(formatted(-1500000), "PT-1.5S");
    EXPECT_EQ(formatted(-90061 * microsecondsPerSecond), "PT-25H-1M-1S");
    EXPECT_EQ(formatted(-5400 * microsecondsPerSecond), "PT-1H-30M");
}

TEST(DurationFormatTest, writesTheEndsOfTheRange) {
    EXPECT_EQ(formatted(std::numeric_limits<int64_t>::max()), "PT2562047788H54.775807S");
    EXPECT_EQ(formatted(std::numeric_limits<int64_t>::min()), "PT-2562047788H-54.775808S");
}

#include <gtest/gtest.h>

#include <limits>

#include "metadata/Duration.h"

using namespace db;

namespace {

constexpr int64_t microsecondsPerAverageMonth = 2629746000000;
constexpr int64_t microsecondsPerAverageYear = 31556952000000;

}

TEST(DurationComponentTest, readsEveryUnitOfADayAndAHalf) {
    const Duration value {90061123456};

    EXPECT_EQ(Duration::component(value, DurationPart::Years), 0);
    EXPECT_EQ(Duration::component(value, DurationPart::Quarters), 0);
    EXPECT_EQ(Duration::component(value, DurationPart::Months), 0);
    EXPECT_EQ(Duration::component(value, DurationPart::Weeks), 0);
    EXPECT_EQ(Duration::component(value, DurationPart::Days), 1);
    EXPECT_EQ(Duration::component(value, DurationPart::Hours), 25);
    EXPECT_EQ(Duration::component(value, DurationPart::Minutes), 1501);
    EXPECT_EQ(Duration::component(value, DurationPart::Seconds), 90061);
    EXPECT_EQ(Duration::component(value, DurationPart::Milliseconds), 90061123);
    EXPECT_EQ(Duration::component(value, DurationPart::Microseconds), 90061123456);
}

TEST(DurationComponentTest, readsAnAverageMonth) {
    const Duration value {microsecondsPerAverageMonth};

    EXPECT_EQ(Duration::component(value, DurationPart::Years), 0);
    EXPECT_EQ(Duration::component(value, DurationPart::Quarters), 0);
    EXPECT_EQ(Duration::component(value, DurationPart::Months), 1);
    EXPECT_EQ(Duration::component(value, DurationPart::Weeks), 4);
    EXPECT_EQ(Duration::component(value, DurationPart::Days), 30);
    EXPECT_EQ(Duration::component(value, DurationPart::Hours), 730);
}

TEST(DurationComponentTest, readsNoMonthOneMicrosecondShortOfOne) {
    const Duration value {microsecondsPerAverageMonth - 1};

    EXPECT_EQ(Duration::component(value, DurationPart::Months), 0);
    EXPECT_EQ(Duration::component(value, DurationPart::Days), 30);
}

TEST(DurationComponentTest, readsAnAverageYear) {
    const Duration value {microsecondsPerAverageYear};
    EXPECT_EQ(Duration::component(value, DurationPart::Years), 1);
    EXPECT_EQ(Duration::component(value, DurationPart::Quarters), 4);
    EXPECT_EQ(Duration::component(value, DurationPart::Months), 12);
    EXPECT_EQ(Duration::component(value, DurationPart::Weeks), 52);
    EXPECT_EQ(Duration::component(value, DurationPart::Days), 365);
    EXPECT_EQ(Duration::component(value, DurationPart::Hours), 8765);
}

TEST(DurationComponentTest, truncatesANegativeDurationTowardZero) {
    const Duration ninetySecondsBack {-90000000};

    EXPECT_EQ(Duration::component(ninetySecondsBack, DurationPart::Hours), 0);
    EXPECT_EQ(Duration::component(ninetySecondsBack, DurationPart::Minutes), -1);
    EXPECT_EQ(Duration::component(ninetySecondsBack, DurationPart::Seconds), -90);
    EXPECT_EQ(Duration::component(ninetySecondsBack, DurationPart::Milliseconds), -90000);

    const Duration oneMicrosecondBack {-1};

    EXPECT_EQ(Duration::component(oneMicrosecondBack, DurationPart::Seconds), 0);
    EXPECT_EQ(Duration::component(oneMicrosecondBack, DurationPart::Milliseconds), 0);
    EXPECT_EQ(Duration::component(oneMicrosecondBack, DurationPart::Microseconds), -1);
}

TEST(DurationComponentTest, readsZero) {
    const Duration value {0};

    EXPECT_EQ(Duration::component(value, DurationPart::Years), 0);
    EXPECT_EQ(Duration::component(value, DurationPart::Days), 0);
    EXPECT_EQ(Duration::component(value, DurationPart::Microseconds), 0);
}

TEST(DurationComponentTest, readsTheLongestDurations) {
    const Duration longest {std::numeric_limits<int64_t>::max()};

    EXPECT_EQ(Duration::component(longest, DurationPart::Years), 292277);
    EXPECT_EQ(Duration::component(longest, DurationPart::Quarters), 1169108);
    EXPECT_EQ(Duration::component(longest, DurationPart::Months), 3507324);
    EXPECT_EQ(Duration::component(longest, DurationPart::Weeks), 15250284);
    EXPECT_EQ(Duration::component(longest, DurationPart::Days), 106751991);
    EXPECT_EQ(Duration::component(longest, DurationPart::Hours), 2562047788);
    EXPECT_EQ(Duration::component(longest, DurationPart::Minutes), 153722867280);
    EXPECT_EQ(Duration::component(longest, DurationPart::Seconds), 9223372036854);
    EXPECT_EQ(Duration::component(longest, DurationPart::Milliseconds), 9223372036854775);
    EXPECT_EQ(Duration::component(longest, DurationPart::Microseconds), std::numeric_limits<int64_t>::max());

    const Duration mostNegative {std::numeric_limits<int64_t>::min()};

    EXPECT_EQ(Duration::component(mostNegative, DurationPart::Years), -292277);
    EXPECT_EQ(Duration::component(mostNegative, DurationPart::Days), -106751991);
    EXPECT_EQ(Duration::component(mostNegative, DurationPart::Milliseconds), -9223372036854775);
    EXPECT_EQ(Duration::component(mostNegative, DurationPart::Microseconds), std::numeric_limits<int64_t>::min());
}

TEST(DurationComponentTest, readsTheClockRemaindersOfADayAndAHalf) {
    const Duration value {90061123456};

    EXPECT_EQ(Duration::component(value, DurationPart::DaysOfWeek), 1);
    EXPECT_EQ(Duration::component(value, DurationPart::MinutesOfHour), 1);
    EXPECT_EQ(Duration::component(value, DurationPart::SecondsOfMinute), 1);
    EXPECT_EQ(Duration::component(value, DurationPart::MillisecondsOfSecond), 123);
    EXPECT_EQ(Duration::component(value, DurationPart::MicrosecondsOfSecond), 123456);
}

TEST(DurationComponentTest, readsTheCalendarRemaindersOfSeventeenMonthsAndTenDays) {
    const Duration value {17 * microsecondsPerAverageMonth + 864000000000};

    EXPECT_EQ(Duration::component(value, DurationPart::QuartersOfYear), 1);
    EXPECT_EQ(Duration::component(value, DurationPart::MonthsOfYear), 5);
    EXPECT_EQ(Duration::component(value, DurationPart::MonthsOfQuarter), 2);
    EXPECT_EQ(Duration::component(value, DurationPart::DaysOfWeek), 2);
}

TEST(DurationComponentTest, readsNoRemainderOfAWholeYear) {
    const Duration value {microsecondsPerAverageYear};

    EXPECT_EQ(Duration::component(value, DurationPart::QuartersOfYear), 0);
    EXPECT_EQ(Duration::component(value, DurationPart::MonthsOfYear), 0);
    EXPECT_EQ(Duration::component(value, DurationPart::MonthsOfQuarter), 0);
}

TEST(DurationComponentTest, carriesTheSignOfANegativeDurationOnItsRemainders) {
    const Duration ninetySecondsBack {-90000000};

    EXPECT_EQ(Duration::component(ninetySecondsBack, DurationPart::MinutesOfHour), -1);
    EXPECT_EQ(Duration::component(ninetySecondsBack, DurationPart::SecondsOfMinute), -30);
    EXPECT_EQ(Duration::component(ninetySecondsBack, DurationPart::MillisecondsOfSecond), 0);

    const Duration oneAndAHalfSecondsBack {-1500000};

    EXPECT_EQ(Duration::component(oneAndAHalfSecondsBack, DurationPart::SecondsOfMinute), -1);
    EXPECT_EQ(Duration::component(oneAndAHalfSecondsBack, DurationPart::MillisecondsOfSecond), -500);
    EXPECT_EQ(Duration::component(oneAndAHalfSecondsBack, DurationPart::MicrosecondsOfSecond), -500000);
}

TEST(DurationComponentTest, readsTheRemaindersOfTheLongestDurations) {
    const Duration longest {std::numeric_limits<int64_t>::max()};

    EXPECT_EQ(Duration::component(longest, DurationPart::MicrosecondsOfSecond), 775807);
    EXPECT_EQ(Duration::component(longest, DurationPart::SecondsOfMinute), 9223372036854 % 60);
    EXPECT_EQ(Duration::component(longest, DurationPart::MonthsOfYear), 3507324 % 12);

    const Duration mostNegative {std::numeric_limits<int64_t>::min()};

    EXPECT_EQ(Duration::component(mostNegative, DurationPart::MicrosecondsOfSecond), -775808);
    EXPECT_EQ(Duration::component(mostNegative, DurationPart::MillisecondsOfSecond), -775);
}

#include <gtest/gtest.h>

#include <limits>
#include <optional>
#include <string_view>

#include "metadata/DateTime.h"

using namespace db;

namespace {

DateTime instant(std::string_view text) {
    const std::optional<DateTime> parsed = DateTime::parse(text);
    EXPECT_TRUE(parsed.has_value()) << "did not parse: " << text;

    return parsed.has_value() ? *parsed : DateTime {0};
}

int64_t componentOf(std::string_view text, DateTimePart part) {
    return DateTime::component(instant(text), part);
}

}

TEST(DateTimeComponentTest, readsEveryFieldOfAnInstant) {
    const DateTime value = instant("2026-09-23T14:05:06.123456Z");

    EXPECT_EQ(DateTime::component(value, DateTimePart::Year), 2026);
    EXPECT_EQ(DateTime::component(value, DateTimePart::Month), 9);
    EXPECT_EQ(DateTime::component(value, DateTimePart::Day), 23);
    EXPECT_EQ(DateTime::component(value, DateTimePart::Hour), 14);
    EXPECT_EQ(DateTime::component(value, DateTimePart::Minute), 5);
    EXPECT_EQ(DateTime::component(value, DateTimePart::Second), 6);
    EXPECT_EQ(DateTime::component(value, DateTimePart::Millisecond), 123);
    EXPECT_EQ(DateTime::component(value, DateTimePart::Microsecond), 123456);
}

TEST(DateTimeComponentTest, readsTheEpochItself) {
    const DateTime value = instant("1970-01-01T00:00:00Z");

    EXPECT_EQ(DateTime::component(value, DateTimePart::Year), 1970);
    EXPECT_EQ(DateTime::component(value, DateTimePart::Month), 1);
    EXPECT_EQ(DateTime::component(value, DateTimePart::Day), 1);
    EXPECT_EQ(DateTime::component(value, DateTimePart::Hour), 0);
    EXPECT_EQ(DateTime::component(value, DateTimePart::Minute), 0);
    EXPECT_EQ(DateTime::component(value, DateTimePart::Second), 0);
    EXPECT_EQ(DateTime::component(value, DateTimePart::Microsecond), 0);
}

// The microsecond count of an instant before the epoch is negative while its time of day
// still counts up, so every field reads as the calendar spells it rather than as a
// remainder of that negative count
TEST(DateTimeComponentTest, readsAnInstantBeforeTheEpoch) {
    const DateTime value = instant("1969-12-31T23:59:59.500000Z");

    EXPECT_EQ(DateTime::component(value, DateTimePart::Year), 1969);
    EXPECT_EQ(DateTime::component(value, DateTimePart::Month), 12);
    EXPECT_EQ(DateTime::component(value, DateTimePart::Day), 31);
    EXPECT_EQ(DateTime::component(value, DateTimePart::Hour), 23);
    EXPECT_EQ(DateTime::component(value, DateTimePart::Minute), 59);
    EXPECT_EQ(DateTime::component(value, DateTimePart::Second), 59);
    EXPECT_EQ(DateTime::component(value, DateTimePart::Millisecond), 500);
    EXPECT_EQ(DateTime::component(value, DateTimePart::Microsecond), 500000);
}

TEST(DateTimeComponentTest, readsALeapDay) {
    EXPECT_EQ(componentOf("2024-02-29T12:00:00Z", DateTimePart::Month), 2);
    EXPECT_EQ(componentOf("2024-02-29T12:00:00Z", DateTimePart::Day), 29);
}

// A microsecond count finer than a millisecond truncates towards the millisecond it sits
// in rather than rounding to the next one
TEST(DateTimeComponentTest, truncatesTheSubSecondFieldsToTheirOwnPrecision) {
    EXPECT_EQ(componentOf("2026-01-01T00:00:00.000999Z", DateTimePart::Millisecond), 0);
    EXPECT_EQ(componentOf("2026-01-01T00:00:00.000999Z", DateTimePart::Microsecond), 999);
    EXPECT_EQ(componentOf("2026-01-01T00:00:00.999999Z", DateTimePart::Millisecond), 999);
}

TEST(DateTimeComponentTest, readsACountOfSecondsSinceTheEpoch) {
    DateTime value;

    ASSERT_TRUE(DateTime::fromEpochSeconds(0, value));
    EXPECT_EQ(value.getMicroseconds(), 0);

    ASSERT_TRUE(DateTime::fromEpochSeconds(1700000000, value));
    EXPECT_EQ(DateTime::component(value, DateTimePart::Year), 2023);
    EXPECT_EQ(DateTime::component(value, DateTimePart::Month), 11);
    EXPECT_EQ(DateTime::component(value, DateTimePart::Day), 14);
    EXPECT_EQ(DateTime::component(value, DateTimePart::Hour), 22);
    EXPECT_EQ(DateTime::component(value, DateTimePart::Minute), 13);
    EXPECT_EQ(DateTime::component(value, DateTimePart::Second), 20);

    ASSERT_TRUE(DateTime::fromEpochSeconds(-1, value));
    EXPECT_EQ(DateTime::component(value, DateTimePart::Year), 1969);
    EXPECT_EQ(DateTime::component(value, DateTimePart::Second), 59);
}

TEST(DateTimeComponentTest, turnsAwayASecondCountNoInstantCanSpell) {
    DateTime value;

    EXPECT_FALSE(DateTime::fromEpochSeconds(253402300800, value));
    EXPECT_FALSE(DateTime::fromEpochSeconds(-62167219201, value));
    EXPECT_FALSE(DateTime::fromEpochSeconds(std::numeric_limits<int64_t>::max(), value));
    EXPECT_FALSE(DateTime::fromEpochSeconds(std::numeric_limits<int64_t>::min(), value));

    EXPECT_TRUE(DateTime::fromEpochSeconds(253402300799, value));
    EXPECT_TRUE(DateTime::fromEpochSeconds(-62167219200, value));
}

TEST(DateTimeComponentTest, readsTheClock) {
    const DateTime value = DateTime::now();

    EXPECT_TRUE(DateTime::isRenderable(value));
    EXPECT_GT(value.getMicroseconds(), instant("2020-01-01T00:00:00Z").getMicroseconds());
    EXPECT_LT(value.getMicroseconds(), instant("2100-01-01T00:00:00Z").getMicroseconds());
}

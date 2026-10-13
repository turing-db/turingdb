#include <gtest/gtest.h>

#include <stdint.h>

#include <limits>
#include <optional>

#include "columns/Functions.h"

#include "TuringTest.h"

using namespace db;

class IntegerConversionOfUnsignedTest : public turing::test::TuringTest {
};

TEST_F(IntegerConversionOfUnsignedTest, anUnsignedPastTheLargestIntegerIsNull) {
    ToIntegerFromNumberFunction<uint64_t> toInteger;

    EXPECT_EQ(toInteger(std::numeric_limits<uint64_t>::max()), std::nullopt);
    EXPECT_EQ(toInteger(static_cast<uint64_t>(std::numeric_limits<int64_t>::max()) + 1), std::nullopt);
}

TEST_F(IntegerConversionOfUnsignedTest, anUnsignedUpToTheLargestIntegerConverts) {
    ToIntegerFromNumberFunction<uint64_t> toInteger;

    constexpr int64_t LARGEST = std::numeric_limits<int64_t>::max();

    EXPECT_EQ(toInteger(static_cast<uint64_t>(LARGEST)), std::optional<int64_t> {LARGEST});
    EXPECT_EQ(toInteger(0), std::optional<int64_t> {0});
}

int main(int argc, char** argv) {
    return turing::test::turingTestMain(argc, argv);
}

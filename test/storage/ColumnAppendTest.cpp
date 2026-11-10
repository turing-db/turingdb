#include <gtest/gtest.h>

#include <optional>
#include <string_view>
#include <vector>

#include "columns/ColumnAppend.h"
#include "columns/ColumnMask.h"
#include "columns/ColumnVector.h"
#include "metadata/PropertyType.h"

using namespace db;

// A predicate over values that hold no null returns a mask: MATCH (n:T) RETURN n.i IS NULL.
// The embedded client buffers a result window by window.
TEST(ColumnAppendTest, appendsAWindowOfAMask) {
    const ColumnMask first {true, false, true, false};
    const ColumnMask second {false, true};

    ColumnMask buffered;
    appendColumnRows(&first, &buffered, 1, 2);
    appendColumnRows(&second, &buffered, 0, 2);

    const std::vector<bool> values(buffered.begin(), buffered.end());
    const std::vector<bool> expected {false, true, false, true};
    EXPECT_EQ(values, expected);
}

TEST(ColumnAppendTest, appendsNoRowOfAMask) {
    const ColumnMask chunk {true, false};

    ColumnMask buffered {true};
    appendColumnRows(&chunk, &buffered, 2, 0);

    const std::vector<bool> values(buffered.begin(), buffered.end());
    const std::vector<bool> expected {true};
    EXPECT_EQ(values, expected);
}

TEST(ColumnAppendTest, appendsAWindowOfPlainBooleans) {
    const ColumnVector<CustomBool> chunk {CustomBool {true}, CustomBool {false}, CustomBool {true}};

    ColumnVector<CustomBool> buffered;
    appendColumnRows(&chunk, &buffered, 1, 2);

    const std::vector<CustomBool> expected {CustomBool {false}, CustomBool {true}};
    EXPECT_EQ(buffered.getRaw(), expected);
}

TEST(ColumnAppendTest, appendsAWindowOfNullableBooleans) {
    const ColumnVector<std::optional<CustomBool>> chunk {std::nullopt, CustomBool {true}, CustomBool {false}};

    ColumnVector<std::optional<CustomBool>> buffered;
    appendColumnRows(&chunk, &buffered, 0, 2);

    const std::vector<std::optional<CustomBool>> expected {std::nullopt, CustomBool {true}};
    EXPECT_EQ(buffered.getRaw(), expected);
}

TEST(ColumnAppendTest, appendsAWindowOfStrings) {
    const ColumnVector<std::string_view> chunk {"a", "b", "c"};

    ColumnVector<std::string_view> buffered {"z"};
    appendColumnRows(&chunk, &buffered, 2, 1);

    const std::vector<std::string_view> expected {"z", "c"};
    EXPECT_EQ(buffered.getRaw(), expected);
}

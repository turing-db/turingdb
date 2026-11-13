#include <gtest/gtest.h>

#include <optional>
#include <string>

#include "columns/ColumnConst.h"
#include "columns/ColumnOptVector.h"

using namespace db;

namespace {

template <typename ColumnType>
concept HasNullableAccessors = requires(ColumnType column, const ColumnType constColumn) {
    constColumn.isValid(size_t {0});
    constColumn.get(size_t {0});
    constColumn.getValue(size_t {0});
    constColumn.getNullCount();
    constColumn.hasMask();
    column.setNull(size_t {0});
    column.setOptional(size_t {0}, std::nullopt);
    column.pushNull();
};

template <typename ColumnType>
concept HasConstReadAccessors = requires(const ColumnType constColumn) {
    constColumn.isValid(size_t {0});
    constColumn.get(size_t {0});
    constColumn.getValue(size_t {0});
    constColumn.getNullCount();
};

template <typename ColumnType>
concept HasConstOptionalSetter = requires(ColumnType column) {
    column.setOptional(std::nullopt);
};

}

TEST(ColumnNullableAccessorTest, accessorsExistOnlyOnOptionalElements) {
    static_assert(HasNullableAccessors<ColumnOptVector<int64_t>>);
    static_assert(HasNullableAccessors<ColumnOptVector<std::string>>);
    static_assert(!HasNullableAccessors<ColumnVector<int64_t>>);
    static_assert(!HasNullableAccessors<ColumnVector<std::string>>);

    static_assert(HasConstReadAccessors<ColumnConst<std::optional<int64_t>>>);
    static_assert(!HasConstReadAccessors<ColumnConst<int64_t>>);
    static_assert(HasConstOptionalSetter<ColumnConst<std::optional<int64_t>>>);
    static_assert(!HasConstOptionalSetter<ColumnConst<int64_t>>);
}

TEST(ColumnNullableAccessorTest, pushBackAndPushNull) {
    ColumnOptVector<int64_t> column;
    EXPECT_TRUE(column.empty());
    EXPECT_EQ(column.getNullCount(), 0);
    EXPECT_FALSE(column.hasMask());

    column.push_back(int64_t {7});
    column.pushNull();
    column.push_back(int64_t {9});

    EXPECT_EQ(column.size(), 3);
    EXPECT_FALSE(column.empty());
    EXPECT_EQ(column.getNullCount(), 1);
    EXPECT_TRUE(column.hasMask());

    EXPECT_TRUE(column.isValid(0));
    EXPECT_FALSE(column.isValid(1));
    EXPECT_TRUE(column.isValid(2));

    EXPECT_EQ(column.get(0), std::optional<int64_t> {7});
    EXPECT_EQ(column.get(1), std::nullopt);
    EXPECT_EQ(column.get(2), std::optional<int64_t> {9});

    EXPECT_EQ(column.getValue(0), 7);
    EXPECT_EQ(column.getValue(2), 9);
}

TEST(ColumnNullableAccessorTest, getValueOnNullRowIsValueInitialised) {
    ColumnOptVector<int64_t> integers;
    integers.pushNull();
    EXPECT_EQ(integers.getValue(0), int64_t {});

    ColumnOptVector<std::string> strings;
    strings.pushNull();
    strings.push_back(std::string("a"));
    EXPECT_EQ(strings.getValue(0), std::string {});
    EXPECT_EQ(strings.getValue(1), "a");
}

TEST(ColumnNullableAccessorTest, setAndSetNull) {
    ColumnOptVector<int64_t> column;
    column.resize(4);
    EXPECT_EQ(column.getNullCount(), 4);
    EXPECT_TRUE(column.hasMask());

    column.set(0, int64_t {1});
    column.set(2, int64_t {3});
    EXPECT_EQ(column.getNullCount(), 2);
    EXPECT_TRUE(column.isValid(0));
    EXPECT_FALSE(column.isValid(1));

    column.set(1, int64_t {2});
    column.set(3, int64_t {4});
    EXPECT_EQ(column.getNullCount(), 0);
    EXPECT_FALSE(column.hasMask());

    column.setNull(2);
    EXPECT_EQ(column.getNullCount(), 1);
    EXPECT_FALSE(column.isValid(2));
    EXPECT_EQ(column.get(2), std::nullopt);
    EXPECT_EQ(column.getValue(2), int64_t {});

    column.setNull(2);
    EXPECT_EQ(column.getNullCount(), 1);
}

TEST(ColumnNullableAccessorTest, resizeAddsNullRows) {
    ColumnOptVector<int64_t> column;
    column.push_back(int64_t {5});
    EXPECT_EQ(column.getNullCount(), 0);

    column.resize(3);
    EXPECT_EQ(column.size(), 3);
    EXPECT_EQ(column.getNullCount(), 2);
    EXPECT_TRUE(column.isValid(0));
    EXPECT_FALSE(column.isValid(1));
    EXPECT_FALSE(column.isValid(2));

    column.resize(1);
    EXPECT_EQ(column.getNullCount(), 0);
    EXPECT_FALSE(column.hasMask());

    column.resize(3);
    column.set(1, int64_t {6});
    column.set(2, int64_t {7});
    column.resize(1);
    column.resize(3);
    EXPECT_EQ(column.getNullCount(), 2);
    EXPECT_FALSE(column.isValid(1));
    EXPECT_FALSE(column.isValid(2));
    EXPECT_EQ(column.getValue(2), int64_t {});

    ColumnOptVector<std::string> strings;
    strings.push_back(std::string("kept"));
    strings.push_back(std::string("dropped"));
    strings.resize(1);
    strings.resize(2);
    EXPECT_EQ(strings.getNullCount(), 1);
    EXPECT_EQ(strings.getValue(0), "kept");
    EXPECT_FALSE(strings.isValid(1));
    EXPECT_EQ(strings.getValue(1), std::string {});
}

TEST(ColumnNullableAccessorTest, valueOverloadsMarkRowsValid) {
    ColumnOptVector<int64_t> column;
    column.resize(2);

    const int64_t value = 11;
    column.set(0, value);
    column.set(1, 12);
    EXPECT_EQ(column.getNullCount(), 0);
    EXPECT_EQ(column.getValue(0), 11);
    EXPECT_EQ(column.getValue(1), 12);

    column.push_back(value);
    column.push_back(13);
    EXPECT_EQ(column.getNullCount(), 0);
    EXPECT_EQ(column.getValue(3), 13);

    ColumnOptVector<std::string> strings;
    const std::string text = "text";
    strings.push_back(text);
    strings.push_back(std::string("moved"));
    strings.pushNull();
    strings.set(2, text);
    EXPECT_EQ(strings.getNullCount(), 0);
    EXPECT_EQ(strings.getValue(1), "moved");
    EXPECT_EQ(strings.getValue(2), "text");
}

TEST(ColumnNullableAccessorTest, setOptionalAndPushBackAnOptional) {
    ColumnOptVector<int64_t> column;
    column.resize(3);

    const std::optional<int64_t> present = 4;
    const std::optional<int64_t> absent;
    column.setOptional(0, present);
    column.setOptional(1, absent);
    column.setOptional(2, std::nullopt);
    EXPECT_EQ(column.getNullCount(), 2);
    EXPECT_TRUE(column.isValid(0));

    column.setOptional(0, std::optional<int64_t> {5});
    EXPECT_EQ(column.get(0), std::optional<int64_t> {5});
    column.setOptional(0, present);

    column.push_back(present);
    column.push_back(absent);
    column.push_back(std::nullopt);
    column.push_back(std::optional<int64_t> {6});
    EXPECT_EQ(column.size(), 7);
    EXPECT_EQ(column.getNullCount(), 4);
    EXPECT_EQ(column.get(3), present);
    EXPECT_EQ(column.get(6), std::optional<int64_t> {6});
}

TEST(ColumnNullableAccessorTest, constColumnReadAccessors) {
    ColumnConst<std::optional<int64_t>> column;
    EXPECT_EQ(column.size(), 0);
    EXPECT_EQ(column.getNullCount(), 0);

    column.setOptional(std::optional<int64_t> {8});
    EXPECT_TRUE(column.isValid(0));
    EXPECT_EQ(column.get(0), std::optional<int64_t> {8});
    EXPECT_EQ(column.getValue(0), 8);
    EXPECT_EQ(column.getNullCount(), 0);

    column.setOptional(std::nullopt);
    EXPECT_FALSE(column.isValid(0));
    EXPECT_EQ(column.get(0), std::nullopt);
    EXPECT_EQ(column.getValue(0), int64_t {});
    EXPECT_EQ(column.getNullCount(), 1);

    column.clear();
    EXPECT_EQ(column.getNullCount(), 0);
}

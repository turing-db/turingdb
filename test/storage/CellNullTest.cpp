#include <gtest/gtest.h>

#include <array>
#include <string_view>

#include "columns/BinaryPredicates.h"
#include "CellView.h"
#include "map/MapEntryView.h"

using namespace db;

TEST(CellNullTest, readsAListElementTaggedNullAsNull) {
    const std::array<std::byte, 1> nullElement {std::byte {static_cast<uint8_t>(CellTag::Null)}};
    const std::array<std::byte, 1> integerElement {std::byte {static_cast<uint8_t>(CellTag::Int)}};

    const CellView nullCell {nullElement.data()};
    const CellView integerCell {integerElement.data()};

    EXPECT_FALSE(holdsAValue(nullCell));
    EXPECT_TRUE(holdsAValue(integerCell));
}

TEST(CellNullTest, readsAMapValueTaggedNullAsNull) {
    std::array<std::byte, sizeof(std::string_view) + 1> nullEntry {};
    nullEntry.back() = std::byte {static_cast<uint8_t>(CellTag::Null)};

    std::array<std::byte, sizeof(std::string_view) + 1> integerEntry {};
    integerEntry.back() = std::byte {static_cast<uint8_t>(CellTag::Int)};

    const CellView nullCell = MapEntryView {nullEntry.data()}.getValue();
    const CellView integerCell = MapEntryView {integerEntry.data()}.getValue();

    EXPECT_FALSE(holdsAValue(nullCell));
    EXPECT_TRUE(holdsAValue(integerCell));
}

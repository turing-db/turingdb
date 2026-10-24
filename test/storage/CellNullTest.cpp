#include <gtest/gtest.h>

#include <array>
#include <optional>
#include <string_view>

#include "list/ListElementView.h"
#include "map/MapEntryView.h"
#include "metadata/PropertyNull.h"

using namespace db;

TEST(CellNullTest, readsAListElementTaggedNullAsNull) {
    const std::array<std::byte, 1> nullElement {std::byte {static_cast<uint8_t>(ListBufferTypeTag::Null)}};
    const std::array<std::byte, 1> integerElement {std::byte {static_cast<uint8_t>(ListBufferTypeTag::Int)}};

    const std::optional<ListElementView> nullCell {ListElementView {nullElement.data()}};
    const std::optional<ListElementView> integerCell {ListElementView {integerElement.data()}};

    EXPECT_TRUE(nullCell == PropertyNull {});
    EXPECT_TRUE(PropertyNull {} == nullCell);
    EXPECT_FALSE(integerCell == PropertyNull {});
    EXPECT_TRUE(std::optional<ListElementView> {} == PropertyNull {});
}

TEST(CellNullTest, readsAMapEntryTaggedNullAsNull) {
    std::array<std::byte, sizeof(std::string_view) + 1> nullEntry {};
    nullEntry.back() = std::byte {static_cast<uint8_t>(MapBufferTypeTag::Null)};

    std::array<std::byte, sizeof(std::string_view) + 1> integerEntry {};
    integerEntry.back() = std::byte {static_cast<uint8_t>(MapBufferTypeTag::Int)};

    const std::optional<MapEntryView> nullCell {MapEntryView {nullEntry.data()}};
    const std::optional<MapEntryView> integerCell {MapEntryView {integerEntry.data()}};

    EXPECT_TRUE(nullCell == PropertyNull {});
    EXPECT_FALSE(integerCell == PropertyNull {});
}

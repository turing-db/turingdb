#include <gtest/gtest.h>

#include <optional>

#include "properties/PropertyContainer.h"

using namespace db;

// sort() rebuilds the entity ID map from the value and null ID lists, so a null recorded
// in neither is lost
TEST(TrivialPropertyContainerTest, PropertyNullSurvivesSort) {
    TypedPropertyContainer<types::Int64> container;

    container.add(EntityID(1), types::Int64::Primitive {7});
    container.add(EntityID(2), PropertyNull {});

    ASSERT_EQ(container.nullIds().size(), 1);

    container.sort();

    ASSERT_FALSE(container.tryGetWithNull(EntityID(2)).has_value());

    const std::optional<const types::Int64::Primitive*> value = container.tryGetWithNull(EntityID(1));
    ASSERT_TRUE(value.has_value());
    ASSERT_NE(value.value(), nullptr);
    EXPECT_EQ(*value.value(), 7);
}

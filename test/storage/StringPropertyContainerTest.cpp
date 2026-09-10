#include <gtest/gtest.h>

#include <optional>

#include "properties/PropertyContainer.h"

using namespace db;

TEST(StringPropertyContainerTest, ExplicitNullHoldsNoValue) {
    TypedPropertyContainer<types::String> container;

    container.add(EntityID(1), std::optional<types::String::Primitive> {"Remy"});
    container.add(EntityID(2), std::nullopt);

    ASSERT_EQ(container.size(), 1);

    ASSERT_TRUE(container.has(EntityID(1)));
    ASSERT_FALSE(container.has(EntityID(2)));
    ASSERT_FALSE(container.has(EntityID(3)));

    ASSERT_NE(container.tryGet(EntityID(1)), nullptr);
    ASSERT_EQ(container.tryGet(EntityID(2)), nullptr);
    ASSERT_EQ(container.tryGet(EntityID(3)), nullptr);
}

TEST(StringPropertyContainerTest, TryGetWithNullSeparatesNullFromAbsent) {
    TypedPropertyContainer<types::String> container;

    container.add(EntityID(1), std::optional<types::String::Primitive> {"Remy"});
    container.add(EntityID(2), std::nullopt);

    const std::optional<const types::String::Primitive*> value = container.tryGetWithNull(EntityID(1));
    ASSERT_TRUE(value.has_value());
    ASSERT_NE(value.value(), nullptr);
    ASSERT_EQ(*value.value(), "Remy");

    ASSERT_FALSE(container.tryGetWithNull(EntityID(2)).has_value());

    const std::optional<const types::String::Primitive*> absent = container.tryGetWithNull(EntityID(3));
    ASSERT_TRUE(absent.has_value());
    ASSERT_EQ(absent.value(), nullptr);
}

TEST(StringPropertyContainerTest, SortKeepsExplicitNull) {
    TypedPropertyContainer<types::String> container;

    container.add(EntityID(5), std::optional<types::String::Primitive> {"Adam"});
    container.add(EntityID(3), std::nullopt);
    container.add(EntityID(1), std::optional<types::String::Primitive> {"Remy"});

    container.sort();

    ASSERT_FALSE(container.has(EntityID(3)));
    ASSERT_EQ(container.tryGet(EntityID(3)), nullptr);
    ASSERT_FALSE(container.tryGetWithNull(EntityID(3)).has_value());

    ASSERT_NE(container.tryGet(EntityID(1)), nullptr);
    ASSERT_NE(container.tryGet(EntityID(5)), nullptr);
    ASSERT_EQ(*container.tryGet(EntityID(1)), "Remy");
    ASSERT_EQ(*container.tryGet(EntityID(5)), "Adam");
}

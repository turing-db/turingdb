#include <gtest/gtest.h>

#include <optional>
#include <vector>

#include "list/ListBuffer.h"
#include "list/ListView.h"
#include "properties/PropertyContainer.h"

#include "TuringException.h"

using namespace db;

namespace {

class ListPropertyContainerTest : public testing::Test {
protected:
    ListView list(std::vector<ListBuffer<>::ListItemVariant> elements) {
        return _buffer.insert(elements);
    }

    ListBuffer<> _buffer;
};

}

TEST_F(ListPropertyContainerTest, ExplicitNullHoldsNoValue) {
    TypedPropertyContainer<types::List> container;

    container.add(EntityID(1), list({int64_t {1}, int64_t {2}}));
    container.add(EntityID(2), std::nullopt);

    ASSERT_EQ(container.size(), 1);

    EXPECT_TRUE(container.has(EntityID(1)));
    EXPECT_FALSE(container.has(EntityID(2)));
    EXPECT_FALSE(container.has(EntityID(3)));
}

TEST_F(ListPropertyContainerTest, TryGetWithNullSeparatesNullFromAbsent) {
    TypedPropertyContainer<types::List> container;

    container.add(EntityID(1), list({int64_t {1}, int64_t {2}}));
    container.add(EntityID(2), std::nullopt);

    const std::optional<const types::List::Primitive*> value = container.tryGetWithNull(EntityID(1));
    ASSERT_TRUE(value.has_value());
    ASSERT_NE(value.value(), nullptr);
    EXPECT_EQ(value.value()->size(), 2);

    EXPECT_FALSE(container.tryGetWithNull(EntityID(2)).has_value());

    const std::optional<const types::List::Primitive*> absent = container.tryGetWithNull(EntityID(3));
    ASSERT_TRUE(absent.has_value());
    EXPECT_EQ(absent.value(), nullptr);
}

TEST_F(ListPropertyContainerTest, GetRefusesANullAndAnAbsentEntity) {
    TypedPropertyContainer<types::List> container;

    container.add(EntityID(1), list({int64_t {1}, int64_t {2}}));
    container.add(EntityID(2), std::nullopt);

    EXPECT_EQ(container.get(EntityID(1)).size(), 2);

    EXPECT_THROW(container.get(EntityID(2)), TuringException);
    EXPECT_THROW(container.get(EntityID(3)), TuringException);
}

TEST_F(ListPropertyContainerTest, SortKeepsExplicitNull) {
    TypedPropertyContainer<types::List> container;

    container.add(EntityID(5), list({int64_t {5}}));
    container.add(EntityID(3), std::nullopt);
    container.add(EntityID(1), list({int64_t {1}}));

    container.sort();

    EXPECT_FALSE(container.has(EntityID(3)));
    EXPECT_FALSE(container.tryGetWithNull(EntityID(3)).has_value());

    const std::optional<const types::List::Primitive*> value = container.tryGetWithNull(EntityID(1));
    ASSERT_TRUE(value.has_value());
    ASSERT_NE(value.value(), nullptr);
    EXPECT_EQ(value.value()->size(), 1);
}

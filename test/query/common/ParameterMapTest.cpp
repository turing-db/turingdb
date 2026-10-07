#include <gtest/gtest.h>

#include "ParameterMap.h"
#include "ParameterValue.h"

using namespace db;

TEST(ParameterMapTest, getReturnsTheValueSet) {
    ParameterMap parameters;

    ParameterValue name;
    name.setString("Alice");
    parameters.set("name", name);

    const ParameterValue* value = parameters.get("name");
    ASSERT_NE(value, nullptr);
    ASSERT_EQ(value->getType(), ValueType::String);
    EXPECT_EQ(value->getString(), "Alice");
}

TEST(ParameterMapTest, getReturnsNullForAnUnknownName) {
    ParameterMap parameters;

    EXPECT_EQ(parameters.get("missing"), nullptr);
}

TEST(ParameterMapTest, setReplacesThePreviousValue) {
    ParameterMap parameters;

    ParameterValue first;
    first.setString("Alice");
    parameters.set("x", first);

    ParameterValue second;
    second.setInt64(42);
    parameters.set("x", second);

    const ParameterValue* value = parameters.get("x");
    ASSERT_NE(value, nullptr);
    ASSERT_EQ(value->getType(), ValueType::Int64);
    EXPECT_EQ(value->getInt64(), 42);
    EXPECT_EQ(parameters.getValues().size(), 1);
}

TEST(ParameterMapTest, clearRemovesEveryValue) {
    ParameterMap parameters;

    ParameterValue value;
    value.setBool(true);
    parameters.set("a", value);
    parameters.set("b", value);

    parameters.clear();

    EXPECT_TRUE(parameters.empty());
    EXPECT_EQ(parameters.get("a"), nullptr);
}

TEST(ParameterMapTest, valueIsNullByDefault) {
    const ParameterValue value;

    EXPECT_TRUE(value.isNull());
}

TEST(ParameterMapTest, listAndMapNest) {
    ParameterValue value;

    ParameterValue::Map& map = value.setMap();
    ParameterValue::List& ids = map["ids"].setList();
    ids.emplace_back().setInt64(1);
    ids.emplace_back().setDouble(2.5);
    ids.emplace_back();
    map["active"].setBool(false);

    ParameterMap parameters;
    parameters.set("filter", value);

    const ParameterValue* filter = parameters.get("filter");
    ASSERT_NE(filter, nullptr);
    ASSERT_EQ(filter->getType(), ValueType::Map);

    const ParameterValue::Map& filterMap = filter->getMap();
    ASSERT_EQ(filterMap.size(), 2);
    EXPECT_FALSE(filterMap.at("active").getBool());

    const ParameterValue::List& filterIds = filterMap.at("ids").getList();
    ASSERT_EQ(filterIds.size(), 3);
    EXPECT_EQ(filterIds[0].getInt64(), 1);
    EXPECT_EQ(filterIds[1].getDouble(), 2.5);
    EXPECT_TRUE(filterIds[2].isNull());
}

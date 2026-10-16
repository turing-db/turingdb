#include <gtest/gtest.h>

#include <stddef.h>
#include <type_traits>
#include <utility>

#include "ParameterValue.h"

using namespace db;

namespace {

void buildNestedValue(ParameterValue& value) {
    ParameterValue::Map& map = value.setMap();

    ParameterValue::List& list = map["a"].setList();
    list.resize(3);
    list[0].setInt64(1);
    list[1].setString("x");
    list[2].setMap()["b"].setNull();

    map["c"].setDouble(2.5);
}

void expectNestedValue(const ParameterValue& value) {
    ASSERT_EQ(value.getType(), ValueType::Map);
    const ParameterValue::Map& map = value.getMap();
    ASSERT_EQ(map.size(), 2u);

    const ParameterValue::List& list = map.at("a").getList();
    ASSERT_EQ(list.size(), 3u);
    EXPECT_EQ(list[0].getInt64(), 1);
    EXPECT_EQ(list[1].getString(), "x");
    EXPECT_TRUE(list[2].getMap().at("b").isNull());

    EXPECT_EQ(map.at("c").getDouble(), 2.5);
}

}

TEST(ParameterValueTest, aCopyHoldsEveryNestedValue) {
    ParameterValue value;
    buildNestedValue(value);

    const ParameterValue copy(value);

    expectNestedValue(copy);
    expectNestedValue(value);
}

TEST(ParameterValueTest, anAssignmentReplacesTheValue) {
    ParameterValue source;
    buildNestedValue(source);

    ParameterValue target;
    target.setList().resize(4);
    target = source;

    expectNestedValue(target);
}

TEST(ParameterValueTest, aValueCanTakeOneOfItsElements) {
    ParameterValue value;
    ParameterValue::List& list = value.setList();
    list.resize(1);
    buildNestedValue(list[0]);

    value = value.getList()[0];

    expectNestedValue(value);
}

TEST(ParameterValueTest, aMoveTakesTheValueAndLeavesNull) {
    static_assert(std::is_nothrow_move_constructible_v<ParameterValue>);
    static_assert(std::is_nothrow_move_assignable_v<ParameterValue>);

    ParameterValue source;
    buildNestedValue(source);

    const ParameterValue moved(std::move(source));

    expectNestedValue(moved);
    EXPECT_TRUE(source.isNull());
}

TEST(ParameterValueTest, aMoveAssignmentReplacesTheValue) {
    ParameterValue source;
    buildNestedValue(source);

    ParameterValue target;
    target.setList().resize(4);
    target = std::move(source);

    expectNestedValue(target);
    EXPECT_TRUE(source.isNull());
}

TEST(ParameterValueTest, aDeepValueIsCopiedAndDestroyed) {
    constexpr size_t DEPTH = 1000000;

    ParameterValue value;
    ParameterValue* level = &value;
    for (size_t depth = 0; depth < DEPTH; depth++) {
        ParameterValue::List& list = level->setList();
        list.resize(1);
        level = &list[0];
    }
    level->setInt64(7);

    const ParameterValue copy(value);

    const ParameterValue* copiedLevel = &copy;
    for (size_t depth = 0; depth < DEPTH; depth++) {
        ASSERT_EQ(copiedLevel->getList().size(), 1u);
        copiedLevel = &copiedLevel->getList()[0];
    }
    EXPECT_EQ(copiedLevel->getInt64(), 7);
}

TEST(ParameterValueTest, aDeepValueIsMovedOverAnotherDeepValue) {
    constexpr size_t DEPTH = 1000000;

    ParameterValue first;
    ParameterValue second;
    ParameterValue* firstLevel = &first;
    ParameterValue* secondLevel = &second;
    for (size_t depth = 0; depth < DEPTH; depth++) {
        ParameterValue::List& firstList = firstLevel->setList();
        firstList.resize(1);
        firstLevel = &firstList[0];

        ParameterValue::List& secondList = secondLevel->setList();
        secondList.resize(1);
        secondLevel = &secondList[0];
    }
    secondLevel->setInt64(7);

    first = std::move(second);

    const ParameterValue* level = &first;
    for (size_t depth = 0; depth < DEPTH; depth++) {
        level = &level->getList()[0];
    }
    EXPECT_EQ(level->getInt64(), 7);
}

#include <gtest/gtest.h>

#include <stdint.h>
#include <initializer_list>
#include <limits>
#include <optional>
#include <string>
#include <vector>

#include "LocalMemory.h"
#include "ParameterValue.h"
#include "ParameterValueReader.h"
#include "TuringException.h"
#include "columns/ColumnConst.h"
#include "columns/ColumnOptVector.h"
#include "columns/ColumnVector.h"
#include "list/ListBuffer.h"
#include "map/MapBuffer.h"
#include "metadata/PropertyNull.h"

using namespace db;

namespace {

using Int64 = types::Int64::Primitive;
using UInt64 = types::UInt64::Primitive;
using String = types::String::Primitive;

}

class ParameterValueReaderTest : public ::testing::Test {
protected:
    template <typename T>
    ColumnVector<T>* column(std::initializer_list<T> values) {
        ColumnVector<T>* column = _memory.alloc<ColumnVector<T>>();
        for (const T& value : values) {
            column->push_back(value);
        }

        return column;
    }

    LocalMemory _memory;
    ListBuffer<> _lists;
    MapBuffer<> _maps;
};

TEST_F(ParameterValueReaderTest, scalarsKeepTheirTypes) {
    ParameterValue value;

    readParameterValue(column<Int64>({41, 42}), 1, value);
    EXPECT_EQ(value.getInt64(), 42);

    readParameterValue(column<UInt64>({7}), 0, value);
    EXPECT_EQ(value.getInt64(), 7);

    readParameterValue(column<types::Double::Primitive>({1.5}), 0, value);
    EXPECT_EQ(value.getDouble(), 1.5);

    readParameterValue(column<types::Bool::Primitive>({CustomBool {true}}), 0, value);
    EXPECT_TRUE(value.getBool());

    readParameterValue(column<String>({"Remy"}), 0, value);
    EXPECT_EQ(value.getString(), "Remy");

    readParameterValue(column<types::Duration::Primitive>({Duration {3'600'000'000}}), 0, value);
    EXPECT_EQ(value.getDuration(), Duration {3'600'000'000});

    readParameterValue(column<types::DateTime::Primitive>({DateTime {1'793'887'500'000'000}}), 0, value);
    EXPECT_EQ(value.getDateTime(), DateTime {1'793'887'500'000'000});
}

TEST_F(ParameterValueReaderTest, aNullableColumnReadsItsValueOrNull) {
    ParameterValue value;

    ColumnOptVector<Int64>* maybe = _memory.alloc<ColumnOptVector<Int64>>();
    maybe->push_back(Int64 {5});
    maybe->pushNull();

    readParameterValue(maybe, 0, value);
    EXPECT_EQ(value.getInt64(), 5);

    readParameterValue(maybe, 1, value);
    EXPECT_TRUE(value.isNull());
}

TEST_F(ParameterValueReaderTest, aNullIsNull) {
    ParameterValue value;

    ColumnConst<std::optional<Int64>>* nothing = _memory.alloc<ColumnConst<std::optional<Int64>>>();
    nothing->setOptional(std::nullopt);
    readParameterValue(nothing, 0, value);
    EXPECT_TRUE(value.isNull());

    ColumnConst<std::optional<Int64>>* something = _memory.alloc<ColumnConst<std::optional<Int64>>>();
    something->setOptional(std::optional<Int64> {3});
    readParameterValue(something, 0, value);
    EXPECT_EQ(value.getInt64(), 3);

    ColumnConst<PropertyNull>* null = _memory.alloc<ColumnConst<PropertyNull>>();
    null->set(PropertyNull {});
    readParameterValue(null, 0, value);
    EXPECT_TRUE(value.isNull());
}

TEST_F(ParameterValueReaderTest, listsAndMapsNest) {
    std::vector<ListBuffer<>::ListItemVariant> inner;
    inner.emplace_back(Int64 {1});
    inner.emplace_back(String {"a"});
    const ListView innerView = _lists.insert(inner);

    std::vector<MapBuffer<>::MapKeyValuePair> entries;
    entries.push_back({.key = "n", .value = Int64 {2}});
    entries.push_back({.key = "none", .value = PropertyNull {}});
    const MapView mapView = _maps.insert(entries);

    std::vector<ListBuffer<>::ListItemVariant> outer;
    outer.emplace_back(innerView);
    outer.emplace_back(mapView);
    outer.emplace_back(Duration {5});
    const ListView outerView = _lists.insert(outer);

    ColumnConst<ListView>* list = _memory.alloc<ColumnConst<ListView>>();
    list->set(outerView);

    ParameterValue value;
    readParameterValue(list, 0, value);

    ASSERT_EQ(value.getType(), ValueType::List);
    const ParameterValue::List& elements = value.getList();
    ASSERT_EQ(elements.size(), 3u);

    const ParameterValue::List& innerElements = elements[0].getList();
    ASSERT_EQ(innerElements.size(), 2u);
    EXPECT_EQ(innerElements[0].getInt64(), 1);
    EXPECT_EQ(innerElements[1].getString(), "a");

    const ParameterValue::Map& map = elements[1].getMap();
    ASSERT_EQ(map.size(), 2u);
    EXPECT_EQ(map.at("n").getInt64(), 2);
    EXPECT_TRUE(map.at("none").isNull());

    EXPECT_EQ(elements[2].getDuration(), Duration {5});
}

TEST_F(ParameterValueReaderTest, whatNoParameterHoldsIsRefused) {
    ParameterValue value;

    EXPECT_THROW(readParameterValue(column<NodeID>({NodeID {1}}), 0, value), TuringException);
    EXPECT_THROW(readParameterValue(column<UInt64>({UInt64 {1} << 63}), 0, value), TuringException);

    std::vector<ListBuffer<>::ListItemVariant> elements;
    elements.emplace_back(EdgeID {1});
    ColumnConst<ListView>* list = _memory.alloc<ColumnConst<ListView>>();
    list->set(_lists.insert(elements));

    try {
        readParameterValue(list, 0, value);
        ADD_FAILURE() << "a list holding an edge was accepted";
    } catch (const TuringException& error) {
        EXPECT_EQ(std::string(error.what()), "A parameter cannot hold an edge");
    }
}

TEST_F(ParameterValueReaderTest, aDeeplyNestedListIsRead) {
    constexpr size_t DEPTH = 100'000;

    std::vector<ListBuffer<>::ListItemVariant> elements;
    elements.emplace_back(Int64 {1});
    ListView view = _lists.insert(elements);

    for (size_t level = 1; level < DEPTH; level++) {
        elements.clear();
        elements.emplace_back(view);
        view = _lists.insert(elements);
    }

    ColumnConst<ListView>* list = _memory.alloc<ColumnConst<ListView>>();
    list->set(view);

    ParameterValue value;
    readParameterValue(list, 0, value);

    size_t depth = 0;
    const ParameterValue* current = &value;
    while (current->getType() == ValueType::List) {
        ASSERT_EQ(current->getList().size(), 1u);
        current = &current->getList().front();
        depth++;
    }

    EXPECT_EQ(depth, DEPTH);
    EXPECT_EQ(current->getInt64(), 1);
}

TEST_F(ParameterValueReaderTest, theInt64BoundsAreRead) {
    ParameterValue value;

    readParameterValue(column<UInt64>({static_cast<UInt64>(std::numeric_limits<int64_t>::max())}), 0, value);
    EXPECT_EQ(value.getInt64(), std::numeric_limits<int64_t>::max());

    readParameterValue(column<Int64>({std::numeric_limits<int64_t>::min()}), 0, value);
    EXPECT_EQ(value.getInt64(), std::numeric_limits<int64_t>::min());

    try {
        readParameterValue(column<UInt64>({UInt64 {1} << 63}), 0, value);
        ADD_FAILURE() << "an integer beyond Int64 was accepted";
    } catch (const TuringException& error) {
        EXPECT_EQ(std::string(error.what()), "Integer 9223372036854775808 exceeds the Int64 range");
    }
}

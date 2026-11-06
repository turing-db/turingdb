#include <gtest/gtest.h>

#include <stddef.h>
#include <vector>

#include "ParameterMap.h"
#include "ParameterValue.h"

#include "CallV3Test.h"
#include "StringRowSink.h"

using namespace db;
using namespace turing::test;

class ListAndMapParameterTest : public CallV3Test {
};

TEST_F(ListAndMapParameterTest, aListParameterUnwindsToItsElements) {
    ParameterMap parameters;
    ParameterValue::List& ids = parameters.insert("ids").setList();
    ids.emplace_back().setInt64(1);
    ids.emplace_back().setInt64(2);
    ids.emplace_back().setInt64(3);

    StringRowSink sink;
    runQuery("UNWIND $ids AS id RETURN id + 1", parameters, sink);

    const std::vector<StringRowSink::Row> expected {{"2"}, {"3"}, {"4"}};
    EXPECT_EQ(sink.getRows(), expected);
}

TEST_F(ListAndMapParameterTest, aListParameterIsReadAsAList) {
    ParameterMap parameters;
    ParameterValue::List& names = parameters.insert("names").setList();
    names.emplace_back().setString("a");
    names.emplace_back().setString("b");

    StringRowSink sink;
    runQuery("RETURN size($names), $names[0], $names[1] + '!'", parameters, sink);

    const std::vector<StringRowSink::Row> expected {{"2", "a", "b!"}};
    EXPECT_EQ(sink.getRows(), expected);
}

TEST_F(ListAndMapParameterTest, aListParameterFiltersWithIn) {
    ParameterMap parameters;
    ParameterValue::List& names = parameters.insert("names").setList();
    names.emplace_back().setString("Remy");
    names.emplace_back().setString("Adam");

    StringRowSink sink;
    runQuery("MATCH (p) WHERE p.name IN $names RETURN p.name ORDER BY p.name", parameters, sink);

    const std::vector<StringRowSink::Row> expected {{"Adam"}, {"Remy"}};
    EXPECT_EQ(sink.getRows(), expected);
}

TEST_F(ListAndMapParameterTest, aListOfListsUnwindsTwice) {
    ParameterMap parameters;
    ParameterValue::List& rows = parameters.insert("rows").setList();
    rows.emplace_back().setList().emplace_back().setInt64(1);
    ParameterValue::List& second = rows.emplace_back().setList();
    second.emplace_back().setInt64(2);
    second.emplace_back().setInt64(3);

    StringRowSink sink;
    runQuery("UNWIND $rows AS row UNWIND row AS x RETURN x", parameters, sink);

    const std::vector<StringRowSink::Row> expected {{"1"}, {"2"}, {"3"}};
    EXPECT_EQ(sink.getRows(), expected);
}

TEST_F(ListAndMapParameterTest, aMixedListUnwindsToEachValue) {
    ParameterMap parameters;
    ParameterValue::List& mixed = parameters.insert("mixed").setList();
    mixed.emplace_back().setInt64(1);
    mixed.emplace_back().setString("a");
    mixed.emplace_back().setNull();

    StringRowSink sink;
    runQuery("UNWIND $mixed AS x RETURN x", parameters, sink);

    const std::vector<StringRowSink::Row> expected {{"1"}, {"a"}, {"null"}};
    EXPECT_EQ(sink.getRows(), expected);
}

TEST_F(ListAndMapParameterTest, aMapParameterIsReadByKey) {
    ParameterMap parameters;
    ParameterValue::Map& entries = parameters.insert("m").setMap();
    entries["name"].setString("Remy");
    entries["age"].setInt64(30);

    StringRowSink sink;
    runQuery("RETURN $m.name, $m.age + 1", parameters, sink);

    const std::vector<StringRowSink::Row> expected {{"Remy", "31"}};
    EXPECT_EQ(sink.getRows(), expected);

    StringRowSink keys;
    runQuery("UNWIND keys($m) AS k RETURN k", parameters, keys);

    const std::vector<StringRowSink::Row> expectedKeys {{"age"}, {"name"}};
    EXPECT_EQ(keys.getRows(), expectedKeys);
}

TEST_F(ListAndMapParameterTest, aMapInsideAListIsReadByKey) {
    ParameterMap parameters;
    parameters.insert("deep").setList().emplace_back().setMap()["k"].setInt64(7);

    StringRowSink sink;
    runQuery("UNWIND $deep AS entry RETURN entry.k", parameters, sink);

    const std::vector<StringRowSink::Row> expected {{"7"}};
    EXPECT_EQ(sink.getRows(), expected);
}

TEST_F(ListAndMapParameterTest, aDeeplyNestedListBindsWithoutRecursion) {
    constexpr size_t DEPTH = 100'000;

    ParameterMap parameters;
    ParameterValue::List* level = &parameters.insert("deep").setList();
    for (size_t nesting = 1; nesting < DEPTH; nesting++) {
        level = &level->emplace_back().setList();
    }

    StringRowSink sink;
    runQuery("RETURN size($deep)", parameters, sink);

    const std::vector<StringRowSink::Row> expected {{"1"}};
    EXPECT_EQ(sink.getRows(), expected);
}

#include <gtest/gtest.h>

#include <stddef.h>
#include <stdint.h>
#include <limits>
#include <string>

#include <nlohmann/json.hpp>

#include "ParameterJson.h"
#include "ParameterValue.h"
#include "TuringException.h"

using namespace db;

namespace {

void readParameter(const std::string& text, ParameterValue& value) {
    const nlohmann::json json = nlohmann::json::parse(text);
    readJsonParameter(json, value);
}

}

TEST(ParameterJsonTest, nestedValuesKeepTheirTypes) {
    ParameterValue value;
    readParameter(R"({"a": [1, "x", {"b": null}], "c": 2.5, "d": true})", value);

    ASSERT_EQ(value.getType(), ValueType::Map);
    const ParameterValue::Map& map = value.getMap();
    ASSERT_EQ(map.size(), 3u);

    const ParameterValue::List& list = map.at("a").getList();
    ASSERT_EQ(list.size(), 3u);
    EXPECT_EQ(list[0].getInt64(), 1);
    EXPECT_EQ(list[1].getString(), "x");
    EXPECT_TRUE(list[2].getMap().at("b").isNull());

    EXPECT_EQ(map.at("c").getDouble(), 2.5);
    EXPECT_TRUE(map.at("d").getBool());
}

TEST(ParameterJsonTest, theInt64BoundsAreRead) {
    ParameterValue largest;
    readParameter("9223372036854775807", largest);
    EXPECT_EQ(largest.getInt64(), std::numeric_limits<int64_t>::max());

    ParameterValue smallest;
    readParameter("-9223372036854775808", smallest);
    EXPECT_EQ(smallest.getInt64(), std::numeric_limits<int64_t>::min());
}

TEST(ParameterJsonTest, anIntegerBeyondInt64IsRejected) {
    ParameterValue value;

    std::string message;
    try {
        readParameter("9223372036854775808", value);
    } catch (const TuringException& e) {
        message = e.what();
    }

    EXPECT_EQ(message, "Integer 9223372036854775808 exceeds the Int64 range");
}

TEST(ParameterJsonTest, aDeeplyNestedListIsRead) {
    constexpr size_t DEPTH = 400000;

    ParameterValue value;
    readParameter(std::string(DEPTH, '[') + std::string(DEPTH, ']'), value);

    size_t depth = 0;
    const ParameterValue* current = &value;
    while (current->getType() == ValueType::List && !current->getList().empty()) {
        current = &current->getList().front();
        depth++;
    }

    EXPECT_EQ(depth, DEPTH - 1);
    EXPECT_EQ(current->getType(), ValueType::List);
}

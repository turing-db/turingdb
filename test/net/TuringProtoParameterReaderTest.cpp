#include <gtest/gtest.h>

#include <stddef.h>
#include <stdint.h>
#include <string.h>
#include <string>
#include <vector>

#include "ParameterMap.h"
#include "ParameterValue.h"
#include "ProtocolException.h"
#include "TuringException.h"
#include "TuringProtoInBuf.h"
#include "TuringProtoNestedWriter.h"
#include "TuringProtoOutBuf.h"
#include "TuringProtoParameterReader.h"
#include "TuringProtoParameterWriter.h"
#include "map/MapBuffer.h"

using namespace db;
using namespace net::proto;

namespace {

constexpr size_t BUFFER_CAPACITY = 16ul * 1024 * 1024;

}

class TuringProtoParameterReaderTest : public ::testing::Test {
protected:
    void write(const ParameterMap& parameters) {
        TuringProtoParameterWriter writer(&_outBuf);
        writer.writeParameters(parameters);
    }

    void read(size_t byteCount, ParameterMap& parameters) {
        TuringProtoInBuf inBuf(byteCount);
        memcpy(inBuf.data(), _outBuf.data(), byteCount);
        inBuf.increaseWriteOffset(byteCount);

        readParameters(&inBuf, parameters);
        EXPECT_EQ(inBuf.readable(), 0u);
    }

    void roundTrip(const ParameterMap& source, ParameterMap& decoded) {
        write(source);
        read(_outBuf.size(), decoded);
    }

    TuringProtoOutBuf _outBuf {BUFFER_CAPACITY};
};

TEST_F(TuringProtoParameterReaderTest, scalarsComeBackWithTheirTypes) {
    const std::string longText(200, 'x');

    ParameterMap source;
    ParameterValue value;
    value.setString("Remy");
    source.set("name", value);
    value.setString(longText);
    source.set("long", value);
    value.setString("");
    source.set("empty", value);
    value.setInt64(-32);
    source.set("age", value);
    value.setDouble(1.5);
    source.set("score", value);
    value.setBool(true);
    source.set("active", value);
    value.setNull();
    source.set("missing", value);

    ParameterMap decoded;
    roundTrip(source, decoded);

    ASSERT_EQ(decoded.getValues().size(), 7u);
    EXPECT_EQ(decoded.get("name")->getString(), "Remy");
    EXPECT_EQ(decoded.get("long")->getString(), longText);
    EXPECT_EQ(decoded.get("empty")->getString(), "");
    EXPECT_EQ(decoded.get("age")->getInt64(), -32);
    EXPECT_EQ(decoded.get("score")->getDouble(), 1.5);
    EXPECT_TRUE(decoded.get("active")->getBool());
    EXPECT_TRUE(decoded.get("missing")->isNull());
}

TEST_F(TuringProtoParameterReaderTest, listsAndMapsNest) {
    ParameterValue labels;
    ParameterValue::List& labelList = labels.setList();
    labelList.resize(3);
    labelList[0].setString("Person");
    ParameterValue::List& innerList = labelList[1].setList();
    innerList.resize(2);
    innerList[0].setString("a");
    innerList[1].setInt64(1);
    labelList[2].setMap()["k"].setBool(false);

    ParameterValue properties;
    ParameterValue::Map& propertyMap = properties.setMap();
    propertyMap["b"].setMap()["c"].setString("deep");
    propertyMap["a"].setList();
    propertyMap[""].setInt64(5);

    ParameterMap source;
    source.set("labels", labels);
    source.set("properties", properties);

    ParameterMap decoded;
    roundTrip(source, decoded);
    ASSERT_EQ(decoded.getValues().size(), 2u);

    const ParameterValue::List& decodedLabels = decoded.get("labels")->getList();
    ASSERT_EQ(decodedLabels.size(), 3u);
    EXPECT_EQ(decodedLabels[0].getString(), "Person");
    const ParameterValue::List& decodedInner = decodedLabels[1].getList();
    ASSERT_EQ(decodedInner.size(), 2u);
    EXPECT_EQ(decodedInner[0].getString(), "a");
    EXPECT_EQ(decodedInner[1].getInt64(), 1);
    EXPECT_FALSE(decodedLabels[2].getMap().at("k").getBool());

    const ParameterValue::Map& decodedProperties = decoded.get("properties")->getMap();
    ASSERT_EQ(decodedProperties.size(), 3u);
    EXPECT_EQ(decodedProperties.at("").getInt64(), 5);
    EXPECT_TRUE(decodedProperties.at("a").getList().empty());
    EXPECT_EQ(decodedProperties.at("b").getMap().at("c").getString(), "deep");
}

TEST_F(TuringProtoParameterReaderTest, anEmptyMapReadsAsNoParameters) {
    const ParameterMap source;

    ParameterMap decoded;
    roundTrip(source, decoded);

    EXPECT_TRUE(decoded.empty());
}

TEST_F(TuringProtoParameterReaderTest, aDeeplyNestedParameterIsRead) {
    constexpr size_t DEPTH = 100000;

    ParameterValue value;
    ParameterValue* level = &value;
    for (size_t depth = 0; depth < DEPTH; depth++) {
        ParameterValue::List& list = level->setList();
        list.resize(1);
        level = &list[0];
    }
    level->setString("bottom");

    ParameterMap source;
    source.set("deep", value);

    ParameterMap decoded;
    roundTrip(source, decoded);

    const ParameterValue* decodedLevel = decoded.get("deep");
    for (size_t depth = 0; depth < DEPTH; depth++) {
        ASSERT_EQ(decodedLevel->getList().size(), 1u);
        decodedLevel = &decodedLevel->getList()[0];
    }
    EXPECT_EQ(decodedLevel->getString(), "bottom");
}

TEST_F(TuringProtoParameterReaderTest, aMapThatEndsEarlyIsRejected) {
    ParameterValue list;
    ParameterValue::List& elements = list.setList();
    elements.resize(2);
    elements[0].setString("first");
    elements[1].setInt64(2);

    ParameterMap source;
    source.set("p", list);
    write(source);

    for (size_t byteCount = 0; byteCount < _outBuf.size(); byteCount++) {
        ParameterMap decoded;
        EXPECT_THROW(read(byteCount, decoded), ProtocolException) << byteCount;
    }
}

TEST_F(TuringProtoParameterReaderTest, aStoredTypeAParameterCannotHoldIsRejected) {
    MapBuffer<> mapBuffer;
    const std::vector<MapBuffer<>::MapKeyValuePair> entries = {{"count", types::UInt64::Primitive {7}}};

    NestedContainerWriter writer(&_outBuf);
    writer.writeMapView(mapBuffer.insert(entries));

    ParameterMap decoded;
    try {
        read(_outBuf.size(), decoded);
        FAIL() << "A UInt64 value was read into a parameter";
    } catch (const TuringException& e) {
        EXPECT_STREQ(e.what(), "A parameter cannot hold a UInt64 value");
    }
}

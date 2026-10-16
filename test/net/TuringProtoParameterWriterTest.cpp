#include <gtest/gtest.h>

#include <stddef.h>
#include <string.h>
#include <string>
#include <string_view>

#include "ChunkedBuffer.h"
#include "DecodeContext.h"
#include "Decoders.h"
#include "LocalMemory.h"
#include "ParameterMap.h"
#include "ParameterValue.h"
#include "TuringProtoInBuf.h"
#include "TuringProtoOutBuf.h"
#include "TuringProtoParameterWriter.h"
#include "TuringSink.h"
#include "columns/ColumnConst.h"
#include "list/ListBuffer.h"
#include "map/MapBuffer.h"

using namespace db;
using namespace net::proto;

namespace {

constexpr size_t BUFFER_CAPACITY = 16ul * 1024 * 1024;

using Int64 = types::Int64::Primitive;
using Double = types::Double::Primitive;
using Bool = types::Bool::Primitive;
using String = types::String::Primitive;

}

class TuringProtoParameterWriterTest : public ::testing::Test {
protected:
    void write(const ParameterMap& parameters) {
        TuringProtoParameterWriter writer(&_outBuf);
        writer.writeParameters(parameters);
    }

    void decode(MapView& view) {
        TuringProtoInBuf inBuf(_outBuf.size());
        memcpy(inBuf.data(), _outBuf.data(), _outBuf.size());
        inBuf.increaseWriteOffset(_outBuf.size());

        DecodeContext context;
        context._inBuf = &inBuf;

        TuringSink sink(&_localMemory, &_embeddingBuffer, &_listBuffer, &_mapBuffer);
        ColumnConst<MapView>* column = _localMemory.alloc<ColumnConst<MapView>>();

        ASSERT_TRUE((ConstColumnDecoder<MapView, TuringSink>::decode(&context, &sink, column)));
        EXPECT_EQ(inBuf.readable(), 0u);

        view = column->getRaw();
    }

    TuringProtoOutBuf _outBuf {BUFFER_CAPACITY};
    LocalMemory _localMemory;
    ChunkedBuffer<float> _embeddingBuffer;
    ListBuffer<> _listBuffer;
    MapBuffer<> _mapBuffer;
};

TEST_F(TuringProtoParameterWriterTest, scalarsKeepTheirTypesUnderSortedNames) {
    ParameterMap parameters;
    ParameterValue value;

    value.setString("Remy");
    parameters.set("name", value);
    value.setInt64(-32);
    parameters.set("age", value);
    value.setDouble(1.5);
    parameters.set("score", value);
    value.setBool(true);
    parameters.set("active", value);
    value.setNull();
    parameters.set("missing", value);

    write(parameters);

    MapView view;
    decode(view);
    ASSERT_EQ(view.size(), 5u);

    auto entry = view.begin();
    EXPECT_EQ(entry->getKey(), "active");
    ASSERT_EQ(entry->getValueTag(), MapBufferTypeTag::Bool);
    EXPECT_TRUE(entry->getValueAs<Bool>());
    ++entry;
    EXPECT_EQ(entry->getKey(), "age");
    ASSERT_EQ(entry->getValueTag(), MapBufferTypeTag::Int);
    EXPECT_EQ(entry->getValueAs<Int64>(), -32);
    ++entry;
    EXPECT_EQ(entry->getKey(), "missing");
    EXPECT_EQ(entry->getValueTag(), MapBufferTypeTag::Null);
    ++entry;
    EXPECT_EQ(entry->getKey(), "name");
    ASSERT_EQ(entry->getValueTag(), MapBufferTypeTag::String);
    EXPECT_EQ(entry->getValueAs<String>(), "Remy");
    ++entry;
    EXPECT_EQ(entry->getKey(), "score");
    ASSERT_EQ(entry->getValueTag(), MapBufferTypeTag::Double);
    EXPECT_EQ(entry->getValueAs<Double>(), 1.5);
}

TEST_F(TuringProtoParameterWriterTest, listsAndMapsNest) {
    ParameterValue labels;
    ParameterValue::List& labelList = labels.setList();
    labelList.resize(2);
    labelList[0].setString("Person");
    ParameterValue::List& innerList = labelList[1].setList();
    innerList.resize(2);
    innerList[0].setString("a");
    innerList[1].setInt64(1);

    ParameterValue properties;
    ParameterValue::Map& propertyMap = properties.setMap();
    propertyMap["b"].setMap()["c"].setInt64(2);
    propertyMap["a"].setList();

    ParameterMap parameters;
    parameters.set("labels", labels);
    parameters.set("properties", properties);

    write(parameters);

    MapView view;
    decode(view);
    ASSERT_EQ(view.size(), 2u);

    const MapEntryView labelsEntry = view.front();
    EXPECT_EQ(labelsEntry.getKey(), "labels");
    ASSERT_EQ(labelsEntry.getValueTag(), MapBufferTypeTag::ListView);

    const ListView decodedLabels = labelsEntry.getValueAs<ListView>();
    ASSERT_EQ(decodedLabels.size(), 2u);
    ASSERT_EQ(decodedLabels.front().getTag(), ListBufferTypeTag::String);
    EXPECT_EQ(decodedLabels.front().getAs<String>(), "Person");
    ASSERT_EQ(decodedLabels.back().getTag(), ListBufferTypeTag::ListView);

    const ListView decodedInner = decodedLabels.back().getAs<ListView>();
    ASSERT_EQ(decodedInner.size(), 2u);
    EXPECT_EQ(decodedInner.front().getAs<String>(), "a");
    ASSERT_EQ(decodedInner.back().getTag(), ListBufferTypeTag::Int);
    EXPECT_EQ(decodedInner.back().getAs<Int64>(), 1);

    const MapEntryView propertiesEntry = view.back();
    EXPECT_EQ(propertiesEntry.getKey(), "properties");
    ASSERT_EQ(propertiesEntry.getValueTag(), MapBufferTypeTag::MapView);

    const MapView decodedProperties = propertiesEntry.getValueAs<MapView>();
    ASSERT_EQ(decodedProperties.size(), 2u);
    EXPECT_EQ(decodedProperties.front().getKey(), "a");
    ASSERT_EQ(decodedProperties.front().getValueTag(), MapBufferTypeTag::ListView);
    EXPECT_TRUE(decodedProperties.front().getValueAs<ListView>().empty());
    EXPECT_EQ(decodedProperties.back().getKey(), "b");
    ASSERT_EQ(decodedProperties.back().getValueTag(), MapBufferTypeTag::MapView);

    const MapView decodedC = decodedProperties.back().getValueAs<MapView>();
    ASSERT_EQ(decodedC.size(), 1u);
    EXPECT_EQ(decodedC.front().getKey(), "c");
    EXPECT_EQ(decodedC.front().getValueAs<Int64>(), 2);
}

TEST_F(TuringProtoParameterWriterTest, anEmptyMapIsAnEmptyMapValue) {
    const ParameterMap parameters;
    write(parameters);

    MapView view;
    decode(view);
    EXPECT_TRUE(view.empty());
}

TEST_F(TuringProtoParameterWriterTest, aWriterWritesTheSameBytesTwice) {
    ParameterValue value;
    ParameterValue::List& list = value.setList();
    list.resize(1);
    list[0].setMap()["x"].setString("y");

    ParameterMap parameters;
    parameters.set("p", value);

    TuringProtoParameterWriter writer(&_outBuf);
    writer.writeParameters(parameters);
    const std::string first(_outBuf.data(), _outBuf.size());

    _outBuf.reset();
    writer.writeParameters(parameters);
    const std::string second(_outBuf.data(), _outBuf.size());

    EXPECT_EQ(first, second);
}

TEST_F(TuringProtoParameterWriterTest, aDeeplyNestedParameterIsWritten) {
    constexpr size_t DEPTH = 100000;

    ParameterValue value;
    ParameterValue* level = &value;
    for (size_t depth = 0; depth < DEPTH; depth++) {
        ParameterValue::List& list = level->setList();
        list.resize(1);
        level = &list[0];
    }
    level->setInt64(7);

    ParameterMap parameters;
    parameters.set("deep", value);

    write(parameters);

    MapView view;
    decode(view);
    ASSERT_EQ(view.size(), 1u);
    ASSERT_EQ(view.front().getValueTag(), MapBufferTypeTag::ListView);

    ListView list = view.front().getValueAs<ListView>();
    for (size_t depth = 1; depth < DEPTH; depth++) {
        ASSERT_EQ(list.size(), 1u);
        ASSERT_EQ(list.front().getTag(), ListBufferTypeTag::ListView);
        list = list.front().getAs<ListView>();
    }

    ASSERT_EQ(list.size(), 1u);
    EXPECT_EQ(list.front().getAs<Int64>(), 7);
}

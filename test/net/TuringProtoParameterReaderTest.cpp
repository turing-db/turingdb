#include <gtest/gtest.h>

#include <stddef.h>
#include <stdint.h>
#include <string.h>
#include <algorithm>
#include <limits>
#include <string>
#include <string_view>
#include <vector>

#include "CellTag.h"
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
#include "metadata/Point.h"

using namespace db;
using namespace net::proto;

namespace {

constexpr size_t BUFFER_CAPACITY = 16ul * 1024 * 1024;

// The map {pt: <point>}, the point as x, y and z, its CRS ordinal and 7 bytes of padding
void writePointMap(double x, double y, double z, uint8_t crs, uint8_t padding, std::vector<uint8_t>& bytes) {
    const auto append = [&bytes](const void* data, size_t size) {
        const uint8_t* begin = static_cast<const uint8_t*>(data);
        bytes.insert(bytes.end(), begin, begin + size);
    };

    const uint32_t entryCount = 1;
    const uint32_t byteSize = 0;
    const uint32_t keyLength = 2;
    const CellTag tag = CellTag::Point;

    append(&entryCount, sizeof(entryCount));
    append(&byteSize, sizeof(byteSize));
    append(&keyLength, sizeof(keyLength));
    append("pt", keyLength);
    append(&tag, sizeof(tag));
    append(&x, sizeof(x));
    append(&y, sizeof(y));
    append(&z, sizeof(z));
    append(&crs, sizeof(crs));
    bytes.insert(bytes.end(), 7, padding);
}

bool sameBytes(const Point& lhs, const Point& rhs) {
    return memcmp(&lhs, &rhs, sizeof(Point)) == 0;
}

}

class TuringProtoParameterReaderTest : public ::testing::Test {
protected:
    void write(const ParameterMap& parameters) {
        TuringProtoParameterWriter writer(&_outBuf);
        writer.writeParameters(parameters);
    }

    void read(size_t byteCount, ParameterMap& parameters) {
        TuringProtoInBuf inBuf(byteCount);
        std::copy_n(_outBuf.data(), byteCount, inBuf.data());
        inBuf.increaseWriteOffset(byteCount);

        readParameters(&inBuf, parameters);
        EXPECT_EQ(inBuf.readable(), 0u);
    }

    void roundTrip(const ParameterMap& source, ParameterMap& decoded) {
        write(source);
        read(_outBuf.size(), decoded);
    }

    static void readBytes(const std::vector<uint8_t>& bytes, ParameterMap& parameters) {
        TuringProtoInBuf inBuf(bytes.size());
        memcpy(inBuf.data(), bytes.data(), bytes.size());
        inBuf.increaseWriteOffset(bytes.size());

        readParameters(&inBuf, parameters);
        EXPECT_EQ(inBuf.readable(), 0u);
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

// The bytes the npm client (wasm/npm/TuringClient.mjs) writes for {integer: 470, negative: -1n,
// ratio: 2.5, whole: double(2), text: 'Oxford 🚆', flag: false, nothing: null,
// calls: [480, 'S1', [2.5], null], stop: {crs: 'PAD', platforms: [1, 2]}}
TEST_F(TuringProtoParameterReaderTest, readsTheMapTheNpmClientWrites) {
    const std::vector<uint8_t> bytes = {
        0x09, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x05, 0x00, 0x00, 0x00, 0x63, 0x61, 0x6c, 0x6c,
        0x73, 0x06, 0x04, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xe0, 0x01, 0x00, 0x00, 0x00,
        0x00, 0x00, 0x00, 0x04, 0x02, 0x00, 0x00, 0x00, 0x53, 0x31, 0x06, 0x01, 0x00, 0x00, 0x00, 0x00,
        0x00, 0x00, 0x00, 0x02, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x04, 0x40, 0x07, 0x00, 0x04, 0x00,
        0x00, 0x00, 0x66, 0x6c, 0x61, 0x67, 0x03, 0x00, 0x07, 0x00, 0x00, 0x00, 0x69, 0x6e, 0x74, 0x65,
        0x67, 0x65, 0x72, 0x00, 0xd6, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x08, 0x00, 0x00, 0x00,
        0x6e, 0x65, 0x67, 0x61, 0x74, 0x69, 0x76, 0x65, 0x00, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
        0xff, 0x07, 0x00, 0x00, 0x00, 0x6e, 0x6f, 0x74, 0x68, 0x69, 0x6e, 0x67, 0x07, 0x00, 0x05, 0x00,
        0x00, 0x00, 0x72, 0x61, 0x74, 0x69, 0x6f, 0x02, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x04, 0x40,
        0x04, 0x00, 0x00, 0x00, 0x73, 0x74, 0x6f, 0x70, 0x0b, 0x02, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        0x00, 0x03, 0x00, 0x00, 0x00, 0x63, 0x72, 0x73, 0x04, 0x03, 0x00, 0x00, 0x00, 0x50, 0x41, 0x44,
        0x09, 0x00, 0x00, 0x00, 0x70, 0x6c, 0x61, 0x74, 0x66, 0x6f, 0x72, 0x6d, 0x73, 0x06, 0x02, 0x00,
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        0x02, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x04, 0x00, 0x00, 0x00, 0x74, 0x65, 0x78, 0x74,
        0x04, 0x0b, 0x00, 0x00, 0x00, 0x4f, 0x78, 0x66, 0x6f, 0x72, 0x64, 0x20, 0xf0, 0x9f, 0x9a, 0x86,
        0x05, 0x00, 0x00, 0x00, 0x77, 0x68, 0x6f, 0x6c, 0x65, 0x02, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        0x00, 0x40,
    };

    TuringProtoInBuf inBuf(bytes.size());
    memcpy(inBuf.data(), bytes.data(), bytes.size());
    inBuf.increaseWriteOffset(bytes.size());

    ParameterMap decoded;
    readParameters(&inBuf, decoded);
    EXPECT_EQ(inBuf.readable(), 0u);

    EXPECT_EQ(decoded.get("integer")->getType(), ValueType::Int64);
    EXPECT_EQ(decoded.get("integer")->getInt64(), 470);
    EXPECT_EQ(decoded.get("negative")->getInt64(), -1);
    EXPECT_EQ(decoded.get("ratio")->getDouble(), 2.5);
    EXPECT_EQ(decoded.get("whole")->getType(), ValueType::Double);
    EXPECT_EQ(decoded.get("whole")->getDouble(), 2.0);
    EXPECT_EQ(decoded.get("text")->getString(), "Oxford 🚆");
    EXPECT_FALSE(decoded.get("flag")->getBool());
    EXPECT_TRUE(decoded.get("nothing")->isNull());

    const ParameterValue::List& calls = decoded.get("calls")->getList();
    ASSERT_EQ(calls.size(), 4u);
    EXPECT_EQ(calls[0].getInt64(), 480);
    EXPECT_EQ(calls[1].getString(), "S1");
    ASSERT_EQ(calls[2].getList().size(), 1u);
    EXPECT_EQ(calls[2].getList()[0].getDouble(), 2.5);
    EXPECT_TRUE(calls[3].isNull());

    const ParameterValue::Map& stop = decoded.get("stop")->getMap();
    ASSERT_EQ(stop.size(), 2u);
    EXPECT_EQ(stop.at("crs").getString(), "PAD");

    const ParameterValue::List& platforms = stop.at("platforms").getList();
    ASSERT_EQ(platforms.size(), 2u);
    EXPECT_EQ(platforms[0].getInt64(), 1);
    EXPECT_EQ(platforms[1].getInt64(), 2);
}

TEST_F(TuringProtoParameterReaderTest, aDurationAndADateTimeComeBack) {
    ParameterMap source;

    ParameterValue value;
    value.setDuration(Duration {3'600'000'000});
    source.set("wait", value);
    value.setDateTime(DateTime {1'762'351'500'000'000});
    source.set("when", value);

    ParameterMap decoded;
    roundTrip(source, decoded);

    EXPECT_EQ(decoded.get("wait")->getDuration(), Duration {3'600'000'000});
    EXPECT_EQ(decoded.get("when")->getDateTime(), DateTime {1'762'351'500'000'000});
}

TEST_F(TuringProtoParameterReaderTest, aNestedDurationAndDateTimeComeBack) {
    ParameterMap source;

    ParameterValue waits;
    ParameterValue::List& list = waits.setList();
    list.emplace_back().setDuration(Duration {3'600'000'000});
    list.emplace_back().setDateTime(DateTime {1'762'351'500'000'000});
    source.set("waits", waits);

    ParameterValue entries;
    entries.setMap()["when"].setDateTime(DateTime {1'762'351'500'000'000});
    source.set("entries", entries);

    ParameterMap decoded;
    roundTrip(source, decoded);

    const ParameterValue::List& decodedList = decoded.get("waits")->getList();
    ASSERT_EQ(decodedList.size(), 2u);
    EXPECT_EQ(decodedList[0].getDuration(), Duration {3'600'000'000});
    EXPECT_EQ(decodedList[1].getDateTime(), DateTime {1'762'351'500'000'000});
    EXPECT_EQ(decoded.get("entries")->getMap().at("when").getDateTime(), DateTime {1'762'351'500'000'000});
}

TEST_F(TuringProtoParameterReaderTest, anInstantTheEngineCannotSpellIsRefused) {
    ParameterMap source;
    ParameterValue value;
    value.setDateTime(DateTime {std::numeric_limits<int64_t>::max()});
    source.set("when", value);

    ParameterMap decoded;
    EXPECT_THROW(roundTrip(source, decoded), TuringException);
}

TEST_F(TuringProtoParameterReaderTest, anEmbeddingComesBackAsItsFloats) {
    const std::vector<float> profile {1.0f, -0.5f, 3.25f};
    const std::vector<float> origin {0.0f, 0.0f};

    ParameterMap source;
    source.insert("profile").setEmbedding(profile);
    source.insert("none").setEmbedding({});

    ParameterValue::List& profiles = source.insert("profiles").setList();
    profiles.emplace_back().setEmbedding(origin);
    profiles.emplace_back().setInt64(1);

    source.insert("stop").setMap()["at"].setEmbedding(origin);

    ParameterMap decoded;
    roundTrip(source, decoded);

    const ParameterValue* decodedProfile = decoded.get("profile");
    ASSERT_EQ(decodedProfile->getType(), ValueType::Embedding);
    EXPECT_TRUE(std::ranges::equal(decodedProfile->getEmbedding(), profile));
    EXPECT_TRUE(decoded.get("none")->getEmbedding().empty());

    const ParameterValue::List& decodedProfiles = decoded.get("profiles")->getList();
    ASSERT_EQ(decodedProfiles.size(), 2u);
    EXPECT_TRUE(std::ranges::equal(decodedProfiles[0].getEmbedding(), origin));
    EXPECT_EQ(decodedProfiles[1].getInt64(), 1);

    EXPECT_TRUE(std::ranges::equal(decoded.get("stop")->getMap().at("at").getEmbedding(), origin));
}

// The bytes the npm client (wasm/npm/TuringClient.mjs) writes for {v: new Float32Array([1, 0.5])}
// and the Python clients for {"v": numpy.array([1, 0.5], dtype=numpy.float32)}
TEST_F(TuringProtoParameterReaderTest, readsTheEmbeddingAClientWrites) {
    const std::vector<uint8_t> bytes = {
        0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x76, 0x05, 0x08, 0x00,
        0x00, 0x00, 0x00, 0x00, 0x80, 0x3f, 0x00, 0x00, 0x00, 0x3f,
    };

    TuringProtoInBuf inBuf(bytes.size());
    memcpy(inBuf.data(), bytes.data(), bytes.size());
    inBuf.increaseWriteOffset(bytes.size());

    ParameterMap decoded;
    readParameters(&inBuf, decoded);
    EXPECT_EQ(inBuf.readable(), 0u);

    const std::vector<float> expected {1.0f, 0.5f};
    ASSERT_EQ(decoded.get("v")->getType(), ValueType::Embedding);
    EXPECT_TRUE(std::ranges::equal(decoded.get("v")->getEmbedding(), expected));
}

TEST_F(TuringProtoParameterReaderTest, aPointComesBackInEachCRS) {
    const Point cartesian(PointCRS::Cartesian, 1.0, 2.0, 0.0);
    const Point cartesian3D(PointCRS::Cartesian3D, 1.5, -2.0, 3.0);
    const Point wgs84(PointCRS::WGS84, 2.3522, 48.8566, 0.0);
    const Point wgs843D(PointCRS::WGS84_3D, 2.3522, 48.8566, 35.0);

    ParameterMap source;
    source.insert("c").setPoint(cartesian);
    source.insert("c3").setPoint(cartesian3D);
    source.insert("g").setPoint(wgs84);

    ParameterValue::List& route = source.insert("route").setList();
    route.emplace_back().setPoint(wgs843D);
    route.emplace_back().setNull();

    source.insert("stop").setMap()["at"].setPoint(cartesian3D);

    ParameterMap decoded;
    roundTrip(source, decoded);

    ASSERT_EQ(decoded.get("c")->getType(), ValueType::Point);
    EXPECT_TRUE(sameBytes(decoded.get("c")->getPoint(), cartesian));
    EXPECT_TRUE(sameBytes(decoded.get("c3")->getPoint(), cartesian3D));
    EXPECT_TRUE(sameBytes(decoded.get("g")->getPoint(), wgs84));

    const ParameterValue::List& decodedRoute = decoded.get("route")->getList();
    ASSERT_EQ(decodedRoute.size(), 2u);
    EXPECT_TRUE(sameBytes(decodedRoute[0].getPoint(), wgs843D));
    EXPECT_TRUE(decodedRoute[1].isNull());

    EXPECT_TRUE(sameBytes(decoded.get("stop")->getMap().at("at").getPoint(), cartesian3D));
}

// The bytes the npm client (wasm/npm/TuringClient.mjs) writes for {pt: new Point(4326, 2.5, 48.5)}
TEST_F(TuringProtoParameterReaderTest, readsThePointAClientWrites) {
    const std::vector<uint8_t> bytes = {
        0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02, 0x00, 0x00, 0x00, 0x70, 0x74, 0x0e, 0x00,
        0x00, 0x00, 0x00, 0x00, 0x00, 0x04, 0x40, 0x00, 0x00, 0x00, 0x00, 0x00, 0x40, 0x48, 0x40, 0x00,
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
    };

    ParameterMap decoded;
    readBytes(bytes, decoded);

    ASSERT_EQ(decoded.get("pt")->getType(), ValueType::Point);
    EXPECT_TRUE(sameBytes(decoded.get("pt")->getPoint(), Point(PointCRS::WGS84, 2.5, 48.5, 0.0)));
}

TEST_F(TuringProtoParameterReaderTest, aPointThatBreaksItsInvariantsIsRefused) {
    constexpr double NOT_A_NUMBER = std::numeric_limits<double>::quiet_NaN();
    constexpr double INFINITE = std::numeric_limits<double>::infinity();

    struct RefusedPoint {
        double _x {0.0};
        double _y {0.0};
        double _z {0.0};
        uint8_t _crs {0};
        std::string_view _message;
    };

    const std::vector<RefusedPoint> refused {
        {1.0, 2.0, 0.0, 4, "Unknown coordinate reference system ordinal: 4"},
        {1.0, 2.0, 0.0, 255, "Unknown coordinate reference system ordinal: 255"},
        {NOT_A_NUMBER, 2.0, 0.0, 0, "A point cannot have a non-finite coordinate"},
        {1.0, INFINITE, 0.0, 2, "A point cannot have a non-finite coordinate"},
        {1.0, 2.0, -INFINITE, 1, "A point cannot have a non-finite coordinate"},
        {2.0, 91.0, 0.0, 2, "The latitude of a wgs-84 point lies in [-90, 90], not 91"},
        {2.0, -90.5, 3.0, 3, "The latitude of a wgs-84-3d point lies in [-90, 90], not -90.5"},
        {1.0, 2.0, 5.0, 0, "A cartesian point has 2 coordinates, not 3"},
        {2.0, 48.0, 35.0, 2, "A wgs-84 point has 2 coordinates, not 3"},
    };

    for (const RefusedPoint& point : refused) {
        std::vector<uint8_t> bytes;
        writePointMap(point._x, point._y, point._z, point._crs, 0, bytes);

        ParameterMap decoded;
        try {
            readBytes(bytes, decoded);
            ADD_FAILURE() << "A point parameter was read where it should fail with: " << point._message;
        } catch (const TuringException& error) {
            EXPECT_EQ(std::string_view(error.what()), point._message);
        }
    }
}

TEST_F(TuringProtoParameterReaderTest, aPointIsReadInTheFormItIsStoredIn) {
    struct SentPoint {
        double _x {0.0};
        double _y {0.0};
        double _z {0.0};
        uint8_t _crs {0};
        uint8_t _padding {0};
        Point _stored {};
    };

    const std::vector<SentPoint> sent {
        {-0.0, 2.0, -0.0, 0, 0xff, Point(PointCRS::Cartesian, 0.0, 2.0, 0.0)},
        {1.0, -0.0, -0.0, 1, 0, Point(PointCRS::Cartesian3D, 1.0, 0.0, 0.0)},
        {190.0, 48.0, 0.0, 2, 0, Point(PointCRS::WGS84, -170.0, 48.0, 0.0)},
        {540.0, -0.0, 12.0, 3, 0x01, Point(PointCRS::WGS84_3D, 180.0, 0.0, 12.0)},
    };

    for (const SentPoint& point : sent) {
        std::vector<uint8_t> bytes;
        writePointMap(point._x, point._y, point._z, point._crs, point._padding, bytes);

        ParameterMap decoded;
        readBytes(bytes, decoded);

        EXPECT_TRUE(sameBytes(decoded.get("pt")->getPoint(), point._stored)) << point._x << ", " << point._y << ", " << point._z;
    }
}

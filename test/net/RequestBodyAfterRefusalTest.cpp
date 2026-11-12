#include <gtest/gtest.h>

#include <stddef.h>
#include <stdint.h>
#include <string.h>
#include <span>
#include <string>
#include <string_view>

#include "ParameterMap.h"
#include "ParameterValue.h"
#include "ProtoTestUtils.h"
#include "TuringException.h"
#include "TuringProtoHeaders.h"
#include "TuringProtoInBuf.h"
#include "TuringProtoParameterReader.h"
#include "TuringProtoRequestBody.h"

using namespace db;
using namespace net::proto;
using namespace turing::test;

namespace {

void makeNestedParameters(ParameterMap& parameters) {
    ParameterValue ids;
    ParameterValue::List& idList = ids.setList();
    idList.resize(3);
    idList[0].setInt64(1);
    idList[1].setInt64(2);
    idList[2].setInt64(3);

    ParameterValue properties;
    ParameterValue::Map& propertyMap = properties.setMap();
    propertyMap["name"].setString("Remy");
    ParameterValue::List& tags = propertyMap["tags"].setList();
    tags.resize(1);
    tags[0].setString("founder");

    ParameterValue x;
    x.setInt64(5);

    parameters.set("ids", ids);
    parameters.set("properties", properties);
    parameters.set("x", x);
}

void refuse(TuringProtoRequestBody& body, const ParameterMap& parameters) {
    try {
        body.build("UNWIND $p AS p RETURN count(*) AS c", &parameters);
        ADD_FAILURE() << "Parameters larger than the buffer were accepted";
    } catch (const TuringException& e) {
        std::string message;
        makeTooLargeParametersMessage(message);
        EXPECT_EQ(std::string_view(e.what()), message);
    }
}

void suffixOf(TuringProtoRequestBody& body, std::string& bytes) {
    const std::span<char> suffix = body.getSuffix();
    bytes.assign(suffix.data(), suffix.size());
}

void decodeSuffix(TuringProtoRequestBody& body, ParameterMap& decoded) {
    const std::span<char> suffix = body.getSuffix();
    ASSERT_GE(suffix.size(), sizeof(WireSize));

    WireSize parameterLength = 0;
    memcpy(&parameterLength, suffix.data(), sizeof(parameterLength));
    ASSERT_EQ(parameterLength, suffix.size() - sizeof(WireSize));

    TuringProtoInBuf inBuf(parameterLength);
    memcpy(inBuf.data(), suffix.data() + sizeof(WireSize), parameterLength);
    inBuf.increaseWriteOffset(parameterLength);

    readParameters(&inBuf, decoded);
    EXPECT_EQ(inBuf.readable(), 0u);
}

}

// A request body that refused its parameters is left as it was: the next build writes the
// bytes a fresh request body writes
TEST(RequestBodyAfterRefusalTest, aScalarParameterAfterARefusal) {
    ParameterMap tooLarge;
    makeTooLargeParameters(tooLarge);

    ParameterMap parameters;
    ParameterValue x;
    x.setInt64(5);
    parameters.set("x", x);

    TuringProtoRequestBody freshBody;
    freshBody.build("RETURN $x AS x", &parameters);
    std::string expected;
    suffixOf(freshBody, expected);

    TuringProtoRequestBody body;
    refuse(body, tooLarge);

    ASSERT_NO_THROW(body.build("RETURN $x AS x", &parameters));
    std::string written;
    suffixOf(body, written);
    EXPECT_EQ(written, expected);

    ParameterMap decoded;
    decodeSuffix(body, decoded);
    ASSERT_EQ(decoded.getValues().size(), 1u);
    const ParameterValue* decodedX = decoded.get("x");
    ASSERT_NE(decodedX, nullptr);
    EXPECT_EQ(decodedX->getInt64(), 5);
}

TEST(RequestBodyAfterRefusalTest, nestedParametersAfterARefusal) {
    ParameterMap tooLarge;
    makeTooLargeParameters(tooLarge);

    ParameterMap parameters;
    makeNestedParameters(parameters);

    TuringProtoRequestBody freshBody;
    freshBody.build("UNWIND $ids AS x RETURN x", &parameters);
    std::string expected;
    suffixOf(freshBody, expected);

    TuringProtoRequestBody body;
    refuse(body, tooLarge);

    ASSERT_NO_THROW(body.build("UNWIND $ids AS x RETURN x", &parameters));
    std::string written;
    suffixOf(body, written);
    EXPECT_EQ(written, expected);

    ParameterMap decoded;
    decodeSuffix(body, decoded);
    ASSERT_EQ(decoded.getValues().size(), 3u);

    const ParameterValue* idsValue = decoded.get("ids");
    ASSERT_NE(idsValue, nullptr);
    const ParameterValue::List& ids = idsValue->getList();
    ASSERT_EQ(ids.size(), 3u);
    EXPECT_EQ(ids[0].getInt64(), 1);
    EXPECT_EQ(ids[1].getInt64(), 2);
    EXPECT_EQ(ids[2].getInt64(), 3);

    const ParameterValue* propertiesValue = decoded.get("properties");
    ASSERT_NE(propertiesValue, nullptr);
    const ParameterValue::Map& properties = propertiesValue->getMap();
    ASSERT_EQ(properties.size(), 2u);
    EXPECT_EQ(properties.at("name").getString(), "Remy");
    const ParameterValue::List& tags = properties.at("tags").getList();
    ASSERT_EQ(tags.size(), 1u);
    EXPECT_EQ(tags[0].getString(), "founder");

    const ParameterValue* decodedX = decoded.get("x");
    ASSERT_NE(decodedX, nullptr);
    EXPECT_EQ(decodedX->getInt64(), 5);
}

TEST(RequestBodyAfterRefusalTest, twoRefusalsThenParameters) {
    ParameterMap tooLarge;
    makeTooLargeParameters(tooLarge);

    ParameterMap parameters;
    makeNestedParameters(parameters);

    TuringProtoRequestBody freshBody;
    freshBody.build("UNWIND $ids AS x RETURN x", &parameters);
    std::string expected;
    suffixOf(freshBody, expected);

    TuringProtoRequestBody body;
    refuse(body, tooLarge);
    refuse(body, tooLarge);

    ASSERT_NO_THROW(body.build("UNWIND $ids AS x RETURN x", &parameters));
    std::string written;
    suffixOf(body, written);
    EXPECT_EQ(written, expected);
}

TEST(RequestBodyAfterRefusalTest, everyBuildAfterARefusalIsTheSame) {
    ParameterMap tooLarge;
    makeTooLargeParameters(tooLarge);

    ParameterMap parameters;
    makeNestedParameters(parameters);

    TuringProtoRequestBody freshBody;
    freshBody.build("UNWIND $ids AS x RETURN x", &parameters);
    std::string expected;
    suffixOf(freshBody, expected);

    TuringProtoRequestBody body;
    refuse(body, tooLarge);

    for (size_t attempt = 0; attempt < 3; attempt++) {
        SCOPED_TRACE(::testing::Message() << "attempt=" << attempt);

        ASSERT_NO_THROW(body.build("UNWIND $ids AS x RETURN x", &parameters));
        std::string written;
        suffixOf(body, written);
        EXPECT_EQ(written, expected);
    }
}

TEST(RequestBodyAfterRefusalTest, noParametersAfterARefusal) {
    ParameterMap tooLarge;
    makeTooLargeParameters(tooLarge);

    TuringProtoRequestBody body;
    refuse(body, tooLarge);

    ASSERT_NO_THROW(body.build("MATCH (n:Person) RETURN n.name LIMIT 2", nullptr));
    const std::span<char> suffix = body.getSuffix();
    ASSERT_EQ(suffix.size(), sizeof(WireSize));

    WireSize parameterLength = 1;
    memcpy(&parameterLength, suffix.data(), sizeof(parameterLength));
    EXPECT_EQ(parameterLength, 0u);
}

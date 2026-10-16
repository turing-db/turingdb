#include <gtest/gtest.h>

#include <stddef.h>
#include <string.h>
#include <sys/mman.h>
#include <span>
#include <string>
#include <string_view>

#include "ParameterMap.h"
#include "ParameterValue.h"
#include "TuringException.h"
#include "TuringProtoHeaders.h"
#include "TuringProtoInBuf.h"
#include "TuringProtoParameterReader.h"
#include "TuringProtoRequestBody.h"

using namespace db;
using namespace net::proto;

namespace {

WireSize readLength(std::span<const char> bytes) {
    WireSize length = 0;
    memcpy(&length, bytes.data(), sizeof(length));
    return length;
}

}

TEST(TuringProtoRequestBodyTest, aQueryWithoutParametersEndsWithAZeroLength) {
    TuringProtoRequestBody body;
    body.build("RETURN 1", nullptr);

    ASSERT_EQ(body.getPrefix().size(), sizeof(WireSize));
    EXPECT_EQ(readLength(body.getPrefix()), 8u);

    ASSERT_EQ(body.getSuffix().size(), sizeof(WireSize));
    EXPECT_EQ(readLength(body.getSuffix()), 0u);

    const ParameterMap empty;
    body.build("RETURN 1", &empty);
    ASSERT_EQ(body.getSuffix().size(), sizeof(WireSize));
    EXPECT_EQ(readLength(body.getSuffix()), 0u);
}

TEST(TuringProtoRequestBodyTest, theParameterLengthCoversTheParameterMap) {
    ParameterValue value;
    value.setString("Founder");

    ParameterMap parameters;
    parameters.set("label", value);

    TuringProtoRequestBody body;
    body.build("MATCH (n:$label) RETURN n", &parameters);

    const std::span<char> suffix = body.getSuffix();
    const WireSize parameterLength = readLength(suffix);
    ASSERT_EQ(parameterLength, suffix.size() - sizeof(WireSize));

    TuringProtoInBuf inBuf(parameterLength);
    memcpy(inBuf.data(), suffix.data() + sizeof(WireSize), parameterLength);
    inBuf.increaseWriteOffset(parameterLength);

    ParameterMap decoded;
    readParameters(&inBuf, decoded);
    ASSERT_NE(decoded.get("label"), nullptr);
    EXPECT_EQ(decoded.get("label")->getString(), "Founder");
}

TEST(TuringProtoRequestBodyTest, aQueryLongerThanTheWireAllowsIsRejected) {
    const size_t querySize = MAX_WIRE_SIZE + 1;

    // build reads only the size of the query, so the pages are reserved and never touched
    void* pages = mmap(nullptr, querySize, PROT_READ, MAP_PRIVATE | MAP_ANONYMOUS | MAP_NORESERVE, -1, 0);
    ASSERT_NE(pages, MAP_FAILED);
    const std::string_view query(static_cast<const char*>(pages), querySize);

    TuringProtoRequestBody body;
    try {
        body.build(query, nullptr);
        ADD_FAILURE() << "A query longer than the wire allows was accepted";
    } catch (const TuringException& e) {
        EXPECT_STREQ(e.what(), "The query is 4294967296 bytes, more than the 4294967295 bytes the binary protocol allows");
    }

    munmap(pages, querySize);
}

TEST(TuringProtoRequestBodyTest, parametersLargerThanTheBufferAreRejected) {
    ParameterValue value;
    value.setString(std::string(DEFAULT_BUFFER_CAPACITY, 'x'));

    ParameterMap parameters;
    parameters.set("big", value);

    TuringProtoRequestBody body;
    try {
        body.build("RETURN 1", &parameters);
        ADD_FAILURE() << "Parameters larger than the buffer were accepted";
    } catch (const TuringException& e) {
        EXPECT_STREQ(e.what(), "The query parameters exceed 1048576 bytes");
    }
}

#include <gtest/gtest.h>

#include <cstdint>
#include <cstring>
#include <string>
#include <string_view>

#include "NetBuffer.h"
#include "TuringProtoHeaders.h"
#include "TuringProtoOutBuf.h"
#include "TuringProtoParser.h"

namespace {

void writeBuffer(net::NetBuffer* buffer, std::string_view bytes) {
    auto writer = buffer->getWriter();
    writer.writeString(bytes.data(), bytes.size());
}

// Build the exact on-the-wire packet once so individual tests can truncate it
// to exercise partial-header and partial-payload parser states.
std::string framePacket(net::proto::MessageTypes type, std::string_view payload) {
    net::proto::TuringProtoOutBuf outBuf(
        net::proto::ProtoHeader::wireSize() + payload.size());
    outBuf.setOnBufferFullCallBack([]() {});
    net::proto::frameMessage(type, payload, &outBuf);
    return std::string(outBuf.data(), outBuf.size());
}

std::string encodeHeaderOnly(net::proto::MessageTypes type, uint32_t dataLen) {
    const net::proto::ProtoHeader header {
        ._type = type,
        ._dataLen = dataLen};

    net::proto::TuringProtoOutBuf outBuf(net::proto::ProtoHeader::wireSize());
    outBuf.copyHeader(&header);
    return std::string(outBuf.data(), outBuf.size());
}

} // namespace

// When fewer than wireSize() bytes are buffered, analyze() must report
// "need more data" (a successful result holding false) rather than erroring.
// The parser must keep its state intact so the next read can complete the
// header in place.
TEST(TuringProtoParserTest, AnalyzeReturnsFalseForPartialHeader) {
    const auto packet = framePacket(net::proto::MessageTypes::QUERY, "abc");

    net::NetBuffer buffer;
    writeBuffer(&buffer, std::string_view(packet).substr(0, net::proto::ProtoHeader::wireSize() - 1));

    net::proto::TuringProtoParser parser(&buffer);
    const auto result = parser.analyze();

    ASSERT_TRUE(result.has_value());
    EXPECT_FALSE(result.value());
}

// Same need-more-data behavior once the header has arrived but the payload
// is short by even a single byte. The parser must remember the parsed header
// (so it doesn't redecode it) and still report incomplete.
TEST(TuringProtoParserTest, AnalyzeReturnsFalseForPartialPayload) {
    const auto packet = framePacket(net::proto::MessageTypes::QUERY, "abc");

    net::NetBuffer buffer;
    writeBuffer(&buffer, std::string_view(packet).substr(0, packet.size() - 1));

    net::proto::TuringProtoParser parser(&buffer);
    const auto result = parser.analyze();

    ASSERT_TRUE(result.has_value());
    EXPECT_FALSE(result.value());
}

// A complete packet yields a successful result with value true. After that,
// the parser must expose both the decoded ProtoHeader and a payload string
// view that points at the bytes inside the input buffer (no copy).
TEST(TuringProtoParserTest, AnalyzeSucceedsForCompleteMessage) {
    constexpr std::string_view payload = "match (n) return n";
    const auto packet = framePacket(net::proto::MessageTypes::QUERY, payload);

    net::NetBuffer buffer;
    writeBuffer(&buffer, packet);

    net::proto::TuringProtoParser parser(&buffer);
    const auto result = parser.analyze();

    ASSERT_TRUE(result.has_value());
    ASSERT_TRUE(result.value());
    EXPECT_EQ(parser.getHeader()._type, net::proto::MessageTypes::QUERY);
    EXPECT_EQ(parser.getHeader()._dataLen, payload.size());
    EXPECT_EQ(parser.getPayload(), payload);
}

// A packet whose type byte is at or beyond MessageTypes::_SIZE must fail
// analyze() with INVALID_MESSAGE_TYPE so the server never dispatches on a
// type it does not understand. The error path is what writes a
// PROTOCOL_ERROR back to the client and tears the connection down.
TEST(TuringProtoParserTest, RejectsInvalidMessageType) {
    const auto packet = encodeHeaderOnly(
        static_cast<net::proto::MessageTypes>(static_cast<uint8_t>(net::proto::MessageTypes::_SIZE)), 0);

    net::NetBuffer buffer;
    writeBuffer(&buffer, packet);

    net::proto::TuringProtoParser parser(&buffer);
    const auto result = parser.analyze();

    EXPECT_FALSE(result.has_value());
    EXPECT_EQ(result.error(),
              static_cast<net::AbstractTCPParser::AnalyzeError>(
                  net::proto::TuringProtoParser::Error::INVALID_MESSAGE_TYPE));
}

// If the header advertises a payload that cannot physically fit in the
// connection's NetBuffer, the parser must fail fast with REQUEST_TOO_BIG.
// Without this check the server would block forever waiting for bytes that
// can never arrive.
TEST(TuringProtoParserTest, RejectsPacketsLargerThanNetBuffer) {
    const auto packet = encodeHeaderOnly(net::proto::MessageTypes::QUERY, net::NetBuffer::BUFFER_SIZE);

    net::NetBuffer buffer;
    writeBuffer(&buffer, packet);

    net::proto::TuringProtoParser parser(&buffer);
    const auto result = parser.analyze();

    EXPECT_FALSE(result.has_value());
    EXPECT_EQ(result.error(),
              static_cast<net::AbstractTCPParser::AnalyzeError>(
                  net::proto::TuringProtoParser::Error::REQUEST_TOO_BIG));
}

// Boundary case for the size check above: a payload of exactly
// BUFFER_SIZE - wireSize() bytes fills the NetBuffer to capacity and must be
// accepted. This pins the comparison as strict (>) rather than (>=).
TEST(TuringProtoParserTest, AcceptsLargestPayloadThatFitsInNetBuffer) {
    const size_t payloadSize = net::NetBuffer::BUFFER_SIZE - net::proto::ProtoHeader::wireSize();
    const std::string payload(payloadSize, 'p');
    const auto packet = framePacket(net::proto::MessageTypes::QUERY, payload);

    net::NetBuffer buffer;
    writeBuffer(&buffer, packet);

    net::proto::TuringProtoParser parser(&buffer);
    const auto result = parser.analyze();

    ASSERT_TRUE(result.has_value());
    ASSERT_TRUE(result.value());
    EXPECT_EQ(parser.getHeader()._dataLen, payloadSize);
    EXPECT_EQ(parser.getPayload().size(), payloadSize);
}

// reset() drops the parsed-header flag so the parser will redecode whatever
// is still pending in the input buffer. Because this test does not drain
// the NetBuffer between analyze() calls, re-running analyze() after reset()
// must reproduce the same packet rather than reusing the cached state from
// the first pass.
TEST(TuringProtoParserTest, ResetClearsPreviousState) {
    const auto firstPacket = framePacket(net::proto::MessageTypes::QUERY, "first");
    net::NetBuffer buffer;
    writeBuffer(&buffer, firstPacket);

    net::proto::TuringProtoParser parser(&buffer);
    auto result = parser.analyze();
    ASSERT_TRUE(result.has_value());
    ASSERT_TRUE(result.value());
    EXPECT_EQ(parser.getPayload(), "first");

    parser.reset();
    result = parser.analyze();
    ASSERT_TRUE(result.has_value());
    ASSERT_TRUE(result.value());
    EXPECT_EQ(parser.getHeader()._type, net::proto::MessageTypes::QUERY);
    EXPECT_EQ(parser.getPayload(), "first");
}

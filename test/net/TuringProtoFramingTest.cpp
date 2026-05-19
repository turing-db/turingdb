#include <gtest/gtest.h>

#include <array>
#include <cstring>
#include <string_view>

#include "FatalException.h"
#include "TuringProtoHeaders.h"
#include "TuringProtoOutBuf.h"

namespace {

// Slice using the decoded header length so these assertions validate the
// framing helpers, not assumptions about the backing buffer layout.
std::string_view getPacketPayload(std::string_view packet, const net::proto::ProtoHeader& header) {
    return packet.substr(net::proto::ProtoHeader::wireSize(), header._dataLen);
}

} // namespace

// frameMessage(type, payload, outBuf) lays a header followed by the payload
// into a single buffer. After framing, decoding the same buffer must recover
// the message type and length, and the bytes after the header must equal the
// original payload. This is the round-trip invariant for the simple framing
// path used for fixed-size messages.
TEST(TuringProtoFramingTest, FramesAndDecodesStringPayload) {
    constexpr std::string_view payload = "payload-body";

    net::proto::TuringProtoOutBuf outBuf(
        net::proto::ProtoHeader::wireSize() + payload.size());
    outBuf.setOnBufferFullCallBack([]() {});
    net::proto::frameMessage(net::proto::MessageTypes::CHUNK, payload, &outBuf);

    const std::string_view packet(outBuf.data(), outBuf.size());
    const auto header = net::proto::ProtoHeader::decode(packet.data(), packet.size());

    EXPECT_EQ(header._type, net::proto::MessageTypes::CHUNK);
    EXPECT_EQ(header._dataLen, payload.size());
    EXPECT_EQ(getPacketPayload(packet, header), payload);
}

// The chunk path keeps the header in its own buffer so the data buffer can
// flow through scatter/gather I/O. This verifies that copyHeader writes the
// header bytes into a buffer and decode reads them back to an equivalent
// ProtoHeader, without going through the unified frameMessage helper.
TEST(TuringProtoFramingTest, EncodesHeaderIntoSeparateBuffer) {
    const net::proto::ProtoHeader header {
        ._type = net::proto::MessageTypes::CHUNK,
        ._dataLen = 17};

    net::proto::TuringProtoOutBuf outBuf(net::proto::ProtoHeader::wireSize());
    outBuf.copyHeader(&header);

    const auto decoded = net::proto::ProtoHeader::decode(outBuf.data(), outBuf.size());
    EXPECT_EQ(decoded._type, header._type);
    EXPECT_EQ(decoded._dataLen, header._dataLen);
}

// The two-buffer overload of frameMessage is what the chunk-emitting path
// uses: header in headerBuf, payload in dataBuf, both wired into an iovec
// array for writev. The header it produces must advertise the data buffer's
// size so the receiver knows how many bytes follow it on the wire.
TEST(TuringProtoFramingTest, FrameMessageWithSplitBuffersProducesExpectedHeader) {
    std::array<char, net::proto::ProtoHeader::wireSize()> headerBuf{};
    net::proto::TuringProtoOutBuf dataBuf(64);

    constexpr std::string_view payload = "chunk-data";
    dataBuf.copyVarLenData(payload.data(), payload.size());

    std::array<iovec, 2> iovecs {};
    net::proto::frameMessage(net::proto::MessageTypes::CHUNK, std::span(headerBuf), &dataBuf, iovecs);

    ASSERT_EQ(iovecs[0].iov_len, net::proto::ProtoHeader::wireSize());
    ASSERT_EQ(iovecs[1].iov_len, dataBuf.size());

    const auto decoded =
        net::proto::ProtoHeader::decode(static_cast<const char*>(iovecs[0].iov_base), iovecs[0].iov_len);
    EXPECT_EQ(decoded._type, net::proto::MessageTypes::CHUNK);
    EXPECT_EQ(decoded._dataLen, dataBuf.size());
}

// Decode must refuse to read a header out of fewer bytes than wireSize();
// otherwise it would read past the end of the input. This guards against
// silent corruption when a partial buffer accidentally reaches the decoder.
TEST(TuringProtoFramingTest, RejectsShortHeaderInput) {
    const std::array<char, net::proto::ProtoHeader::wireSize() - 1> bytes {};
    EXPECT_THROW(
        [&]() {
            static_cast<void>(net::proto::ProtoHeader::decode(bytes.data(), bytes.size()));
        }(),
        FatalException);
}

// Symmetric to the decode check: copyHeader must refuse to write into a
// buffer that cannot fit a full header, so we never emit a truncated frame
// onto the wire.
TEST(TuringProtoFramingTest, RejectsEncodingHeaderIntoTooSmallBuffer) {
    const net::proto::ProtoHeader header {
        ._type = net::proto::MessageTypes::END,
        ._dataLen = 8};

    net::proto::TuringProtoOutBuf outBuf(net::proto::ProtoHeader::wireSize() - 1);
    EXPECT_THROW(outBuf.copyHeader(&header), FatalException);
}

// The parser uses isValidMessageType to vet the type byte read off the
// wire. Spot-check that the boundary enumerators (the first valid value, a
// representative middle value, and the _SIZE sentinel) are classified
// correctly so out-of-range bytes can never reach a handler dispatch.
TEST(TuringProtoFramingTest, ValidatesKnownMessageTypes) {
    EXPECT_TRUE(net::proto::ProtoHeader::isValidMessageType(net::proto::MessageTypes::CHUNK_HEADER));
    EXPECT_TRUE(net::proto::ProtoHeader::isValidMessageType(net::proto::MessageTypes::ERROR));
    EXPECT_FALSE(net::proto::ProtoHeader::isValidMessageType(
        static_cast<net::proto::MessageTypes>(static_cast<uint8_t>(net::proto::MessageTypes::_SIZE))));
}

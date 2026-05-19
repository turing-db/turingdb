#include "HTTPUtils.h"

#include <string.h>
#include <string>
#include <string_view>

#include "BioAssert.h"

using namespace net::http;

namespace {

// 256-entry table of two-char lowercase hex byte renderings (00..ff). Lets
// writeHexU32 be a branch-free 4-byte to 8-char conversion: each byte indexes
// directly into a 2-char slot.
constexpr char HEX_BYTE_PAIRS[513] =
    "000102030405060708090a0b0c0d0e0f"
    "101112131415161718191a1b1c1d1e1f"
    "202122232425262728292a2b2c2d2e2f"
    "303132333435363738393a3b3c3d3e3f"
    "404142434445464748494a4b4c4d4e4f"
    "505152535455565758595a5b5c5d5e5f"
    "606162636465666768696a6b6c6d6e6f"
    "707172737475767778797a7b7c7d7e7f"
    "808182838485868788898a8b8c8d8e8f"
    "909192939495969798999a9b9c9d9e9f"
    "a0a1a2a3a4a5a6a7a8a9aaabacadaeaf"
    "b0b1b2b3b4b5b6b7b8b9babbbcbdbebf"
    "c0c1c2c3c4c5c6c7c8c9cacbcccdcecf"
    "d0d1d2d3d4d5d6d7d8d9dadbdcdddedf"
    "e0e1e2e3e4e5e6e7e8e9eaebecedeeef"
    "f0f1f2f3f4f5f6f7f8f9fafbfcfdfeff";

// On-wire header bytes. These match the byte sequences HTTPWriter already
// emits, so HTTPWriter can be refactored onto these helpers later without
// changing what goes out on the socket.
constexpr std::string_view STATUS_LINE_VERSION = "HTTP/1.1 ";
constexpr std::string_view CRLF = "\r\n";
constexpr std::string_view HEADER_KEY_VALUE_SEP = ": ";
constexpr std::string_view CONTENT_LENGTH_KEY = "Content-Length: ";
constexpr std::string_view CHUNKED_TRANSFER_ENCODING_LINE = "Transfer-Encoding: chunked\r\n";
constexpr std::string_view CONTENT_TYPE_TEXT_LINE = "Content-type: text/plain\r\n";
constexpr std::string_view CONTENT_TYPE_JSON_LINE = "Content-type: application/json\r\n";
constexpr std::string_view CONNECTION_KEEP_ALIVE_LINE = "Connection: Keep-Alive\r\n";
constexpr std::string_view CONNECTION_CLOSE_LINE = "Connection: close\r\n";

}

void net::http::writeHexU32(uint32_t num, char* dst, size_t dstSize) {
    bioassert(dstSize >= CHUNK_HEX_DIGITS, "writeHexU32: dst too small");
    for (size_t i = 4; i-- > 0; ) {
        const size_t pos = (num & 0xFF) * 2;
        dst[i * 2] = HEX_BYTE_PAIRS[pos];
        dst[i * 2 + 1] = HEX_BYTE_PAIRS[pos + 1];
        num >>= 8;
    }
}

void net::http::writeChunkHeaderLine(uint32_t chunkSize, char* dst, size_t dstSize) {
    bioassert(dstSize >= CHUNK_HEADER_LINE_SIZE, "writeChunkHeaderLine: dst too small");
    writeHexU32(chunkSize, dst, dstSize);
    dst[CHUNK_HEX_DIGITS] = '\r';
    dst[CHUNK_HEX_DIGITS + 1] = '\n';
}

void net::http::writeChunkTrailer(char* dst, size_t dstSize) {
    bioassert(dstSize >= CHUNK_TRAILER_SIZE, "writeChunkTrailer: dst too small");
    dst[0] = '\r';
    dst[1] = '\n';
}

size_t net::http::writeStatusLine(net::HTTP::Status status, char* dst, size_t dstSize) {
    const std::string_view statusStr = net::HTTP::StatusDescription::value(status);
    const size_t needed = STATUS_LINE_VERSION.size() + statusStr.size() + CRLF.size();
    bioassert(dstSize >= needed, "writeStatusLine: dst too small");
    size_t offset = 0;
    memcpy(dst + offset, STATUS_LINE_VERSION.data(), STATUS_LINE_VERSION.size());
    offset += STATUS_LINE_VERSION.size();
    memcpy(dst + offset, statusStr.data(), statusStr.size());
    offset += statusStr.size();
    memcpy(dst + offset, CRLF.data(), CRLF.size());
    offset += CRLF.size();
    return offset;
}

size_t net::http::writeRawHeader(std::string_view headerLine, char* dst, size_t dstSize) {
    const size_t needed = headerLine.size() + CRLF.size();
    bioassert(dstSize >= needed, "writeRawHeader: dst too small");
    memcpy(dst, headerLine.data(), headerLine.size());
    memcpy(dst + headerLine.size(), CRLF.data(), CRLF.size());
    return needed;
}

size_t net::http::writeHeaderLine(std::string_view key,
                                  std::string_view value,
                                  char* dst,
                                  size_t dstSize) {
    const size_t needed = key.size() + HEADER_KEY_VALUE_SEP.size() + value.size() + CRLF.size();
    bioassert(dstSize >= needed, "writeHeaderLine: dst too small");
    size_t offset = 0;
    memcpy(dst + offset, key.data(), key.size());
    offset += key.size();
    memcpy(dst + offset, HEADER_KEY_VALUE_SEP.data(), HEADER_KEY_VALUE_SEP.size());
    offset += HEADER_KEY_VALUE_SEP.size();
    memcpy(dst + offset, value.data(), value.size());
    offset += value.size();
    memcpy(dst + offset, CRLF.data(), CRLF.size());
    offset += CRLF.size();
    return offset;
}

size_t net::http::writeContentLength(size_t length, char* dst, size_t dstSize) {
    const std::string lengthString = std::to_string(length);
    const size_t needed = CONTENT_LENGTH_KEY.size() + lengthString.size() + CRLF.size();
    bioassert(dstSize >= needed, "writeContentLength: dst too small");
    size_t offset = 0;
    memcpy(dst + offset, CONTENT_LENGTH_KEY.data(), CONTENT_LENGTH_KEY.size());
    offset += CONTENT_LENGTH_KEY.size();
    memcpy(dst + offset, lengthString.data(), lengthString.size());
    offset += lengthString.size();
    memcpy(dst + offset, CRLF.data(), CRLF.size());
    offset += CRLF.size();
    return offset;
}

size_t net::http::writeChunkedTransferEncoding(char* dst, size_t dstSize) {
    bioassert(dstSize >= CHUNKED_TRANSFER_ENCODING_LINE.size(),
              "writeChunkedTransferEncoding: dst too small");
    memcpy(dst, CHUNKED_TRANSFER_ENCODING_LINE.data(), CHUNKED_TRANSFER_ENCODING_LINE.size());
    return CHUNKED_TRANSFER_ENCODING_LINE.size();
}

size_t net::http::writeContentType(net::ContentType contentType, char* dst, size_t dstSize) {
    std::string_view line;
    switch (contentType) {
        case net::ContentType::TEXT:
            line = CONTENT_TYPE_TEXT_LINE;
        break;
        case net::ContentType::JSON:
            line = CONTENT_TYPE_JSON_LINE;
        break;
    }
    bioassert(dstSize >= line.size(), "writeContentType: dst too small");
    memcpy(dst, line.data(), line.size());
    return line.size();
}

size_t net::http::writeConnection(net::ConnectionHeader connection, char* dst, size_t dstSize) {
    std::string_view line;
    switch (connection) {
        case net::ConnectionHeader::KEEP_ALIVE:
            line = CONNECTION_KEEP_ALIVE_LINE;
        break;
        case net::ConnectionHeader::CLOSE:
            line = CONNECTION_CLOSE_LINE;
        break;
    }
    bioassert(dstSize >= line.size(), "writeConnection: dst too small");
    memcpy(dst, line.data(), line.size());
    return line.size();
}

size_t net::http::writeHeadersEnd(char* dst, size_t dstSize) {
    bioassert(dstSize >= CRLF.size(), "writeHeadersEnd: dst too small");
    memcpy(dst, CRLF.data(), CRLF.size());
    return CRLF.size();
}

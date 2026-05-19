#pragma once

#include <stddef.h>
#include <stdint.h>
#include <string_view>

#include "ConnectionHeader.h"
#include "ContentType.h"
#include "HTTP.h"

namespace net::http {

// Reusable HTTP/1.1 write-path helpers. Each `write*` function memcpy's its
// bytes into a caller-provided char* dst and returns the number of bytes
// written. The caller passes the remaining capacity at dst as dstSize; the
// helper bioasserts that the write fits before touching memory, so a buffer
// undersize is a deterministic crash rather than a stack/heap smash.
//
// Each `writeXxx` for a header line writes the full line including the
// terminating CRLF, except writeHeadersEnd which writes the standalone blank
// line that closes the header section. A typical assembly looks like:
//
//     std::array<char, RESPONSE_HEADER_BUFFER_SIZE> buf;
//     size_t off = 0;
//     off += writeStatusLine(HTTP::Status::OK, buf.data() + off, buf.size() - off);
//     off += writeContentType(ContentType::JSON, buf.data() + off, buf.size() - off);
//     off += writeChunkedTransferEncoding(buf.data() + off, buf.size() - off);
//     off += writeConnection(ConnectionHeader::KEEP_ALIVE, buf.data() + off, buf.size() - off);
//     off += writeHeadersEnd(buf.data() + off, buf.size() - off);
//     send(socket, buf.data(), off);

// ---- Chunked transfer encoding ---------------------------------------------

inline constexpr size_t CHUNK_HEX_DIGITS = 8;
inline constexpr size_t CHUNK_HEADER_LINE_SIZE = CHUNK_HEX_DIGITS + 2; // hex + \r\n
inline constexpr size_t CHUNK_TRAILER_SIZE = 2;                        // \r\n
inline constexpr char CHUNK_TERMINATOR[] = "00000000\r\n\r\n";
inline constexpr size_t CHUNK_TERMINATOR_SIZE = 12;

// Default stack buffer size for assembling a response's status line + headers.
// Sized to fit the status line plus a handful of typical header lines plus
// the terminating CRLF. Callers needing more headers should pick a larger
// array and pass its size through to each write*().
inline constexpr size_t RESPONSE_HEADER_BUFFER_SIZE = 256;

// Writes 8 lowercase, zero-padded hex digits for the 4 bytes of `num` into
// dst[0..7]. dstSize must be at least CHUNK_HEX_DIGITS.
void writeHexU32(uint32_t num, char* dst, size_t dstSize);

// Writes "XXXXXXXX\r\n" (CHUNK_HEADER_LINE_SIZE bytes) into dst.
void writeChunkHeaderLine(uint32_t chunkSize, char* dst, size_t dstSize);

void writeChunkTrailer(char* dst, size_t dstSize);

// ---- HTTP header lines -----------------------------------------------------

[[nodiscard]] size_t writeStatusLine(net::HTTP::Status status, char* dst, size_t dstSize);

// Writes "<headerLine>\r\n" — the caller supplies the full key+value text
// without the terminating CRLF.
[[nodiscard]] size_t writeRawHeader(std::string_view headerLine, char* dst, size_t dstSize);

// Writes "<key>: <value>\r\n".
[[nodiscard]] size_t writeHeaderLine(std::string_view key,
                                     std::string_view value,
                                     char* dst,
                                     size_t dstSize);

[[nodiscard]] size_t writeContentLength(size_t length, char* dst, size_t dstSize);

[[nodiscard]] size_t writeChunkedTransferEncoding(char* dst, size_t dstSize);

[[nodiscard]] size_t writeContentType(net::ContentType contentType, char* dst, size_t dstSize);

[[nodiscard]] size_t writeConnection(net::ConnectionHeader connection, char* dst, size_t dstSize);

// Writes the standalone CRLF that terminates the response header section.
[[nodiscard]] size_t writeHeadersEnd(char* dst, size_t dstSize);

}

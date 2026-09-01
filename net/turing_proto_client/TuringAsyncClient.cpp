
#include <errno.h>
#include <fcntl.h>
#include <string.h>
#include <netdb.h>
#include <sys/socket.h>
#include <sys/uio.h>
#include <unistd.h>
#include <algorithm>
#include <array>
#include <cctype>
#include <charconv>
#include <limits>
#include <string>
#include <string_view>
#include <vector>

#include "HTTPUtils.h"
#include "TuringProtoDecoder.h"
#include "TuringProtoHeaders.h"
#include "TuringException.h"
#include "TuringSink.h"
#include "TuringSinkColumnContainer.h"
#include "dataframe/Dataframe.h"
#include "dataframe/DataframeManager.h"

#include "TuringAsyncClient.h"

#include "BioAssert.h"

using namespace net::proto;

namespace {

// Length of the "\r\n\r\n" sentinel that terminates an HTTP/1.1 header block.
constexpr size_t HEADERS_END_SIZE = 4;

// Size of the CRLF that follows every HTTP/1.1 chunk body.
constexpr size_t CRLF_SIZE = 2;

// Parses the integer status code from an HTTP/1.1 status line —
// "HTTP/1.1 200 OK" -> 200. Throws TuringException on malformed input.
int parseStatusCode(std::string_view statusLine) {
    const size_t space1 = statusLine.find(' ');
    if (space1 == std::string_view::npos) {
        throw TuringException("Malformed HTTP response status line");
    }
    const std::string_view afterVersion = statusLine.substr(space1 + 1);
    const size_t space2 = afterVersion.find(' ');
    const std::string_view codeStr = (space2 == std::string_view::npos)
                                       ? afterVersion
                                       : afterVersion.substr(0, space2);
    int statusCode = 0;
    for (const char c : codeStr) {
        if (c < '0' || c > '9') {
            throw TuringException("Malformed HTTP status code");
        }
        statusCode = statusCode * 10 + (c - '0');
    }
    return statusCode;
}

// Inverse of net::http::writeHexU32. Lowercases each digit first so the a-f
// and A-F cases collapse into one branch.
size_t parseHexU32(std::string_view hexDigits) {
    size_t value = 0;
    for (const char c : hexDigits) {
        const char lc = static_cast<char>(std::tolower(static_cast<unsigned char>(c)));
        size_t digit = 0;
        if (lc >= '0' && lc <= '9') {
            digit = static_cast<size_t>(lc - '0');
        } else if (lc >= 'a' && lc <= 'f') {
            digit = static_cast<size_t>(lc - 'a') + 10;
        } else {
            throw TuringException("Invalid character in chunk size line");
        }
        value = (value << 4) | digit;
    }
    return value;
}

// Server-side ChangeID/CommitHash::fromString() parses with std::from_chars(..., 16),
// so the wire encoding must be hex. std::to_string() would silently emit decimal,
// which matches hex only for values 0-9 and then misroutes everything from 10 on.
static std::string toHexString(uint64_t value) {
    std::array<char, 17> buffer;
    const auto res = std::to_chars(buffer.data(), buffer.data() + buffer.size(), value, 16);
    return std::string(buffer.data(), res.ptr);
}

// Classifies the result of a recv/recvmsg/send syscall. Throws on a fatal error or a peer
// close; otherwise tells the caller whether to reissue the call, yield to the event loop,
// or proceed with the transferred bytes.
TuringAsyncClient::AsyncIOProgress classifyIOResult(ssize_t result,
                                                    const std::string_view failMessage,
                                                    const std::string_view closedMessage) {
    if (result < 0) {
        if (errno == EINTR) {
            return TuringAsyncClient::AsyncIOProgress::Retry;
        } else if (errno == EAGAIN || errno == EWOULDBLOCK) {
            return TuringAsyncClient::AsyncIOProgress::WouldBlock;
        } else {
            throw TuringException(std::string(failMessage) + ": " + strerror(errno));
        }
    } else if (result == 0) {
        throw TuringException(std::string(closedMessage));
    }

    return TuringAsyncClient::AsyncIOProgress::Ok;
}

// After reading 'bytesProcessed' bytes from the socket we use this function to advance our
// iovec
template <size_t numIoVecs>
void advanceIovecs(std::array<iovec, numIoVecs>& iovecs, size_t& iovIndex, size_t bytesProcessed) {
    size_t left = bytesProcessed;
    // advance the iovec index
    while (iovIndex < iovecs.size() && left >= iovecs[iovIndex].iov_len) {
        left -= iovecs[iovIndex].iov_len;
        ++iovIndex;
    }

    // if we have only partially processed data in an iov - modify the starting pointer
    // and the length of the iov, so the system call starts processing the iov from the
    // correct point when called again
    if (iovIndex < iovecs.size() && left > 0) {
        iovec& current = iovecs[iovIndex];
        current.iov_base = static_cast<char*>(current.iov_base) + left;
        current.iov_len -= left;
    }
}

}

TuringAsyncClient::TuringAsyncClient(const std::string& remoteAddress,
                                     const std::string& remotePort,
                                     db::LocalMemory* localMem,
                                     size_t bufferCapacity)
    : _remoteAddress(remoteAddress),
    _remotePort(remotePort),
    _localMem(localMem),
    _inBuf(bufferCapacity),
    _dfMan(std::make_unique<db::DataframeManager>()),
    _df(std::make_unique<db::Dataframe>()),
    _decoder(std::make_unique<TuringProtoDecoder<TuringSink>>(&_inBuf, &_sink, _colSchemas))
{
    _sendBuffer.reserve(256);
}

TuringAsyncClient::~TuringAsyncClient() {
    if (_socket >= 0) {
        ::close(_socket);
    }
}

/**--------Socket Connection And Disconnection Functions--------**/
void TuringAsyncClient::connect() {
    disconnect();

    addrinfo hints {};
    hints.ai_family = AF_UNSPEC;
    hints.ai_socktype = SOCK_STREAM;

    addrinfo* result = nullptr;
    const int gaiStatus = ::getaddrinfo(_remoteAddress.c_str(), _remotePort.c_str(), &hints, &result);
    if (gaiStatus != 0) {
        throw TuringException(std::string("Failed to resolve remote address: ")
                              + ::gai_strerror(gaiStatus));
    }

    int lastErrno = 0;
    for (addrinfo* addr = result; addr != nullptr; addr = addr->ai_next) {
        const int sock = ::socket(addr->ai_family, addr->ai_socktype, addr->ai_protocol);
        if (sock < 0) {
            lastErrno = errno;
            continue;
        }

        // The event loop drives this socket through epoll, so it must be non-blocking.
        // SOCK_NONBLOCK can't ride along in the getaddrinfo hints (it rejects it as an
        // invalid ai_socktype), so set O_NONBLOCK on the fd after creating it.
        const int flags = ::fcntl(sock, F_GETFL, 0);
        if (flags < 0 || ::fcntl(sock, F_SETFL, flags | O_NONBLOCK) < 0) {
            lastErrno = errno;
            ::close(sock);
            continue;
        }

        // A non-blocking connect() typically returns -1/EINPROGRESS: the handshake is
        // still in flight and completes later, surfaced as the socket becoming writable.
        // Treat that as success and let the event loop finish it; any other errno is fatal.
        const int connectResult = ::connect(sock, addr->ai_addr, addr->ai_addrlen);
        if (connectResult != 0 && errno != EINPROGRESS) {
            lastErrno = errno;
            ::close(sock);
            continue;
        }

        _socket = sock;
        ::freeaddrinfo(result);
        return;
    }

    ::freeaddrinfo(result);
    throw TuringException(std::string("Failed to connect to ")
                          + _remoteAddress + ":" + _remotePort + ": "
                          + strerror(lastErrno));
}

void TuringAsyncClient::disconnect() {
    _inBuf.reset();
    _scratchHead = 0;
    _scratchTail = 0;
    if (_socket >= 0) {
        ::close(_socket);
        _socket = -1;
    }
}
/**-------------------------------------------------------------**/

/**-------- Buffer Receive And Processing Functions--------**/
TuringAsyncClient::AsyncIOProgress TuringAsyncClient::recvToChunkFramingBuffer(size_t need) {
    drainScratch(_chunkFramingBuf.data(), need, _chunkFramingBufOffset);

    while (_chunkFramingBufOffset < need) {
        const ssize_t bytesRead = ::recv(_socket,
                                         _chunkFramingBuf.data() + _chunkFramingBufOffset,
                                         need - _chunkFramingBufOffset,
                                         0);
        const AsyncIOProgress progress = classifyIOResult(bytesRead,
                                                          "Failed to read chunk framing",
                                                          "Connection closed mid-response");
        if (progress == AsyncIOProgress::Retry) {
            continue;
        } else if (progress == AsyncIOProgress::WouldBlock) {
            return progress;
        }

        _chunkFramingBufOffset += static_cast<size_t>(bytesRead);
    }

    return AsyncIOProgress::Ok;
}

TuringAsyncClient::AsyncIOProgress TuringAsyncClient::recvToHTTPHeaderScratchBuffer() {
    while (true) {
        const std::string_view scratch(_httpScratch.data(), _scratchTail);
        const size_t headerEnd = scratch.find("\r\n\r\n");

        if (headerEnd != std::string_view::npos) {
            const std::string_view headerBlock(_httpScratch.data(), headerEnd);
            const size_t crlf = headerBlock.find("\r\n");
            const std::string_view statusLine = (crlf == std::string_view::npos)
                                                  ? headerBlock
                                                  : headerBlock.substr(0, crlf);
            const int statusCode = parseStatusCode(statusLine);
            if (statusCode != 200) {
                throw TuringException("Server returned HTTP " + std::to_string(statusCode));
            }

            _scratchHead = headerEnd + HEADERS_END_SIZE;
            _chunkFramingBufOffset = 0;
            _recvState = RecvState::ChunkSize;
            break;
        }

        if (_scratchTail == HTTP_SCRATCH_CAPACITY) {
            throw TuringException("HTTP response headers exceed scratch capacity");
        }

        const ssize_t bytesRead = ::recv(_socket,
                                         _httpScratch.data() + _scratchTail,
                                         HTTP_SCRATCH_CAPACITY - _scratchTail,
                                         0);
        const AsyncIOProgress progress = classifyIOResult(bytesRead,
                                                          "Failed to read HTTP response",
                                                          "Connection closed during HTTP read");
        if (progress == AsyncIOProgress::Retry) {
            continue;
        } else if (progress == AsyncIOProgress::WouldBlock) {
            return progress;
        }

        _scratchTail += static_cast<size_t>(bytesRead);
    }

    return AsyncIOProgress::Ok;
}

TuringAsyncClient::AsyncIOProgress TuringAsyncClient::recvChunk() {
    while (_protoHeaderRecvIovecIndex < _protoHeaderRecvIovecs.size()) {
        msghdr msg {};
        msg.msg_iov = _protoHeaderRecvIovecs.data() + _protoHeaderRecvIovecIndex;
        msg.msg_iovlen = _protoHeaderRecvIovecs.size() - _protoHeaderRecvIovecIndex;

        const ssize_t bytesRead = ::recvmsg(_socket, &msg, 0);
        const AsyncIOProgress progress = classifyIOResult(bytesRead,
                                                          "recvmsg failed",
                                                          "Connection closed mid-chunk");
        if (progress == AsyncIOProgress::Retry) {
            continue;
        } else if (progress == AsyncIOProgress::WouldBlock) {
            return progress;
        }

        advanceIovecs(_protoHeaderRecvIovecs,
                      _protoHeaderRecvIovecIndex,
                      static_cast<size_t>(bytesRead));
    }

    return AsyncIOProgress::Ok;
}

TuringAsyncClient::RecvState TuringAsyncClient::processChunkSize() {
    if (_chunkFramingBuf[net::http::CHUNK_HEX_DIGITS] != '\r'
        || _chunkFramingBuf[net::http::CHUNK_HEX_DIGITS + 1] != '\n') {
        throw TuringException("Malformed chunk size line: expected CRLF after hex digits");
    }

    _incomingHTTPChunkSize = parseHexU32(std::string_view(_chunkFramingBuf.data(), net::http::CHUNK_HEX_DIGITS));
    _chunkFramingBufOffset = 0;

    if (_incomingHTTPChunkSize == 0) {
        // End-of-response terminator chunk: only its trailing CRLF remains.
        return RecvState::Crlf;
    }

    if (_incomingHTTPChunkSize < ProtoHeader::wireSize()) {
        throw TuringException("Server chunk smaller than ProtoHeader wire size");
    }
    const size_t payloadSize = _incomingHTTPChunkSize - ProtoHeader::wireSize();

    _inBuf.reset();
    if (payloadSize > _inBuf.capacity()) {
        throw TuringException("Server proto payload exceeds client inbuf capacity");
    }

    _protoHeaderRecvIovecs = {
        {
         {_protoHeaderBuf.data(), ProtoHeader::wireSize()},
         {_inBuf.data(), payloadSize},
         }
    };
    _protoHeaderRecvIovecIndex = 0;
    drainScratchIntoIovecs(_protoHeaderRecvIovecs, _protoHeaderRecvIovecIndex);

    return RecvState::Chunk;
}

TuringAsyncClient::RecvState TuringAsyncClient::processCrlf() {
    if (_chunkFramingBuf[0] != '\r' || _chunkFramingBuf[1] != '\n') {
        throw TuringException("Expected CRLF between chunks");
    }
    _chunkFramingBufOffset = 0;

    if (_incomingHTTPChunkSize == 0) {
        // Terminator chunk fully consumed: the response stream is complete.
        return RecvState::Done;
    }

    if (_sawTerminalPacket) {
        throw TuringException("Unexpected proto packet after END/ERROR");
    }

    processProtoPacket();

    // Loop back to read the next chunk's size line.
    return RecvState::ChunkSize;
}

void TuringAsyncClient::processProtoPacket() {
    // Decode the 5-byte ProtoHeader staged in _protoHeaderBuf and dispatch on the packet
    // type. _inBuf holds the payload, so the header's dataLen must match its size.
    const ProtoHeader responseHeader = ProtoHeader::decode(_protoHeaderBuf.data(), _protoHeaderBuf.size());
    if (responseHeader._dataLen != _inBuf.size()) {
        throw TuringException("Proto header dataLen does not match chunk payload size");
    }

    TuringSinkColumnContainer dataframeContainer(_df.get(), _dfMan.get());

    switch (responseHeader._type) {
        case MessageTypes::CHUNK_HEADER:
            _decoder->decodeIncomingChunkHeader(&dataframeContainer);
        break;

        case MessageTypes::CHUNK:
            _decoder->decodeIncomingChunk(&dataframeContainer);
        break;

        case MessageTypes::END_CHUNK:
            _callbackFired = true;
            _callback(_df.get());
            _df->clear();
            for (auto& schema : _colSchemas) {
                schema.getColState().reset();
            }
            _decoder->reset();
        break;

        case MessageTypes::END: {
            if (!_callbackFired) {
                _callback(_df.get());
            }

            if (responseHeader._dataLen != sizeof(db::QueryCallbacks::ExecTimeMilliseconds)) {
                throw TuringException("Invalid END packet payload size");
            }

            db::QueryCallbacks::ExecTimeMilliseconds totalTimeMs = 0;
            memcpy(&totalTimeMs, _inBuf.data(), sizeof(totalTimeMs));
            _res.setTotalTime(Milliseconds(totalTimeMs));
            _sawTerminalPacket = true;
        }
        break;

        case MessageTypes::ERROR: {
            if (_inBuf.size() < sizeof(db::QueryStatus::Status)) {
                throw TuringException("Invalid ERROR packet payload size");
            }

            db::QueryStatus::Status status;
            memcpy(&status, _inBuf.data(), sizeof(status));
            _res.setStatus(status);
            _res.setMessage(std::string_view(_inBuf.data() + sizeof(status),
                                             _inBuf.size() - sizeof(status)));
            // ERROR is not the response terminator — the server still emits an END packet
            // afterwards (with the elapsed time) even when the query failed. Mirrors the
            // binary protocol semantics: the END is what closes the response stream.
        }
        break;

        case MessageTypes::PROTOCOL_ERROR:
            throw TuringException("Protocol error from server: "
                                  + std::string(_inBuf.data(), _inBuf.size()));
        break;

        default:
            throw TuringException("Invalid message type received");
        break;
    }
}
/**-------------------------------------------------------**/

/**-------- Drain Scratch Buffer Functions--------**/
void TuringAsyncClient::drainScratch(char* dst, size_t need, size_t& filled) {
    if (_scratchHead >= _scratchTail || filled >= need) {
        return;
    }

    const size_t available = _scratchTail - _scratchHead;
    const size_t want = need - filled;
    const size_t take = std::min(available, want);
    memcpy(dst + filled, _httpScratch.data() + _scratchHead, take);
    _scratchHead += take;
    filled += take;
}

void TuringAsyncClient::drainScratchIntoIovecs(std::array<iovec, 2>& iovecs, size_t& iovIndex) {
    while (iovIndex < iovecs.size() && _scratchHead < _scratchTail) {
        const size_t take = std::min(_scratchTail - _scratchHead, iovecs[iovIndex].iov_len);
        memcpy(iovecs[iovIndex].iov_base, _httpScratch.data() + _scratchHead, take);
        _scratchHead += take;
        advanceIovecs(iovecs, iovIndex, take);
    }
}
/**-------- --------------------------------------**/

/**-------- Send Functions --------**/
void TuringAsyncClient::buildRequest(const std::string& query) {
    // Serialize the whole request (headers + body) into the owned send buffer; send()
    // pumps it from _sendOffset and resumes across EAGAIN. Keep-Alive means the buffer is
    // reused across requests, so clear it first and size it for this request's body.
    _sendBuffer.clear();
    _sendBuffer.reserve(query.size() + 256);

    const std::string commitParam = (_commitHash.get() == db::CommitHash::head().get())
                                      ? std::string("head")
                                      : toHexString(_commitHash.get());
    const std::string changeParam = (_changeID.get() == db::ChangeID::head().get())
                                      ? std::string("head")
                                      : toHexString(_changeID.get());

    _sendBuffer += "POST /query?graph=";
    _sendBuffer += _graphName;
    _sendBuffer += "&commit=";
    _sendBuffer += commitParam;
    _sendBuffer += "&change=";
    _sendBuffer += changeParam;
    _sendBuffer += " HTTP/1.1\r\n";

    _sendBuffer += "Host: ";
    _sendBuffer += _remoteAddress;
    _sendBuffer += ":";
    _sendBuffer += _remotePort;
    _sendBuffer += "\r\n";

    _sendBuffer += "Content-type: application/turing-proto\r\n";
    _sendBuffer += "Content-Length: ";
    _sendBuffer += std::to_string(query.size());
    _sendBuffer += "\r\n";
    _sendBuffer += "Connection: Keep-Alive\r\n";
    _sendBuffer += "\r\n";

    // The body follows the header block in the same buffer — no separate copy, no second
    // iovec, and send() only reads the bytes so no const_cast is needed.
    _sendBuffer += query;

    _sendOffset = 0;
    _sending = true;
}

void TuringAsyncClient::send() {
    if (!_sending) {
        return;
    }

    while (_sendOffset < _sendBuffer.size()) {
        const ssize_t bytesSent = ::send(_socket,
                                         _sendBuffer.data() + _sendOffset,
                                         _sendBuffer.size() - _sendOffset,
                                         MSG_NOSIGNAL);
        const AsyncIOProgress progress = classifyIOResult(bytesSent, "Failed to send HTTP request", "send returned 0");
        if (progress == AsyncIOProgress::Retry) {
            continue;
        } else if (progress == AsyncIOProgress::WouldBlock) {
            return;
        }

        _sendOffset += static_cast<size_t>(bytesSent);
    }

    _sending = false;
}

db::QueryStatus TuringAsyncClient::sendQuery(const std::string& query,
                                             const db::QueryCallbacks::OnOutputData& callback) {
    bioassert(query.length() <= std::numeric_limits<uint32_t>::max(), "Query length exceeds uint32 maximum");
    bioassert(_graphName.length() <= std::numeric_limits<uint32_t>::max(), "Graph name length exceeds uint32 maximum");

    // Prime the per-query state and stage the request, then make the first send/recv
    // attempt. On a blocking socket this runs the whole exchange; on a non-blocking
    // socket send()/recv() return as soon as they would block and the caller resumes
    // them from its event loop until isRecvComplete(). The returned status is only final
    // once isRecvComplete() is true.
    reset();
    _callback = callback;

    buildRequest(query);

    send();
    if (isSendComplete()) {
        recv();
    }

    return _res;
}
/**--------------------------------**/

void TuringAsyncClient::recv() {
    while (true) {
        switch (_recvState) {
            case RecvState::ScratchBuffer: {
                // Read into _httpScratch until the end-of-headers sentinel appears.
                auto progress = recvToHTTPHeaderScratchBuffer();

                if (progress == AsyncIOProgress::WouldBlock) {
                    return;
                }
            }
            break;

            case RecvState::ChunkSize: {
                // The fixed-width chunk size line: 8 hex digits + CRLF.
                auto progress = recvToChunkFramingBuffer(_chunkFramingBuf.size());
                if (progress == TuringAsyncClient::AsyncIOProgress::WouldBlock) {
                    return ;
                }

                _recvState = processChunkSize();
            }
            break;

            case RecvState::Chunk: {
                // ProtoHeader + payload. Any bytes already pulled from _httpScratch were
                // drained into the iovecs during the ChunkSize transition.
                auto res = recvChunk();

                if (res == AsyncIOProgress::WouldBlock) {
                    return;
                }

                _inBuf.increaseWriteOffset(_incomingHTTPChunkSize - ProtoHeader::wireSize());
                _chunkFramingBufOffset = 0;
                _recvState = RecvState::Crlf;
            }
            break;

            case RecvState::Crlf: {
                // The CRLF trailing the chunk we just read (a data chunk or the terminator).
                auto progress = recvToChunkFramingBuffer(CRLF_SIZE);
                if (progress == TuringAsyncClient::AsyncIOProgress::WouldBlock) {
                    return ;
                }

                _recvState = processCrlf();
            }
            break;

            case RecvState::Done: {
                return;
            }
            break;
        }
    }
}

void TuringAsyncClient::reset() {
    // Outgoing request state.
    _sendBuffer.clear();
    _sendOffset = 0;
    _sending = false;

    // HTTP framing scratch and chunk-framing state machine.
    _scratchHead = 0;
    _scratchTail = 0;
    _recvState = RecvState::ScratchBuffer;
    _chunkFramingBufOffset = 0;
    _incomingHTTPChunkSize = 0;
    _protoHeaderRecvIovecIndex = 0;

    // Proto payload and decoded-response state. _df/_dfMan are rebuilt from scratch so each
    // query starts with an empty dataframe; recreating _dfMan frees the columns it owned for
    // the previous query. _decoder is rebound to the fresh _dfMan.
    _inBuf.reset();
    _embeddingBuffer.clear();
    _stringBuffer.clear();
    _listBuffer.clear();
    _sink.reset();
    _colSchemas.clear();
    _dfMan = std::make_unique<db::DataframeManager>();
    _df = std::make_unique<db::Dataframe>();
    _decoder = std::make_unique<TuringProtoDecoder<TuringSink>>(&_inBuf,
                                                    &_sink,
                                                    _colSchemas);

    // Per-query callback and accumulated result.
    _callback = db::QueryCallbacks::OnOutputData();
    _res = db::QueryStatus();
    _callbackFired = false;
    _sawTerminalPacket = false;
}

#include "TuringClient.h"

#include <errno.h>
#include <stdlib.h>
#include <string.h>
#include <netdb.h>
#include <sys/socket.h>
#include <sys/uio.h>
#include <unistd.h>
#include <algorithm>
#include <array>
#include <cctype>
#include <charconv>
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

#include "BioAssert.h"

using namespace net::proto;

namespace {

// Length of the "\r\n\r\n" sentinel that terminates an HTTP/1.1 header block.
constexpr size_t HEADERS_END_SIZE = 4;

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

// Rejects control characters (anything below 0x20, plus DEL 0x7f) in a value
// that will be concatenated into the HTTP request line or a header. CR and LF
// are the dangerous case — an embedded "\r\n" terminates the header block early
// and lets a caller splice in arbitrary headers or a request body — but no
// control character is legal in a request target or header value, so the whole
// class is rejected. fieldName names the offending field in the thrown message.
void throwIfControlChars(const char* fieldName, std::string_view value) {
    for (const char c : value) {
        const unsigned char byte = static_cast<unsigned char>(c);
        if (byte < 0x20 || byte == 0x7f) {
            throw TuringException(std::string("Invalid ") + fieldName
                                  + ": control characters (including CR/LF) are not allowed");
        }
    }
}

}

TuringClient::TuringClient(const std::string& remoteAddress,
                           const std::string& remotePort,
                           db::LocalMemory* localMem,
                           size_t bufferCapacity)
    : _remoteAddress(remoteAddress),
    _remotePort(remotePort),
    _localMem(localMem),
    _inBuf(bufferCapacity)
{
    // Default the auth token from the environment so C++ consumers (shell,
    // tools) authenticate automatically against an -auth-on server. An explicit
    // setAuthToken() overrides this. Route it through the setter so a malformed
    // env value (e.g. a stray trailing newline) fails loudly instead of
    // corrupting every request.
    const char* envToken = getenv("TURINGDB_AUTH_TOKEN");
    if (envToken != nullptr && *envToken != '\0') {
        setAuthToken(envToken);
    }
}

TuringClient::~TuringClient() {
    if (_socket >= 0) {
        ::close(_socket);
    }
}

void TuringClient::setGraphName(const std::string& graphName) {
    throwIfControlChars("graph name", graphName);
    _graphName = graphName;
}

void TuringClient::setAuthToken(const std::string& authToken) {
    throwIfControlChars("auth token", authToken);
    _authToken = authToken;
}

void TuringClient::connect() {
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

        if (::connect(sock, addr->ai_addr, addr->ai_addrlen) != 0) {
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

void TuringClient::disconnect() {
    _inBuf.reset();
    _scratchHead = 0;
    _scratchTail = 0;
    if (_socket >= 0) {
        ::close(_socket);
        _socket = -1;
    }
}

// Server-side ChangeID/CommitHash::fromString() parses with std::from_chars(..., 16),
// so the wire encoding must be hex. std::to_string() would silently emit decimal,
// which matches hex only for values 0-9 and then misroutes everything from 10 on.
static std::string toHexString(uint64_t value) {
    std::array<char, 17> buffer;
    const auto res = std::to_chars(buffer.data(), buffer.data() + buffer.size(), value, 16);
    return std::string(buffer.data(), res.ptr);
}

void TuringClient::sendRequest(const std::string& query) {
    std::string headers;
    headers.reserve(256);

    const std::string commitParam = (_commitHash.get() == db::CommitHash::head().get())
                                      ? std::string("head")
                                      : toHexString(_commitHash.get());
    const std::string changeParam = (_changeID.get() == db::ChangeID::head().get())
                                      ? std::string("head")
                                      : toHexString(_changeID.get());

    headers += "POST /query?graph=";
    headers += _graphName;
    headers += "&commit=";
    headers += commitParam;
    headers += "&change=";
    headers += changeParam;
    headers += " HTTP/1.1\r\n";

    headers += "Host: ";
    headers += _remoteAddress;
    headers += ":";
    headers += _remotePort;
    headers += "\r\n";

    headers += "Content-type: application/turing-proto\r\n";
    headers += "Content-Length: ";
    headers += std::to_string(query.size());
    headers += "\r\n";

    if (!_authToken.empty()) {
        headers += "Authorization: Bearer ";
        headers += _authToken;
        headers += "\r\n";
    }

    headers += "Connection: Keep-Alive\r\n";
    headers += "\r\n";

    // iovec.iov_base is void* (non-const) because the struct is shared with
    // recvmsg, which writes through it. sendmsg only reads, so the const_cast
    // on query.data() is a POSIX-ABI wrinkle , not a real mutation.
    std::array<iovec, 2> iovecs = {{
        {headers.data(), headers.size()},
        {const_cast<char*>(query.data()), query.size()},
    }};

    size_t iovIndex = 0;
    while (iovIndex < iovecs.size()) {
        msghdr msg {};
        msg.msg_iov = iovecs.data() + iovIndex;
        msg.msg_iovlen = iovecs.size() - iovIndex;

        const ssize_t bytesSent = ::sendmsg(_socket, &msg, MSG_NOSIGNAL);

        if (bytesSent < 0) {
            if (errno == EINTR || errno == EAGAIN || errno == EWOULDBLOCK) {
                continue;
            }
            throw TuringException(std::string("Failed to send HTTP request: ") + strerror(errno));
        }

        if (bytesSent == 0) {
            throw TuringException("sendmsg returned 0");
        }

        size_t left = static_cast<size_t>(bytesSent);
        while (iovIndex < iovecs.size() && left >= iovecs[iovIndex].iov_len) {
            left -= iovecs[iovIndex].iov_len;
            ++iovIndex;
        }
        if (iovIndex < iovecs.size() && left > 0) {
            iovecs[iovIndex].iov_base = static_cast<char*>(iovecs[iovIndex].iov_base) + left;
            iovecs[iovIndex].iov_len -= left;
        }
    }
}

void TuringClient::fillScratch() {
    const size_t space = HTTP_SCRATCH_CAPACITY - _scratchTail;
    if (space == 0) {
        throw TuringException("HTTP scratch buffer full");
    }

    while (true) {
        const ssize_t bytesRead = ::recv(_socket,
                                         _httpScratch.data() + _scratchTail,
                                         space,
                                         0);
        if (bytesRead < 0) {
            if (errno == EINTR) {
                continue;
            }
            throw TuringException(std::string("Failed to read HTTP response: ") + strerror(errno));
        }
        if (bytesRead == 0) {
            throw TuringException("Connection closed during HTTP read");
        }
        _scratchTail += static_cast<size_t>(bytesRead);
        return;
    }
}

void TuringClient::recvExactly(void* dst, size_t len) {
    char* out = static_cast<char*>(dst);
    size_t need = len;

    //drain any extra data pulled into the scratch buffer into the iovec buffer
    if (_scratchHead < _scratchTail) {
        const size_t available = _scratchTail - _scratchHead;
        const size_t sizeToCopy = std::min(available, need);
        memcpy(out, _httpScratch.data() + _scratchHead, sizeToCopy);
        _scratchHead += sizeToCopy;
        out += sizeToCopy;
        need -= sizeToCopy;
    }

    while (need > 0) {
        const ssize_t bytesRead = ::recv(_socket, out, need, 0);
        if (bytesRead < 0) {
            if (errno == EINTR) {
                continue;
            }
            throw TuringException(std::string("Failed to read response body: ") + strerror(errno));
        }
        if (bytesRead == 0) {
            throw TuringException("Connection closed mid-response");
        }
        out += bytesRead;
        need -= static_cast<size_t>(bytesRead);
    }
}

void TuringClient::recvHttpResponseHeaders() {
    _scratchHead = 0;
    _scratchTail = 0;

    while (true) {
        fillScratch();

        // Search for end-of-headers sentinel (\r\n\r\n) in the buffered bytes.
        if (_scratchTail < 4) {
            continue;
        }

        for (size_t i = 0; i + 3 < _scratchTail; ++i) {
            const bool match = _httpScratch[i] == '\r'
                            && _httpScratch[i + 1] == '\n'
                            && _httpScratch[i + 2] == '\r'
                            && _httpScratch[i + 3] == '\n';
            if (!match) {
                continue;
            }

            const std::string_view headerBlock(_httpScratch.data(), i);
            const size_t crlf = headerBlock.find("\r\n");
            const std::string_view statusLine = (crlf == std::string_view::npos)
                                                  ? headerBlock
                                                  : headerBlock.substr(0, crlf);
            const int statusCode = parseStatusCode(statusLine);
            if (statusCode != 200) {
                throw TuringException("Server returned HTTP " + std::to_string(statusCode));
            }

            _scratchHead = i + HEADERS_END_SIZE;
            return;
        }

        if (_scratchTail == HTTP_SCRATCH_CAPACITY) {
            throw TuringException("HTTP response headers exceed scratch capacity");
        }
    }
}

size_t TuringClient::recvChunkSizeLine() {
    // Server emits a fixed-width chunk size line: 8 hex digits + CRLF.
    // Both data chunks and the terminator chunk use this format.
    std::array<char, net::http::CHUNK_HEADER_LINE_SIZE> line;
    recvExactly(line.data(), line.size());

    if (line[net::http::CHUNK_HEX_DIGITS] != '\r'
        || line[net::http::CHUNK_HEX_DIGITS + 1] != '\n') {
        throw TuringException("Malformed chunk size line: expected CRLF after hex digits");
    }

    return parseHexU32(std::string_view(line.data(), net::http::CHUNK_HEX_DIGITS));
}

void TuringClient::recvChunkBody(size_t chunkSize, ProtoHeader* outHeader) {
    bioassert(chunkSize >= ProtoHeader::wireSize(), "Chunk smaller than ProtoHeader");
    const size_t payloadSize = chunkSize - ProtoHeader::wireSize();

    _inBuf.reset();
    if (payloadSize > _inBuf.capacity()) {
        throw TuringException("Server proto payload exceeds client inbuf capacity");
    }

    std::array<iovec, 2> iovecs = {{
        {_protoHeaderBuf.data(), ProtoHeader::wireSize()},
        {_inBuf.data(), payloadSize},
    }};

    size_t iovIndex = 0;

    // Drain any leftover scratch bytes into the iovecs before issuing readv.
    while (iovIndex < iovecs.size() && _scratchHead < _scratchTail) {
        const size_t available = _scratchTail - _scratchHead;
        const size_t need = iovecs[iovIndex].iov_len;
        const size_t take = std::min(available, need);
        memcpy(iovecs[iovIndex].iov_base, _httpScratch.data() + _scratchHead, take);
        _scratchHead += take;
        if (take == need) {
            ++iovIndex;
        } else {
            iovecs[iovIndex].iov_base = static_cast<char*>(iovecs[iovIndex].iov_base) + take;
            iovecs[iovIndex].iov_len -= take;
        }
    }

    while (iovIndex < iovecs.size()) {
        msghdr msg {};
        msg.msg_iov = iovecs.data() + iovIndex;
        msg.msg_iovlen = iovecs.size() - iovIndex;
        const ssize_t bytesRead = ::recvmsg(_socket,
                                            &msg,
                                            0);
        if (bytesRead < 0) {
            if (errno == EINTR) {
                continue;
            }
            throw TuringException(std::string("readv failed: ") + strerror(errno));
        }
        if (bytesRead == 0) {
            throw TuringException("Connection closed mid-chunk");
        }

        size_t left = static_cast<size_t>(bytesRead);
        while (iovIndex < iovecs.size() && left >= iovecs[iovIndex].iov_len) {
            left -= iovecs[iovIndex].iov_len;
            ++iovIndex;
        }
        if (iovIndex < iovecs.size() && left > 0) {
            iovecs[iovIndex].iov_base = static_cast<char*>(iovecs[iovIndex].iov_base) + left;
            iovecs[iovIndex].iov_len -= left;
        }
    }

    _inBuf.increaseWriteOffset(payloadSize);
    *outHeader = ProtoHeader::decode(_protoHeaderBuf.data(), _protoHeaderBuf.size());
}

void TuringClient::recvCrlf() {
    std::array<char, 2> buffer;
    recvExactly(buffer.data(), buffer.size());
    if (buffer[0] != '\r' || buffer[1] != '\n') {
        throw TuringException("Expected CRLF between chunks");
    }
}

db::QueryStatus TuringClient::sendQuery(const std::string& query,
                                        const db::QueryCallbacks::OnOutputData& callback) {
    bioassert(query.length() <= MAX_WIRE_SIZE, "Query length exceeds maximum wire size");
    bioassert(_graphName.length() <= MAX_WIRE_SIZE, "Graph name length exceeds maximum wire size");

    sendRequest(query);
    recvHttpResponseHeaders();

    _embeddingBuffer.clear();
    _stringBuffer.clear();
    _listBuffer.clear();
    db::DataframeManager dfMan;
    db::QueryStatus res;
    std::vector<DecodedColumnSchema> colSchemas;
    db::Dataframe df;
    TuringSink sink(_localMem, &_embeddingBuffer, &_stringBuffer, &_listBuffer);
    TuringSinkColumnContainer dataframeContainer(&df, &dfMan);
    TuringProtoDecoder<TuringSink> decoder(&_inBuf, &sink, colSchemas);

    bool callbackFired = false;
    bool sawTerminalPacket = false;

    while (true) {
        const size_t chunkSize = recvChunkSizeLine();

        if (chunkSize == 0) {
            // End-of-response terminator chunk: consume trailing CRLF and stop.
            recvCrlf();
            break;
        }

        ProtoHeader responseHeader;
        recvChunkBody(chunkSize, &responseHeader);
        recvCrlf();

        if (sawTerminalPacket) {
            throw TuringException("Unexpected proto packet after END/ERROR");
        }

        if (responseHeader._dataLen != _inBuf.size()) {
            throw TuringException("Proto header dataLen does not match chunk payload size");
        }

        switch (responseHeader._type) {
            case MessageTypes::CHUNK_HEADER:
                decoder.decodeIncomingChunkHeader(&dataframeContainer);
            break;

            case MessageTypes::CHUNK:
                decoder.decodeIncomingChunk(&dataframeContainer);
            break;

            case MessageTypes::END_CHUNK:
                callbackFired = true;
                callback(&df);
                df.clear();
                for (auto& schema : colSchemas) {
                    schema.getColState().reset();
                }
                decoder.reset();
            break;

            case MessageTypes::END: {
                if (!callbackFired) {
                    callback(&df);
                }

                if (responseHeader._dataLen != sizeof(db::QueryCallbacks::ExecTimeMilliseconds)) {
                    throw TuringException("Invalid END packet payload size");
                }

                db::QueryCallbacks::ExecTimeMilliseconds totalTimeMs = 0;
                memcpy(&totalTimeMs, _inBuf.data(), sizeof(totalTimeMs));
                res.setTotalTime(Milliseconds(totalTimeMs));
                sawTerminalPacket = true;
            }
            break;

            case MessageTypes::ERROR: {
                if (_inBuf.size() < sizeof(db::QueryStatus::Status)) {
                    throw TuringException("Invalid ERROR packet payload size");
                }

                db::QueryStatus::Status status;
                memcpy(&status, _inBuf.data(), sizeof(status));
                res.setStatus(status);
                res.setMessage(std::string_view(_inBuf.data() + sizeof(status),
                                                _inBuf.size() - sizeof(status)));
                // ERROR is not the response terminator — the server still
                // emits an END packet afterwards (with the elapsed time)
                // even when the query failed. Mirrors the binary protocol
                // semantics: the END is what closes the response stream.
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

    return res;
}

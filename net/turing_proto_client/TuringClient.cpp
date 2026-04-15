#include "TuringClient.h"

#include <errno.h>
#include <string.h>
#include <netdb.h>
#include <sys/socket.h>
#include <unistd.h>
#include <array>
#include <vector>

#include "TuringProtoDecoder.h"
#include "TuringProtoHeaders.h"
#include "TuringException.h"
#include "dataframe/Dataframe.h"
#include "dataframe/DataframeManager.h"

#include "BioAssert.h"

using namespace net::proto;

TuringClient::TuringClient(const std::string& remoteAddress,
                           const std::string& remotePort,
                           db::LocalMemory* localMem, size_t bufferCapacity)
    : _remoteAddress(remoteAddress),
    _remotePort(remotePort),
    _socket(-1),
    _localMem(localMem),
    _outBuf(bufferCapacity),
    _inBuf(bufferCapacity)
{
}

TuringClient::~TuringClient() {
    if (_socket >= 0) {
        ::close(_socket);
    }
}

void TuringClient::disconnect() {
    _inBuf.reset();
    _outBuf.reset();
    if (_socket >= 0) {
        ::close(_socket);
        _socket = -1;
    }
}

void TuringClient::recvAll(size_t recvLen) {
    _inBuf.reset();

    uint64_t totalBytesRead = 0;
    recvLen = std::min(recvLen, _inBuf.capacity());

    while (totalBytesRead < recvLen) {
        const ssize_t bytesRead = ::recv(_socket, _inBuf.end(), recvLen - totalBytesRead, 0);

        if (bytesRead < 0) {
            throw TuringException(std::string("Failed to read response: ") + strerror(errno));
        }

        if (bytesRead == 0) {
            throw TuringException("Connection closed before a response was received");
        }

        totalBytesRead += bytesRead;
        _inBuf.increaseWriteOffset(bytesRead);
    }
}

ProtoHeader TuringClient::recvMsgHeader() {
    std::array<char, ProtoHeader::wireSize()> recvHeaderBuf {};
    size_t totalBytesRead = 0;

    while (totalBytesRead < recvHeaderBuf.size()) {
        const ssize_t bytesRead = ::recv(_socket,
                                          recvHeaderBuf.data() + totalBytesRead,
                                          recvHeaderBuf.size() - totalBytesRead,
                                          0);

        if (bytesRead < 0) {
            throw TuringException(std::string("Failed to read response: ") + strerror(errno));
        }

        if (bytesRead == 0) {
            throw TuringException("Connection closed before a response was received");
        }

        totalBytesRead += bytesRead;
    }

    const ProtoHeader header = ProtoHeader::decode(recvHeaderBuf.data(), recvHeaderBuf.size());
    return header;
}

ProtoHeader TuringClient::send() {
    const char* data = _outBuf.data();
    size_t remainingBytes = _outBuf.size();

    while (remainingBytes > 0) {
        const ssize_t bytesSent = ::send(_socket, data, remainingBytes, MSG_NOSIGNAL);

        if (bytesSent < 0) {
            if (errno == EINTR || errno == EAGAIN || errno == EWOULDBLOCK) {
                continue;
            }

            throw TuringException(std::string("Failed to send query: ") + strerror(errno));
        }

        if (bytesSent == 0) {
            throw TuringException("Failed to send query: send returned 0");
        }

        data += bytesSent;
        remainingBytes -= bytesSent;
    }

    _outBuf.reset();
    return recvMsgHeader();
}

bool TuringClient::setUpConnection() {
    _inBuf.reset();
    _outBuf.reset();
    const ProtoHeader resHeader = sendHello();

    if (resHeader._dataLen != 0) {
        recvAll(resHeader._dataLen);
    }

    if (resHeader._type == MessageTypes::PROTOCOL_ERROR) {
        throw TuringException("Protocol error from server: "
                              + std::string(_inBuf.data(), _inBuf.size()));
    }

    if (resHeader._type != MessageTypes::IYI) {
        throw TuringException("Invalid hello response type received from server");
    }

    if (resHeader._dataLen != sizeof(uint8_t)) {
        throw TuringException("Invalid hello response payload size");
    }

    uint8_t response = 0;
    memcpy(&response, _inBuf.data(), sizeof(response));

    if (response > 1) {
        throw TuringException("Invalid hello response payload value");
    }

    return response != 0;
}

ProtoHeader TuringClient::sendHello() {
    const uint8_t protocolVersion = 1;
    const uint8_t keepAlive = static_cast<uint8_t>(true);
    const uint8_t timeout = 0;

    const size_t payloadSize =
        sizeof(protocolVersion) + sizeof(keepAlive) + sizeof(timeout);

    size_t offset = 0;
    std::vector<char> payload(payloadSize);

    const ProtoHeader header = {MessageTypes::NABER, sizeof(protocolVersion) + sizeof(keepAlive) + sizeof(timeout)};

    auto write = [&](const void* data, size_t size) {
        memcpy(payload.data() + offset, data, size);
        offset += size;
    };

    write(&protocolVersion, sizeof(protocolVersion));
    write(&keepAlive, sizeof(keepAlive));
    write(&timeout, sizeof(timeout));

    _outBuf.reset();
    frameMessage(header._type, std::string_view(payload.data(), header._dataLen), &_outBuf);
    return send();
}

void TuringClient::connect() {
    disconnect();

    addrinfo hints {0};
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

        if (!setUpConnection()) {
            ::close(sock);
            _socket = -1;
            ::freeaddrinfo(result);
            throw TuringException("Failed To Connect To Server");
        }

        ::freeaddrinfo(result);
        return;
    }

    ::freeaddrinfo(result);
    throw TuringException(std::string("Failed to connect to ")
                          + _remoteAddress + ":" + _remotePort + ": "
                          + strerror(lastErrno));
}

db::QueryStatus TuringClient::sendQuery(const std::string& query,
                                        const db::QueryCallbacks::OnOutputData& callback) {
    bioassert(query.length() <= std::numeric_limits<uint32_t>::max(), "String Size Is Too Large");
    bioassert(_graphName.length() <= std::numeric_limits<uint32_t>::max(), "Graph name is too large");

    const net::proto::QueryWireHeader queryHeader {
        ._commitHash = _commitHash.get(),
        ._changeID = _changeID.get(),
        ._graphNameLen = static_cast<uint32_t>(_graphName.length()),
        ._queryLen = static_cast<uint32_t>(query.length()),
    };

    size_t offset = 0;
    const size_t payloadSize = net::proto::QueryWireHeader::wireSize()
                             + _graphName.length()
                             + query.length();
    std::vector<char> payload(payloadSize);

    queryHeader.copyToBuffer(payload.data(), offset);
    memcpy(payload.data() + offset, _graphName.data(), _graphName.length());
    offset += _graphName.length();
    memcpy(payload.data() + offset, query.data(), query.length());
    offset += query.length();

    // Testing hook: uncomment to append a stray byte to the QUERY payload and
    // trip the server's "Incoming query payload size is inconsistent" protocol
    // error. Verifies the PROTOCOL_ERROR round-trip end-to-end.
    // payload.push_back('\x00');

    _outBuf.reset();
    bioassert(payload.size() <= std::numeric_limits<uint32_t>::max(), "Query payload size exceeds uint32 maximum");
    frameMessage(MessageTypes::QUERY,
                        std::string_view(payload.data(), static_cast<uint32_t>(payload.size())),
                        &_outBuf);

    _embeddingBuffer.clear();
    db::DataframeManager dfMan;
    db::QueryStatus res;
    std::vector<TuringProtoDecoder::DecodedColumnSchema> colSchemas;
    db::Dataframe df;
    ProtoHeader responseHeader = send();
    TuringProtoDecoder decoder(_localMem, &dfMan, &_inBuf, &_embeddingBuffer);
    bool callbackFired = false;
    while (true) {
        switch (responseHeader._type) {
            case MessageTypes::CHUNK_HEADER: {
                if (responseHeader._dataLen != 0) {
                    recvAll(responseHeader._dataLen);
                }

                decoder.decodeIncomingChunkHeader(&df, colSchemas);
            }
            break;

            case MessageTypes::CHUNK: {
                if (responseHeader._dataLen != 0) {
                    recvAll(responseHeader._dataLen);
                }

                decoder.decodeIncomingChunk(&df, colSchemas);
            }
            break;

            case MessageTypes::END_CHUNK: {
                callbackFired = true;
                callback(&df);

                df.clear();
                for (auto& schema : colSchemas) {
                    schema._colState.reset();
                }
                decoder.reset();
            }
            break;

            case MessageTypes::END: {
                if (!callbackFired) {
                    callback(&df);
                }

                if (responseHeader._dataLen != sizeof(db::QueryCallbacks::ExecTimeMilliseconds)) {
                    throw TuringException("Invalid END packet payload size");
                }

                recvAll(sizeof(db::QueryCallbacks::ExecTimeMilliseconds));

                db::QueryCallbacks::ExecTimeMilliseconds totalTimeMs = 0;
                memcpy(&totalTimeMs, _inBuf.data(), sizeof(totalTimeMs));
                res.setTotalTime(Milliseconds(totalTimeMs));
                return res;
            }
            break;

            case MessageTypes::ERROR: {
                if (responseHeader._dataLen != 0) {
                    recvAll(responseHeader._dataLen);
                }

                if (_inBuf.size() < sizeof(db::QueryStatus::Status)) {
                    throw TuringException("Invalid ERROR packet payload size");
                }

                db::QueryStatus::Status status;
                memcpy(&status, _inBuf.data(), sizeof(status));
                res.setStatus(status);
                res.setMessage(std::string_view(_inBuf.data() + sizeof(status),
                                               _inBuf.size() - sizeof(status)));
            }
            break;

            case MessageTypes::PROTOCOL_ERROR: {
                if (responseHeader._dataLen != 0) {
                    recvAll(responseHeader._dataLen);
                }

                throw TuringException("Protocol error from server: "
                                      + std::string(_inBuf.data(), _inBuf.size()));
            }
            break;

            default:
                throw TuringException("Invalid Message Type Received");
            break;
        }
        responseHeader = recvMsgHeader();
    }
}

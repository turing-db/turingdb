#include "PyTuringClient.h"

#include <stdint.h>
#include <string.h>
#include <memory>
#include <string>
#include <string_view>
#include <vector>

#include "NumpyColumnContainer.h"
#include "NumpySink.h"

#include "DecodedColumnSchema.h"
#include "LocalMemory.h"
#include "QueryCallbacks.h"
#include "QueryStatus.h"
#include "TuringProtoDecoder.h"
#include "TuringProtoHeaders.h"
#include "TuringProtoInBuf.h"
#include "TuringTime.h"

#include "BioAssert.h"
#include "TuringException.h"

namespace pybindings {

PyTuringClient::PyTuringClient(const std::string& host, const std::string& port)
    : _localMem(std::make_unique<db::LocalMemory>()),
    _client(std::make_unique<net::proto::TuringClient>(host, port, _localMem.get()))
{
}

PyTuringClient::~PyTuringClient() = default;

void PyTuringClient::setCommitHash(const std::string& commitHash) {
    const auto result = db::CommitHash::fromString(commitHash);
    if (!result.has_value()) {
        throw TuringException("Invalid commit hash: " + std::string(result.error()));
    }

    _client->setCommitHash(result.value());
}

nb::dict PyTuringClient::query(const std::string& cypher) {
    NumpyColumnContainer container;
    db::QueryStatus status;

    {
        nb::gil_scoped_release release;
        status = receiveQuery(cypher, &container);
    }

    if (!status.isOk()) {
        throw TuringException(std::string(db::QueryStatusDescription::value(status.getStatus())) + ": " + status.getError());
    }

    nb::dict envelope = container.toPython();
    envelope["time"] = nb::cast(status.getTotalTime().count());
    return envelope;
}

db::QueryStatus PyTuringClient::receiveQuery(const std::string& cypher, NumpyColumnContainer* container) {
    using namespace net::proto;

    bioassert(cypher.length() <= MAX_WIRE_SIZE, "Query length exceeds maximum wire size");
    bioassert(_client->getGraphName().length() <= MAX_WIRE_SIZE, "Graph name length exceeds maximum wire size");

    _client->sendRequest(cypher);
    _client->recvHttpResponseHeaders();

    _embeddingBuffer.clear();
    _listBuffer.clear();
    _mapBuffer.clear();
    _localMem->clear();

    TuringProtoInBuf& inBuf = _client->getInBuf();

    db::QueryStatus res;
    std::vector<DecodedColumnSchema> columnSchemas;
    NumpySink sink(_localMem.get(), &_embeddingBuffer, &_listBuffer, &_mapBuffer);
    TuringProtoDecoder<NumpySink> decoder(&inBuf, &sink, columnSchemas);

    bool sawTerminalPacket = false;

    while (true) {
        const size_t chunkSize = _client->recvChunkSizeLine();

        if (chunkSize == 0) {
            _client->recvCrlf();
            break;
        }

        ProtoHeader responseHeader;
        _client->recvChunkBody(chunkSize, &responseHeader);
        _client->recvCrlf();

        if (sawTerminalPacket) {
            throw TuringException("Unexpected proto packet after END/ERROR");
        }

        if (responseHeader._dataLen != inBuf.size()) {
            throw TuringException("Proto header dataLen does not match chunk payload size");
        }

        switch (responseHeader._type) {
            case MessageTypes::CHUNK_HEADER:
                decoder.decodeIncomingChunkHeader(container);
            break;

            case MessageTypes::CHUNK:
                decoder.decodeIncomingChunk(container);
            break;

            case MessageTypes::END_CHUNK:
                decoder.decodeChunkFooter(container);
                container->finishChunk();
                for (DecodedColumnSchema& schema : columnSchemas) {
                    schema.getColumnState().reset();
                }
                decoder.reset();
            break;

            case MessageTypes::END: {
                if (responseHeader._dataLen != sizeof(db::QueryCallbacks::ExecTimeMilliseconds)) {
                    throw TuringException("Invalid END packet payload size");
                }

                db::QueryCallbacks::ExecTimeMilliseconds totalTimeMs = 0;
                memcpy(&totalTimeMs, inBuf.data(), sizeof(totalTimeMs));
                res.setTotalTime(Milliseconds(totalTimeMs));
                sawTerminalPacket = true;
            }
            break;

            case MessageTypes::ERROR: {
                if (inBuf.size() < sizeof(db::QueryStatus::Status)) {
                    throw TuringException("Invalid ERROR packet payload size");
                }

                db::QueryStatus::Status status;
                memcpy(&status, inBuf.data(), sizeof(status));
                res.setStatus(status);
                res.setMessage(std::string_view(inBuf.data() + sizeof(status),
                                                inBuf.size() - sizeof(status)));
            }
            break;

            case MessageTypes::PROTOCOL_ERROR:
                throw TuringException("Protocol error from server: "
                                      + std::string(inBuf.data(), inBuf.size()));
            break;

            default:
                throw TuringException("Invalid message type received");
            break;
        }
    }

    return res;
}

}

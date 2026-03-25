#include "TuringProtoServerProcessor.h"

#include <string.h>

#include "DBThreadContext.h"
#include "NetException.h"
#include "ProtocolException.h"
#include "QueryCallbacks.h"
#include "TuringProtoHeaders.h"
#include "QueryStatus.h"
#include "TCPConnection.h"
#include "TuringDB.h"
#include "TuringProtoParser.h"
#include "TuringProtoWriter.h"
#include "dataframe/Dataframe.h"

using namespace db;

namespace {

constexpr size_t HelloPayloadSize =
    sizeof(uint8_t) + sizeof(uint8_t) + sizeof(uint8_t);

void ensureBytesAvailable(size_t offset, size_t required, size_t totalLen) {
    if (offset + required > totalLen) {
        throw ProtocolException("Incoming query payload is truncated");
    }
}

}

TuringProtoServerProcessor::TuringProtoServerProcessor(TuringDB& db,
                                                       net::TCPConnection& connection)
    : _db(db),
    _connection(connection)
{
}

TuringProtoServerProcessor::~TuringProtoServerProcessor() = default;

void TuringProtoServerProcessor::process(net::AbstractThreadContext* threadContext) {
    _threadContext = static_cast<DBThreadContext*>(threadContext);

    auto& parser = _connection.getParser<net::proto::TuringProtoParser>();
    auto& writer = _connection.getWriter<net::proto::TuringProtoWriter>();

    try {
        switch (parser.getHeader()._type) {
            case net::proto::MessageTypes::NABER:
                handleHello();
            break;

            case net::proto::MessageTypes::QUERY:
                handleQuery();
            break;

            default:
                throw ProtocolException("Unsupported protocol message type");
            break;
        }

        _threadContext->getLocalMemory().clear();
    } catch (const ProtocolException& e) {
        //Protocol exceptions are errors that occur at the protocol level: e.g invalid 
        //headers etc.
        writer.writeProtocolError(e.what());
        _connection.setCloseRequired(true);
    } catch (const NetException& e) {
        //Net Exceptions have to do with errors that occur during networking syscalls
        _connection.setCloseRequired(true);
    }
}

void TuringProtoServerProcessor::handleHello() {
    auto& parser = _connection.getParser<net::proto::TuringProtoParser>();
    auto& writer = _connection.getWriter<net::proto::TuringProtoWriter>();
    const std::string_view payload = parser.getPayload();

    if (payload.size() != HelloPayloadSize) {
        throw ProtocolException("Invalid hello payload size");
    }

    uint8_t protocolVersion = 0;
    bool keepAlive = false;
    uint8_t timeout = 0;

    size_t readOffset = 0;
    memcpy(&protocolVersion, payload.data() + readOffset, sizeof(protocolVersion));
    readOffset += sizeof(protocolVersion);
    memcpy(&keepAlive, payload.data() + readOffset, sizeof(keepAlive));
    readOffset += sizeof(keepAlive);
    memcpy(&timeout, payload.data() + readOffset, sizeof(timeout));

    //currently we don't do any thing with the data received in the handshake, but in 
    //the future we will start checking things as a part of the handshake negotiation - 
    //making sure protocol versions are valid etc.

    constexpr bool ack = true;
    writer.writeHelloAck(ack);
}

//Process the query in the request
void TuringProtoServerProcessor::handleQuery() {
    auto& parser = _connection.getParser<net::proto::TuringProtoParser>();
    auto& writer = _connection.getWriter<net::proto::TuringProtoWriter>();
    auto& mem = _threadContext->getLocalMemory();
    const TransactionInfo transactionInfo = getTransactionInfo(parser.getPayload());

    QueryCallbacks callbacks;

    callbacks.setOnOutputHeader([&](const Dataframe* df) {
        bioassert(df != nullptr, "Cannot output header of a null dataframe");
        writer.writeDataframeHeader(df);
    });

    callbacks.setOnOutputData([&](const Dataframe* df) {
        bioassert(df != nullptr, "Cannot output data of a null dataframe");
        writer.writeDataframe(df);
    });

    callbacks.setOnEnd([&](QueryCallbacks::ExecTimeMilliseconds milliseconds) {
        if (writer.errorOccured()) {
            return;
        }
        writer.writeEndPacket(milliseconds);
    });

    callbacks.setOnError([&](const QueryStatus& status) {
        writer.reset();
        if (writer.errorOccured()) {
            return;
        }
        writer.writeError(&status);
    });

    const QueryState state(transactionInfo.graphName, &mem, &_db.getDefaultQueryConfig(), &callbacks, transactionInfo.commit, transactionInfo.change);
    _db.query(transactionInfo.query, state);
}

//Extract the transaction info from the request
TuringProtoServerProcessor::TransactionInfo TuringProtoServerProcessor::getTransactionInfo(std::string_view payload) const {
    size_t offset = 0;
    ensureBytesAvailable(offset, net::proto::QueryWireHeader::wireSize(), payload.size());

    net::proto::QueryWireHeader queryHeader {};
    queryHeader.copyFromBuffer(payload.data(), offset);

    ensureBytesAvailable(offset,
                         queryHeader._graphNameLen + queryHeader._queryLen,
                         payload.size());

    const std::string_view graphName(payload.data() + offset, queryHeader._graphNameLen);
    offset += queryHeader._graphNameLen;

    const std::string_view query(payload.data() + offset, queryHeader._queryLen);
    offset += queryHeader._queryLen;

    if (offset != payload.size()) {
        throw ProtocolException("Incoming query payload size is inconsistent");
    }

    return TransactionInfo {
        .graphName = graphName,
        .commit = CommitHash {queryHeader._commitHash},
        .change = ChangeID {queryHeader._changeID},
        .query = query};
}

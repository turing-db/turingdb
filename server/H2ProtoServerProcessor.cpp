#include "H2ProtoServerProcessor.h"

#include <spdlog/spdlog.h>

#include "DBThreadContext.h"
#include "H2ConnectionState.h"
#include "NetException.h"
#include "ProtocolException.h"
#include "QueryCallbacks.h"
#include "QueryConfig.h"
#include "QueryState.h"
#include "QueryStatus.h"
#include "TCPConnection.h"
#include "TuringDB.h"
#include "TuringProtoHeaders.h"
#include "dataframe/Dataframe.h"
#include "versioning/ChangeID.h"
#include "versioning/CommitHash.h"

#include "BioAssert.h"

using namespace db;

namespace {

struct TransactionInfo {
    std::string_view graphName;
    CommitHash commit;
    ChangeID change;
    std::string_view query;
};

void ensureBytesAvailable(size_t offset, size_t required, size_t totalLen) {
    if (offset + required > totalLen) {
        throw ProtocolException("Incoming query payload is truncated");
    }
}

TransactionInfo parseTransactionInfo(std::string_view payload) {
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

}

H2ProtoServerProcessor::H2ProtoServerProcessor(TuringDB& db,
                                                net::TCPConnection& connection)
    : _db(db),
      _connection(connection)
{
}

H2ProtoServerProcessor::~H2ProtoServerProcessor() = default;

void H2ProtoServerProcessor::process(net::AbstractThreadContext* threadContext) {
    _threadContext = static_cast<DBThreadContext*>(threadContext);

    auto& state = _connection.getConnectionState<net::H2::H2ConnectionState>();

    // The h2 _processor lambda calls process() on every analyze cycle. Many
    // cycles fire purely because nghttp2 has outbound frames to drain
    // (WINDOW_UPDATE acks etc.), with no request actually ready. Bail in
    // that case — the subsequent flush will move the queued frames out.
    if (!state.isRequestReady()) {
        return;
    }
    state.clearRequestReady();

    const int32_t streamId = state.getActiveStreamId();
    spdlog::info("[h2.processor] request ready on stream {}", streamId);

    try {
        // Decode the binary protocol header from the accumulated request
        // body. analyze() returning BadResult means the bytes are malformed;
        // returning Finished=false means the client closed the stream
        // mid-message (END_STREAM arrived with an incomplete payload).
        auto result = state.analyzeRequestBody();
        if (!result) {
            throw ProtocolException("Invalid binary protocol header");
        }
        if (!result.value()) {
            throw ProtocolException("Incomplete binary protocol message");
        }

        const auto msgType = state.getRequestHeader()._type;
        spdlog::info("[h2.processor] dispatch type={} stream={}",
                     static_cast<int>(msgType), streamId);

        switch (msgType) {
            case net::proto::MessageTypes::NABER:
                handleHello();
            break;

            case net::proto::MessageTypes::QUERY:
                handleQuery();
            break;

            default:
                throw ProtocolException("Unsupported binary protocol message type");
            break;
        }
    } catch (const ProtocolException& e) {
        // Protocol errors travel back to the client as a PROTOCOL_ERROR
        // packet in the response body. The h2 stream itself stays open
        // (we don't RST it — same convention as the binary path's writer).
        spdlog::error("[h2.processor] ProtocolException stream={}: {}",
                      streamId, e.what());
        state.resetResponseState();
        state.emitProtocolError(e.what());
        state.markEncoderDone();
        state.submitResponse(state.getActiveStreamId());
    } catch (const NetException& e) {
        // Networking-level failure — give up on the session.
        spdlog::error("[h2.processor] NetException stream={}: {}",
                      streamId, e.what());
        state.markSessionFatal();
    }
}

void H2ProtoServerProcessor::handleHello() {
    auto& state = _connection.getConnectionState<net::H2::H2ConnectionState>();
    spdlog::info("[h2.processor] handleHello stream={}", state.getActiveStreamId());

    // Real binary path validates payload size, parses protocolVersion +
    // keepAlive + timeout fields, and may negotiate. The stub here just
    // ACKs — matches TuringProtoServerProcessor::handleHello's current
    // "currently we don't do anything with the data received in the
    // handshake" behavior.
    state.emitHelloAck();
    state.markEncoderDone();
    state.submitResponse(state.getActiveStreamId());
}

void H2ProtoServerProcessor::handleQuery() {
    auto& state = _connection.getConnectionState<net::H2::H2ConnectionState>();
    auto& mem = _threadContext->getLocalMemory();

    const std::string_view payload = state.getRequestPayload();
    const TransactionInfo txInfo = parseTransactionInfo(payload);

    spdlog::info("[h2.processor] handleQuery stream={} graph='{}' query='{}'",
                 state.getActiveStreamId(),
                 txInfo.graphName,
                 txInfo.query);

    // Submit the response shell before the query runs so the data provider
    // is attached when the first QueryCallbacks fires. Each callback below
    // stages one binary-protocol packet into _responseBuf and immediately
    // drains it through nghttp2 — bytes never accumulate beyond one packet
    // at a time, mirroring TuringProtoWriter's writePacket→flush rhythm.
    state.submitResponse(state.getActiveStreamId());

    QueryCallbacks callbacks;

    callbacks.setOnOutputHeader([&](const Dataframe* df) {
        bioassert(df != nullptr, "Cannot output null dataframe header");
        spdlog::info("[h2.processor] onOutputHeader stream={}", state.getActiveStreamId());
        state.writeChunkHeaderPacket(df);
    });

    callbacks.setOnOutputData([&](const Dataframe* df) {
        bioassert(df != nullptr, "Cannot output null dataframe");
        spdlog::info("[h2.processor] onOutputData stream={}", state.getActiveStreamId());
        state.writeChunkPacket(df);
    });

    callbacks.setOnEnd([&](QueryCallbacks::ExecTimeMilliseconds ms) {
        // markEncoderDone before the write so the END packet's drain sets
        // EOF on its DATA frame, carrying END_STREAM in one go rather than
        // requiring a separate trailing empty DATA(END_STREAM).
        spdlog::info("[h2.processor] onEnd stream={} execTimeMs={}",
                     state.getActiveStreamId(), ms);
        state.markEncoderDone();
        state.writeEndPacket(ms);
    });

    callbacks.setOnError([&](const QueryStatus& status) {
        // Any partial CHUNK still in flight before the error is dropped.
        // Wire CHUNKs already shipped pre-error are the client's to discard
        // when it sees the terminating ERROR packet.
        //
        // We do NOT markEncoderDone here. QueryInterpreterV2::execute
        // unconditionally fires onEnd right after onError on the failure
        // path — that's where _encoderDone gets set and where the trailing
        // END packet carries END_STREAM. If we set EOF on the ERROR's
        // DATA frame, the END packet's drain would land on a half-closed-
        // local stream and assert in drainResponse.
        spdlog::info("[h2.processor] onError stream={} status={} msg='{}'",
                     state.getActiveStreamId(),
                     static_cast<int>(status.getStatus()),
                     status.getError());
        state.resetResponseState();
        state.writeErrorPacket(&status);
    });

    const QueryState qs(txInfo.graphName,
                        &mem,
                        &_db.getDefaultQueryConfig(),
                        &callbacks,
                        txInfo.commit,
                        txInfo.change);
    _db.query(txInfo.query, qs);

    spdlog::info("[h2.processor] _db.query returned stream={}",
                 state.getActiveStreamId());
}

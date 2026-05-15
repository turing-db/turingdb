#include "H2Parser.h"

#include <nghttp2/nghttp2.h>
#include <spdlog/spdlog.h>

#include "H2ConnectionState.h"
#include "NetBuffer.h"

#include "BioAssert.h"

namespace net::H2 {

H2Parser::H2Parser(NetBuffer* inputBuffer, BaseConnectionState* state)
    : _inputBuffer(inputBuffer),
      _connectionState(static_cast<H2ConnectionState*>(state))
{
}

H2Parser::~H2Parser() {
}

AbstractTCPParser::AnalyzeResult H2Parser::analyze() {
    bioassert(_connectionState != nullptr, "H2Parser used without a connection state");

    if (_connectionState->isSessionFatal()) {
        return BadResult(static_cast<AnalyzeError>(NGHTTP2_ERR_CALLBACK_FAILURE));
    }

    auto reader = _inputBuffer->getReader();
    const size_t available = reader.getSize();

    {
        std::string hexDump;
        const size_t dumpLen = std::min<size_t>(available, 32);
        char hexBuf[4];
        const uint8_t* ptr = reinterpret_cast<const uint8_t*>(reader.getData());
        for (size_t i = 0; i < dumpLen; ++i) {
            snprintf(hexBuf, sizeof(hexBuf), "%02x ", ptr[i]);
            hexDump += hexBuf;
        }
        spdlog::info("[h2.parser] analyze available={} bytes hex={}", available, hexDump);
    }

    // Hand the bytes to nghttp2. It parses frames in place from our buffer
    // (no copy — the byte pointers nghttp2 hands to our callbacks point
    // back into _inputBuffer) and synchronously fires the inbound callbacks
    // registered on the session.
    if (available > 0) {
        const nghttp2_ssize consumed = nghttp2_session_mem_recv2(
                _connectionState->getSession(),
                reinterpret_cast<const uint8_t*>(reader.getData()),
                available);

        spdlog::info("[h2.parser] mem_recv2 consumed={}", consumed);

        if (consumed < 0) {
            // Session-fatal: malformed framing, HPACK desync, settings
            // violation, etc. The session can't be reused.
            _connectionState->markSessionFatal();
            return BadResult(static_cast<AnalyzeError>(consumed));
        }

        // mem_recv2 always consumes the entire buffer on success.
        // Anything it can't parse yet (a partial frame at the tail) is
        // stashed inside the session.
        bioassert(static_cast<size_t>(consumed) == available,
                  "nghttp2_session_mem_recv2 must consume the whole buffer on success");

        // Reset the input buffer here, regardless of whether `finished`
        // returns true below. The binary-proto path leaves un-finished
        // bytes in the buffer for the next cycle to combine with new
        // recvs — but nghttp2 has its OWN internal buffering for partial
        // frames, so anything still pending after mem_recv2 is held
        // inside the session, not in our buffer. If we leave our buffer
        // with _bytes > 0 and analyze returns finished=false, the next
        // recv would write at the OLD offset and we'd re-feed already-
        // consumed bytes to nghttp2, causing a NGHTTP2_ERR_PROTO on
        // benign frames like SETTINGS-ACK.
        _inputBuffer->getWriter().reset();
    }

    // "Finished" in our parser model means "TCPConnectionManager should run
    // the _processor step and then flush." For HTTP/2 this fires in two
    // cases:
    //
    //   1. A request became ready this cycle (END_STREAM arrived on a
    //      HEADERS or DATA frame). The _processor will call
    //      H2ConnectionState::processPendingRequest, which dispatches the
    //      binary protocol request with the DBThreadContext in scope.
    //
    //   2. nghttp2 has outbound frames queued (WINDOW_UPDATE, SETTINGS ACK,
    //      PING ACK, or response DATA frames produced during dispatch).
    //      flush() drives session_send to drain them via send_callback2
    //      and send_data_callback.
    //
    // Either condition warrants the process+flush phase.
    const bool wantsWrite = nghttp2_session_want_write(_connectionState->getSession()) != 0;
    const bool requestReady = _connectionState->isRequestReady();
    return wantsWrite || requestReady;
}

void H2Parser::handleAnalyzeError(AnalyzeError error, AbstractTCPWriter& writer) {
    // Map nghttp2 errors to GOAWAY codes. Most fatal session errors warrant
    // PROTOCOL_ERROR; the bad-client-magic case means it wasn't h2 at all,
    // so don't bother framing a reply — just let the connection close.
    if (error == NGHTTP2_ERR_BAD_CLIENT_MAGIC) {
        return;
    }

    uint32_t goawayCode = NGHTTP2_PROTOCOL_ERROR;
    if (error == NGHTTP2_ERR_NOMEM) {
        goawayCode = NGHTTP2_INTERNAL_ERROR;
    }

    nghttp2_submit_goaway(_connectionState->getSession(),
                          NGHTTP2_FLAG_NONE,
                          _connectionState->getLastSeenStreamId(),
                          goawayCode,
                          /*opaque_data=*/nullptr,
                          /*opaque_data_len=*/0);

    // Push the GOAWAY out before the connection closes.
    writer.flush();
}

void H2Parser::reset() {
    // No-op for HTTP/2. nghttp2_session carries connection-lifetime state
    // (HPACK tables, flow control windows, settings, the active stream)
    // that must persist across the analyze/flush cycle. Per-stream cleanup
    // happens via the on_stream_close callback when nghttp2 closes a stream.
}

}

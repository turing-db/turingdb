#include "H2Writer.h"

#include <nghttp2/nghttp2.h>

#include "H2ConnectionState.h"

#include "BioAssert.h"

namespace net::H2 {

H2Writer::H2Writer(BaseConnectionState* state)
    : _connectionState(static_cast<H2ConnectionState*>(state))
{
}

void H2Writer::setSocket(int socket) {
    _socket = socket;
    if (_connectionState != nullptr) {
        _connectionState->setSocket(socket);
    }
}

H2Writer::~H2Writer() {
}

void H2Writer::flush() {
    bioassert(_connectionState != nullptr, "H2Writer used without a connection state");

    if (_errorOccurred || _connectionState->isSessionFatal()) {
        return;
    }

    // Drives the outbound side of the session. nghttp2 walks its pending
    // frame queue and invokes send_callback2 for non-DATA frames and
    // send_data_callback (with NO_COPY-flagged DATA payloads) for the
    // body — both registered on the session by H2ConnectionState.
    const int rv = nghttp2_session_send(_connectionState->getSession());
    if (rv != 0) {
        // session_send2 returns negative nghttp2 error codes on failure.
        // Most are session-fatal (e.g. NGHTTP2_ERR_NOMEM, _CALLBACK_FAILURE).
        _errorOccurred = true;
        _connectionState->markSessionFatal();
    }
}

void H2Writer::reset() {
    // No-op for HTTP/2. Unlike the per-message TuringProtoWriter, this writer
    // is connection-lifetime — the nghttp2 session carries its own outbound
    // state (flow control windows, pending frame queue, HPACK encoder table)
    // across queries.
    _errorOccurred = false;
}

size_t H2Writer::getBytesWritten() const {
    if (_connectionState == nullptr) {
        return 0;
    }

    // Used by TCPConnectionManager::process to decide whether to call
    // flush after the analyze cycle. For h2, "bytes pending" maps to
    // "session has frames queued to send."
    return nghttp2_session_want_write(_connectionState->getSession()) != 0 ? 1 : 0;
}

bool H2Writer::wroteNonEmptyChunk() const {
    // TCPConnectionManager uses this to decide whether to flush again after
    // _ctxt._process. For h2, session_send2 drains everything it can in one
    // pass — there's no need for a second flush. Any remaining frames live
    // in the session and will be sent on the next epoll cycle when the
    // socket becomes writable again.
    return false;
}

bool H2Writer::errorOccured() const {
    return _errorOccurred;
}

}

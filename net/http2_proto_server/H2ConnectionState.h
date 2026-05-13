#pragma once

#include <nghttp2/nghttp2.h>
#include <stdint.h>

#include "BaseConnectionState.h"

namespace net::H2 {

class H2ConnectionState : public net::BaseConnectionState {
public:
    H2ConnectionState() = default;
    ~H2ConnectionState() override;

    H2ConnectionState(const H2ConnectionState&) = delete;
    H2ConnectionState(H2ConnectionState&&) = delete;
    H2ConnectionState& operator=(const H2ConnectionState&) = delete;
    H2ConnectionState& operator=(H2ConnectionState&&) = delete;

    // Builds parser + writer via the base, then sets up the nghttp2 session.
    // Must call BaseConnectionState::init explicitly — virtual dispatch in
    // C++ replaces the base implementation, it does not chain to it.
    void init(CreateAbstractTCPWriterFunc writerFunc,
              CreateAbstractTCPParserFunc parserFunc,
              NetBuffer* buffer) override;

    [[nodiscard]] nghttp2_session* getSession() const { return _session; }

    [[nodiscard]] int32_t getLastSeenStreamId() const { return _lastSeenStreamId; }
    void setLastSeenStreamId(int32_t id) { _lastSeenStreamId = id; }

    [[nodiscard]] bool isSessionFatal() const { return _sessionFatal; }
    void markSessionFatal() { _sessionFatal = true; }

private:
    // nghttp2 owns everything inside this opaque pointer:
    // HPACK tables, flow control windows, the (one) stream's state,
    // SETTINGS state, pending frame queue, DoS counters.
    nghttp2_session* _session {nullptr};

    int32_t _lastSeenStreamId {0};
    bool _sessionFatal {false};

    void initSession();
    void registerCallbacks(nghttp2_session_callbacks* cbs);
    void submitInitialSettings();

    // Static thunks bridge nghttp2's C callback interface to instance methods.
    // Each one recovers `this` from the user_data pointer and forwards.
    static int onBeginHeadersThunk(nghttp2_session*, const nghttp2_frame*, void*);
    static int onHeaderThunk(nghttp2_session*, const nghttp2_frame*,
                             const uint8_t*, size_t,
                             const uint8_t*, size_t,
                             uint8_t, void*);
    static int onDataChunkRecvThunk(nghttp2_session*, uint8_t,
                                    int32_t, const uint8_t*, size_t, void*);
    static int onFrameRecvThunk(nghttp2_session*, const nghttp2_frame*, void*);
    static int onStreamCloseThunk(nghttp2_session*, int32_t, uint32_t, void*);
    static nghttp2_ssize sendBytesThunk(nghttp2_session*, const uint8_t*, size_t,
                                        int, void*);
    static int sendDataFrameThunk(nghttp2_session*, nghttp2_frame*,
                                  const uint8_t*, size_t,
                                  nghttp2_data_source*, void*);
};

}

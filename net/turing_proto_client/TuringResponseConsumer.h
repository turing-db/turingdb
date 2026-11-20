#pragma once

namespace net::proto {

// Takes the result packets of one response, one call per packet, while the client reads the
// HTTP chunks and the END, ERROR and PROTOCOL_ERROR packets. The packet's payload is in the
// client's input buffer during the call.
class TuringResponseConsumer {
public:
    virtual ~TuringResponseConsumer();

    virtual void onChunkHeader() = 0;
    virtual void onChunk() = 0;
    virtual void onEndChunk() = 0;
    virtual void onEnd() = 0;
};

}

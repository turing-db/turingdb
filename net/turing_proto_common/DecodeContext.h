#pragma once

#include <stddef.h>

namespace net::proto {

class TuringProtoInBuf;

// Byte-level resume point for a value split across packet/buffer boundaries: the
// remaining wire bytes stream into the (stable) destination at _start before the
// decoders re-enter.
struct InterruptedBufferState {
    char* _start {nullptr};
    size_t _len {0};
    size_t _offset {0};

    void reset() {
        _start = nullptr;
        _len = 0;
        _offset = 0;
    }
};

// Wire-side decode state shared by every sink family: stream position and the
// per-value resume state. Family storage (arenas, list builders) lives behind
// the sink.
struct DecodeContext {
    size_t _colIndex {0};
    size_t _rowIndex {0};
    TuringProtoInBuf* _inBuf {nullptr};
    InterruptedBufferState _bufferState;

    void reset() {
        _colIndex = 0;
        _rowIndex = 0;
        _bufferState.reset();
    }
};

}

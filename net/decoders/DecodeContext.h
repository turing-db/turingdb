#pragma once

#include <stddef.h>
#include <optional>

namespace net::proto {

class TuringProtoInBuf;

// Byte-level resume point for a value split across packet/buffer boundaries: the
// remaining wire bytes stream into the (stable) destination at _start on the arrival of
// the next packet.
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
// per-value resume state.
struct DecodeContext {
    // Index of current column being decoded
    size_t _columnIndex {0};
    // Index of current row being decoded
    size_t _rowIndex {0};
    // Pointer to the TuringProto receive buffer
    TuringProtoInBuf* _inBuf {nullptr};
    InterruptedBufferState _bufferState;
    // Entry count of the EntityList row being decoded. The count and actual data can be
    // split across chunks so we need to store state here.
    std::optional<size_t> _entityListEntryCount;
    // Set once a constant list column's header has been read and its space reserved, so a
    // resumed pass drains elements rather than reading the header again. An optional
    // constant spends _rowIndex on its has-value flag, so the two cannot share it.
    bool _constListStarted {false};

    void reset() {
        _columnIndex = 0;
        _rowIndex = 0;
        _bufferState.reset();
        _entityListEntryCount.reset();
        _constListStarted = false;
    }
};

}

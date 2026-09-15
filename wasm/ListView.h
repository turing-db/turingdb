#pragma once

#include <stdint.h>

namespace wasm {

// Handle to a list in the sink's flat list bytes: the byte offset of its header.
struct ListView {
    uint32_t _offset {0};
};

// The same for a map. Declared so the sink satisfies ProtoDecodeSink; the flat-bytes layout
// a map would take, and the JS reader for it, are not written yet.
struct MapView {
    uint32_t _offset {0};
};

}

#pragma once

#include <stdint.h>

namespace wasm {

// Handle to a map entry in the sink's flat map bytes: the byte offset of its key index,
// which is where the entry starts.
struct MapEntryView {
    uint32_t _offset {0};
};

}

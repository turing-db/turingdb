#pragma once

#include <stdint.h>

namespace wasm {

// Handle to a list in the sink's flat list bytes: the byte offset of its header.
struct ListView {
    uint32_t _offset {0};
};

}

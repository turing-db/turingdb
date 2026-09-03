#pragma once

#include <stdint.h>

namespace wasm {

// Handle to a list element in the sink's flat list bytes: the byte offset of its tag.
struct ListElementView {
    uint32_t _offset {0};
};

}

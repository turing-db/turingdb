#pragma once

#include <stdint.h>

namespace wasm {

// Handle to a cell in the sink's flat bytes: the byte offset of its tag.
struct WasmCellView {
    uint32_t _offset {0};
};

}

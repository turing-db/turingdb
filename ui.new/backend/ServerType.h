#pragma once

#include <cstdint>

namespace ui {

enum class ServerType : uint8_t {
    BIOSERVER = 0,
    FLASK,
    REACT,
    
    _SIZE
};

}

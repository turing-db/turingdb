#pragma once

#include <stdint.h>

namespace db {

enum class PathExplorationDir : uint8_t {
    FORWARD,
    BACKWARD,
    BOTH,
};

}

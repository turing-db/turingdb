#pragma once

#include <stdint.h>

namespace db {

enum class PathExplorationDir : uint8_t {
    Forward,
    Backward,
    Both,
};

}

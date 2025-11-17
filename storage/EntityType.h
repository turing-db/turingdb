#pragma once

#include <stdint.h>

namespace db {

enum class EntityType : uint8_t {
    Node = 0,
    Edge = 1
};

}

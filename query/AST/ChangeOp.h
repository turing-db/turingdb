#pragma once

#include <stdint.h>

namespace db {

enum class ChangeOp : uint8_t {
    NEW = 0,
    SUBMIT,
    DELETE,
    LIST,
};

}

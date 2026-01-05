#pragma once

#include <stdint.h>

namespace db::v2 {

enum class ChangeOp : uint8_t {
    NEW = 0,
    SUBMIT,
    DELETE,
    LIST,
};

}

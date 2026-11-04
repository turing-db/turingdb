#pragma once

#include <stdint.h>

namespace db {

enum class Endpoint : int64_t {
    UNKNOWN = -1,

    QUERY,
};

}

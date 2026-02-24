#pragma once

#include <stdint.h>

namespace db {

enum class CSVErrorMode : uint8_t {
    Fail,
    Skip,
};

}

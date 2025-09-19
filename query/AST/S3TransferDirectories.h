#pragma once

#include <cstdint>

namespace db {

enum class S3TransferDirectory : uint8_t {
    DATA = 0,
    GRAPH
};

}

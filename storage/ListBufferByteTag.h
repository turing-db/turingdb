#include <stdint.h>

namespace db {

enum class ListBufferTypeTag : uint8_t {
    Int = 0,
    Double,

    INVALID,
};

}

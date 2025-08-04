#include "indexes/StringIndex.h"

namespace db {

constexpr size_t NODESIZE =
    sizeof(size_t) + ((SIGMA + 7) / 8) * sizeof(uint8_t) + sizeof(char) + sizeof(bool);
constexpr size_t CHILDMASKSIZE = (SIGMA + 7) / 8;

}

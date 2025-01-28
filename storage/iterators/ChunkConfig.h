#pragma once

#include <stddef.h>

namespace db {

class ChunkConfig {
public:
    static constexpr size_t CHUNK_SIZE = 64ull*1024;
};

}

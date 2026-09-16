#pragma once

#include "iterators/ChunkConfig.h"

namespace db {

class QueryConfig {
public:
    size_t getChunkSize() const { return _chunkSize; }

    void setChunkSize(size_t chunkSize) { _chunkSize = chunkSize; }

private:
    size_t _chunkSize {ChunkConfig::CHUNK_SIZE};
};

}

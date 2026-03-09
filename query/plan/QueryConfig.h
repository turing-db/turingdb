#pragma once

#include "PlanGenConfig.h"
#include "iterators/ChunkConfig.h"

namespace db {

class QueryConfig {
public:
    const PlanGenConfig& getPlanGenConfig() const { return _planGenConfig; }
    PlanGenConfig& getPlanGenConfig() { return _planGenConfig; }
    size_t getChunkSize() const { return _chunkSize; }

    void setChunkSize(size_t chunkSize) { _chunkSize = chunkSize; }

private:
    size_t _chunkSize {ChunkConfig::CHUNK_SIZE};
    PlanGenConfig _planGenConfig;
};

}

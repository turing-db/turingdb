#pragma once

#include "iterators/ChunkConfig.h"

namespace db {
class GraphView;
}

namespace db::v2 {

class ExecutionContext {
public:
    ExecutionContext(const GraphView& graphView)
        : _graphView(graphView)
    {
    }

    const GraphView& getGraphView() const { return _graphView; }
    size_t getChunkSize() const { return _chunkSize; }

    void setChunkSize(size_t chunkSize) { _chunkSize = chunkSize; }

private:
    const GraphView& _graphView;
    size_t _chunkSize = ChunkConfig::CHUNK_SIZE;
};
}
